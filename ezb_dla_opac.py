#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Transformation EZB-Daten in DLA-OPAC-Internformat"""

import argparse

import pandas as pd
import yaml

__author__ = 'Felix Lohmeier'

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', help='Input file name(s)', default='input/ezb-dla-kbart.tsv')
parser.add_argument('-o', '--output', help='Output file name', default='output/ezb.tsv')
args = parser.parse_args()

# Import EZB-Quelldaten im KBART-Format
df_input = pd.read_csv(args.input, sep='\t', dtype=str, keep_default_na=False)
df_input = df_input.astype(str)
df_input = df_input.apply(lambda col: col.str.strip())
df_input = df_input.drop_duplicates(subset=['title_id'], keep='first').reset_index(drop=True)

# Import ZDB-Cache
df_zdb_cache = pd.read_csv('cache/cache-zdb.tsv', sep='\t', dtype=str, keep_default_na=False)
df_zdb_cache = df_zdb_cache.astype(str)
df_zdb_cache = df_zdb_cache.apply(lambda col: col.str.strip())
df_zdb_cache = df_zdb_cache[['zdb_id', 'zdb_264_a', 'zdb_041_a', 'zdb_776_w']].drop_duplicates(subset=['zdb_id'])

df_input = df_input.merge(df_zdb_cache, on='zdb_id', how='left')
df_input['zdb_264_a'] = df_input['zdb_264_a'].fillna('')
df_input['zdb_041_a'] = df_input['zdb_041_a'].fillna('')
df_input['zdb_776_w'] = df_input['zdb_776_w'].fillna('')

# Import DLA-Caches fuer verwandte Datensaetze
df_dla_issn_cache = pd.read_csv('cache/cache-dla-issn.tsv', sep='\t', dtype=str, keep_default_na=False)
df_dla_issn_cache = df_dla_issn_cache.astype(str)
df_dla_issn_cache = df_dla_issn_cache.apply(lambda col: col.str.strip())

df_dla_zdb_cache = pd.read_csv('cache/cache-dla-zdb.tsv', sep='\t', dtype=str, keep_default_na=False)
df_dla_zdb_cache = df_dla_zdb_cache.astype(str)
df_dla_zdb_cache = df_dla_zdb_cache.apply(lambda col: col.str.strip())

issn_to_ids = (
	df_dla_issn_cache[df_dla_issn_cache['issn'] != '']
	.groupby('issn', sort=False)['id']
	.apply(lambda values: list(dict.fromkeys(values)))
	.to_dict()
)

zdb_to_ids = (
	df_dla_zdb_cache[df_dla_zdb_cache['zdb'] != ''].assign(zdb=lambda frame: frame['zdb'].str.lower())
	.groupby('zdb', sort=False)['id']
	.apply(lambda values: list(dict.fromkeys(values)))
	.to_dict()
)

id_to_display = (
	pd.concat(
		[
			df_dla_issn_cache[['id', 'display']],
			df_dla_zdb_cache[['id', 'display']]
		],
		ignore_index=True
	)
	.assign(
		id=lambda frame: frame['id'].str.strip(),
		display=lambda frame: frame['display'].str.strip()
	)
	.query("id != '' and display != ''")
	.drop_duplicates(subset=['id'], keep='first')
	.set_index('id')['display']
	.to_dict()
)


def relation_ids_from_row(row):
	related_ids = []
	seen_ids = set()

	for issn in [value.strip() for value in row['print_identifier'].split('␟') if value.strip()]:
		for related_id in issn_to_ids.get(issn, []):
			if related_id not in seen_ids:
				seen_ids.add(related_id)
				related_ids.append(related_id)

	for relation in [value.strip() for value in row['zdb_776_w'].split('␟') if value.strip()]:
		if '(DE-600)' not in relation:
			continue
		zdb_value = relation.split('(DE-600)', 1)[1].strip().lower()
		if not zdb_value:
			continue
		for related_id in zdb_to_ids.get(zdb_value, []):
			if related_id not in seen_ids:
				seen_ids.add(related_id)
				related_ids.append(related_id)

	return '␟'.join(related_ids)

# Import Sprachcodes
df_sprachcodes = pd.read_csv('input/sprachcodes.csv', dtype=str, keep_default_na=False)
df_sprachcodes = df_sprachcodes.astype(str)
df_sprachcodes = df_sprachcodes.apply(lambda col: col.str.strip())
language_map = dict(zip(df_sprachcodes['code'], df_sprachcodes['language']))

# Transformation in Ziel-Dataframe
df = pd.DataFrame()

df['id'] = 'EZB' + df_input['title_id']
df['display'] = df_input['publication_title']
df['title'] = df_input['publication_title']
df['usageRestriction'] = 'benutzbar'
df['filterDigital'] = True
df['filterSource'] = 'Elektronische Zeitschriftenbibliothek'
df['filterMedium_mv'] = 'Zeitschrift'
df['filterType_mv'] = 'Gedrucktes'
df['displayAddition2'] = 'Volltext (Elektronische Zeitschriftenbibliothek)'
df['source'] = 'AK'
df['publisherOriginalText_mv'] = df_input['publisher_name']
df['identifier_id_mv'] = df_input['zdb_id']
df['issn_mv'] = df_input['online_identifier']

df['publisherOriginalLocation_mv'] = df_input['zdb_264_a'].map(
	lambda value: '; '.join(
		[
			part
			for part in dict.fromkeys(item.strip() for item in value.split('␟'))
			if part
		]
	)
	if value
	else ''
)

df['filterLanguage_mv'] = df_input['zdb_041_a'].map(
	lambda value: '␟'.join(
		[
			language_map.get(code.strip(), '')
			for code in value.split('␟')
			if code.strip() and language_map.get(code.strip(), '')
		]
	)
	if value
	else ''
)

df['publicationHistory'] = df_input.apply(
	lambda row: ((
		(
			row['num_first_vol_online']
			+ (f" ({row['date_first_issue_online']})" if row['date_first_issue_online'] else '')
			+ (f", {row['num_first_issue_online']}" if row['num_first_issue_online'] else '')
		)
		if row['num_first_vol_online']
		else (
			row['date_first_issue_online']
			+ (
				f", {row['num_first_issue_online']}"
				if row['num_first_issue_online'] and row['date_first_issue_online']
				else row['num_first_issue_online']
			)
		)
	) + ' - ' + (
		(
			row['num_last_vol_online']
			+ (f" ({row['date_last_issue_online']})" if row['date_last_issue_online'] else '')
			+ (f", {row['num_last_issue_online']}" if row['num_last_issue_online'] else '')
		)
		if row['num_last_vol_online']
		else (
			row['date_last_issue_online']
			+ (
				f", {row['num_last_issue_online']}"
				if row['num_last_issue_online'] and row['date_last_issue_online']
				else row['num_last_issue_online']
			)
		)
	)).strip(),
	axis=1
)

df['textualHolding_mv'] = df['publicationHistory']

df['displayAddition1'] = df.apply(
	lambda row: (f"{row['publisherOriginalText_mv']}, {row['textualHolding_mv']}"
	             if row['publisherOriginalText_mv'] and row['textualHolding_mv']
	             else row['publisherOriginalText_mv'] or row['textualHolding_mv']),
	axis=1
)

df['filterDateRange_mv'] = df_input.apply(
	lambda row: (lambda joined: f'[{joined}]' if 'TO' in joined else joined)(
		' TO '.join([value for value in [row['date_first_issue_online'], row['date_last_issue_online']] if value])
	),
	axis=1
)

df['filterDatePoint_mv'] = df['filterDateRange_mv'].map(
	lambda facet_time: '␟'.join(
		[
			(
				f'{value}-01-01T00:00:00Z'
				if len(value) == 4
				else f'{value}-01T00:00:00Z'
				if len(value) == 6
				else f'{value}T00:00:00Z'
				if len(value) == 8
				else ''
			)
			for value in sorted(
				{
					part
					for part in facet_time.replace('[', '').replace(']', '').replace(' ', '').replace('TO', '␟').split('␟')
					if part.isnumeric()
				}
			)
			if len(value) in {4, 6, 8}
		]
	)
)

df['website_description_mv'] = df.apply(
	lambda row: '␟'.join(
		[
			v
			for v in [
				('Volltext (EZB)' if row['identifier_id_mv'] else ''),
				('Volltext (Verlag)' if df_input.loc[row.name, 'title_url'] else '')
			]
			if v
		]
	),
	axis=1
)

df['website_url_mv'] = df.apply(
	lambda row: '␟'.join(
		[
			v
			for v in [
				(f"http://ezb.uni-regensburg.de/?{row['identifier_id_mv'][:-2]}&bibid=DLA" if row['identifier_id_mv'] else ''),
				(df_input.loc[row.name, 'title_url'] if df_input.loc[row.name, 'title_url'] else '')
			]
			if v
		]
	),
	axis=1
)

df['identifier_type_mv'] = df_input['zdb_id'].map(lambda value: '572z' if value else '')
df['relation_id_mv'] = df_input.apply(relation_ids_from_row, axis=1)
df['relation_type_mv'] = df['relation_id_mv'].map(
	lambda value: '␟'.join(['Erscheint auch als Druck-Ausgabe'] * len([part for part in value.split('␟') if part]))
	if value
	else ''
)
df['relation_display_mv'] = df['relation_id_mv'].map(
	lambda value: '␟'.join([id_to_display.get(related_id, '') for related_id in value.split('␟') if related_id and id_to_display.get(related_id, '')])
	if value
	else ''
)

# Export TSV

df.to_csv(args.output, sep='\t', index=False)

# Export CSV Links AK <-> EZB

df_links = (
	df[['relation_id_mv', 'id', 'display']]
	.assign(relation_id_mv=lambda frame: frame['relation_id_mv'].str.split('␟'))
	.explode('relation_id_mv')
	.assign(relation_id_mv=lambda frame: frame['relation_id_mv'].fillna('').str.strip())
	.query("relation_id_mv != ''")
	[['relation_id_mv', 'id', 'display']]
	.rename(columns={'relation_id_mv': 'ak_id', 'id': 'ezb_id', 'display': 'ezb_display'})
)

df_links.to_csv('output/links-ak-ezb.csv', index=False)

# Export YAML

def remove_empty_string_values(record):
	return {
		key: value
		for key, value in record.items()
		if value != ''
	}

with open('output/ezb.yaml', 'w', encoding='utf-8') as yaml_file:
	yaml.safe_dump(
		[remove_empty_string_values(record) for record in df.to_dict(orient='records')],
		yaml_file,
		allow_unicode=True,
		sort_keys=False
	)
