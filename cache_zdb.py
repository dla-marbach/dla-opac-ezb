#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Abruf von ZDB-Daten und Export als lokaler TSV-Cache."""

import argparse
from http.client import RemoteDisconnected
import time
import xml.etree.ElementTree as ET
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd


parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', help='Input file name(s)', default='input/ezb-dla-kbart.tsv')
parser.add_argument('-o', '--output', help='Output file name', default='cache/cache-zdb.tsv')
parser.add_argument('--progress-every', help='Statusausgabe alle N Datensaetze', type=int, default=50)
args = parser.parse_args()


def fetch_zdb_marc_fields(zdb_id):
	"""Fetch selected MARCXML subfields for a given zdb_id."""
	if not zdb_id:
		return '', '', ''

	url = f'https://ld.zdb-services.de/data/{zdb_id}.plus-1.mrcx'
	request = Request(url, headers={'User-Agent': 'DLA OPAC https://github.com/dla-marbach/dla-opac-ezb/'})

	xml_content = b''
	for _ in range(2):
		try:
			with urlopen(request, timeout=15) as response:
				xml_content = response.read()
			break
		except (HTTPError, URLError, TimeoutError, RemoteDisconnected, OSError):
			continue

	if not xml_content:
		return '', '', ''

	try:
		root = ET.fromstring(xml_content)
	except ET.ParseError:
		return '', '', ''

	ns = {'marc': 'http://www.loc.gov/MARC21/slim'}

	def collect_subfields(tag, code):
		values = [
			(item.text or '').strip()
			for item in root.findall(
				f".//marc:datafield[@tag='{tag}']/marc:subfield[@code='{code}']",
				ns
			)
			if (item.text or '').strip()
		]
		return '␟'.join(values)

	return (
		collect_subfields('264', 'a'),
		collect_subfields('041', 'a'),
		collect_subfields('776', 'w')
	)


def print_progress(processed, total, start_time, current_zdb_id):
	"""Print a compact one-line progress display."""
	elapsed = time.time() - start_time
	rate = processed / elapsed if elapsed > 0 else 0
	remaining = total - processed
	eta_seconds = int(remaining / rate) if rate > 0 else 0
	percent = (processed / total * 100) if total else 100

	eta_min = eta_seconds // 60
	eta_sec = eta_seconds % 60

	print(
		f"\rFortschritt: {processed}/{total} ({percent:5.1f}%) | "
		f"ETA {eta_min:02d}:{eta_sec:02d} | zdb_id: {current_zdb_id}",
		end='',
		flush=True
	)


# Import EZB-Quelldaten im KBART-Format
cols = ['zdb_id']
df_input = pd.read_csv(args.input, sep='\t', dtype=str, keep_default_na=False, usecols=cols)
df_input = df_input.fillna('').astype(str)
df_input = df_input.apply(lambda col: col.str.strip())

# Nur eindeutige IDs verarbeiten
unique_ids = pd.Series(df_input['zdb_id'].unique(), name='zdb_id').tolist()

total_ids = len(unique_ids)
print(f"Starte ZDB-Abruf fuer {total_ids} eindeutige zdb_id...")

cache_rows = []
start_time = time.time()

for idx, zdb_id in enumerate(unique_ids, start=1):
	zdb_264_a, zdb_041_a, zdb_776_w = fetch_zdb_marc_fields(zdb_id)
	cache_rows.append(
		{
			'zdb_id': zdb_id,
			'zdb_264_a': zdb_264_a,
			'zdb_041_a': zdb_041_a,
			'zdb_776_w': zdb_776_w,
		}
	)

	if idx == 1 or idx % max(args.progress_every, 1) == 0 or idx == total_ids:
		print_progress(idx, total_ids, start_time, zdb_id)

if total_ids:
	print('')

df_cache = pd.DataFrame(cache_rows)

# Export
(df_cache
	.sort_values(by='zdb_id')
	.reset_index(drop=True)
	.to_csv(args.output, sep='\t', index=False)
)

print(f"Cache geschrieben: {args.output} ({len(df_cache)} Zeilen)")
