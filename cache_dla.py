#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Abruf von DLA-Daten und Export als lokaler TSV-Cache."""

import argparse
import json
from http.client import IncompleteRead, RemoteDisconnected
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd


QUERY_URL = (
	'https://dataservice.dla-marbach.de/v1/records?'
	'q=%28identifier_type_mv%3A572z%20OR%20issn_mv%3A%2A%29%20AND%20filterSource%3A%20Bibliotheksmaterialien'
	'&format=json&fl=id,identifier_id_mv,identifier_type_mv,issn_mv,display&sort=id%20desc'
)


parser = argparse.ArgumentParser()
parser.add_argument('--output-zdb', help='Output file name for id/zdb', default='cache/cache-dla-zdb.tsv')
parser.add_argument('--output-issn', help='Output file name for id/issn', default='cache/cache-dla-issn.tsv')
args = parser.parse_args()


def fetch_json_records(url):
	"""Fetch JSON content with simple retry handling."""
	request = Request(url, headers={'User-Agent': 'DLA OPAC https://github.com/dla-marbach/dla-opac-ezb/'})

	body = b''
	for _ in range(2):
		try:
			with urlopen(request, timeout=60) as response:
				body = response.read()
			break
		except (HTTPError, URLError, TimeoutError, RemoteDisconnected, IncompleteRead, OSError):
			continue

	if not body:
		return []

	try:
		parsed = json.loads(body.decode('utf-8', errors='replace'))
	except json.JSONDecodeError:
		return []

	if isinstance(parsed, list):
		return parsed

	return []


def normalize_list(values):
	"""Return cleaned list values."""
	if not isinstance(values, list):
		return []

	return [str(value).strip() for value in values if str(value).strip()]


def normalize_display(value):
	"""Return a normalized display string from scalar or list values."""
	if isinstance(value, list):
		for entry in value:
			cleaned = str(entry).strip()
			if cleaned:
				return cleaned
		return ''

	return str(value).strip() if value is not None else ''


def extract_zdb_ids(identifier_ids, identifier_types):
	"""Return only identifier IDs where the aligned type is 572z."""
	if not isinstance(identifier_ids, list) or not isinstance(identifier_types, list):
		return []

	values = []
	for identifier_id, identifier_type in zip(identifier_ids, identifier_types):
		if str(identifier_type).strip() == '572z' and str(identifier_id).strip():
			values.append(str(identifier_id).strip())

	return values


def print_progress(processed, total, start_time, current_id):
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
		f"ETA {eta_min:02d}:{eta_sec:02d} | id: {current_id}",
		end='',
		flush=True
	)


records = fetch_json_records(QUERY_URL)
total_records = len(records)
print(f"Starte DLA-Abruf fuer {total_records} Datensaetze...")

zdb_rows = []
issn_rows = []
start_time = time.time()

for idx, record in enumerate(records, start=1):
	record_id = str(record.get('id', '')).strip()
	if not record_id:
		continue

	record_display = normalize_display(record.get('display', ''))

	zdb_ids = extract_zdb_ids(record.get('identifier_id_mv', []), record.get('identifier_type_mv', []))

	for zdb_id in zdb_ids:
		zdb_rows.append({'id': record_id, 'display': record_display, 'zdb': zdb_id})

	for issn in normalize_list(record.get('issn_mv', [])):
		issn_rows.append({'id': record_id, 'display': record_display, 'issn': issn})

	if idx == 1 or idx % 500 == 0 or idx == total_records:
		print_progress(idx, total_records, start_time, record_id)

if total_records:
	print('')

df_zdb = pd.DataFrame(zdb_rows, columns=['id', 'display', 'zdb'])
df_issn = pd.DataFrame(issn_rows, columns=['id', 'display', 'issn'])

# Export
(df_zdb
	.sort_values(by='id')
	.reset_index(drop=True)
	.to_csv(args.output_zdb, sep='\t', index=False)
)

(df_issn
	.sort_values(by='id')
	.reset_index(drop=True)
	.to_csv(args.output_issn, sep='\t', index=False)
)

print(f"Cache geschrieben: {args.output_zdb} ({len(df_zdb)} Zeilen)")
print(f"Cache geschrieben: {args.output_issn} ({len(df_issn)} Zeilen)")