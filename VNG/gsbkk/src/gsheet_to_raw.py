#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
gsheet_to_raw.py - Google Sheets to HDFS RAW extractor (Python 2.7 compatible)

Reads a Google Sheets worksheet and writes the data as JSON lines to HDFS.
Replaces the hardcoded gsheet input type in etl_engine.py with a proper
two-step flow: RAW extraction → SQL transformation.

Row 0 of the sheet is used as column names; data starts from row 1.
Output format is one JSON object per line (Spark-compatible with spark.read.json()).

Usage (key=value positional args):
    python gsheet_to_raw.py \\
        sheet_id=1F0DpS1Kw43G_mbbluVCLx2HcSCoASk2IzDE3n2rDR7w \\
        worksheet="Daily Overall" \\
        cred_file=tsn-data-0e06f020fc9b.json \\
        output_path="hdfs://c0s/user/gsbkk-workspace-yc9t6/raw/rfc/daily/{logDate}" \\
        logDate=2026-03-17

Required args:
    sheet_id        Google Sheets document ID (from the URL)
    worksheet       Worksheet (tab) name  e.g. "Daily Overall"
    output_path     HDFS destination; may contain {logDate} substitution tokens
    logDate         Date variable for output_path substitution

Optional args:
    cred_file       Service-account JSON filename in CREDENTIALS_DIR
                    (default: tsn-data-0e06f020fc9b.json)
    skip_empty      true/false: skip rows where ALL cells are empty (default: true)
"""
from __future__ import print_function

import json
import os
import subprocess
import sys
import tempfile

try:
    from functools import reduce
except ImportError:
    pass  # Python 2: reduce is a builtin


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args(argv):
    """Parse key=value positional args into a plain dict."""
    def _add(d, token):
        if '=' in token:
            k, v = token.split('=', 1)
            d[k] = v
        return d
    return reduce(_add, argv, {})


def substitute_path(path, job):
    """Replace {key} placeholders in a string from the job args dict."""
    for k, v in job.items():
        path = path.replace('{' + k + '}', v)
    return path


# ---------------------------------------------------------------------------
# Google Sheets read
# ---------------------------------------------------------------------------

def read_sheet(sheet_id, worksheet, cred_path):
    """
    Read all values from a Google Sheets worksheet.
    Returns (headers, rows) where headers is a list of strings and
    rows is a list of lists (one per data row).

    Uses google-auth + googleapiclient (available in the project venv).
    """
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    # Read all values from the worksheet
    result = (
        service.spreadsheets().values()
        .get(spreadsheetId=sheet_id, range=worksheet)
        .execute()
    )
    values = result.get('values', [])

    if not values:
        return [], []

    headers = values[0]
    rows = values[1:]
    return headers, rows


# ---------------------------------------------------------------------------
# HDFS write
# ---------------------------------------------------------------------------

def write_to_hdfs(records, output_path):
    """
    Serialise records as JSON lines to a temp file, then upload to HDFS.
    Output file: output_path/part-00000.json
    """
    if not records:
        print('No records to write. Skipping HDFS upload.')
        sys.stdout.flush()
        return

    tmp_fd, tmp_path = tempfile.mkstemp(suffix='.json', prefix='gsheet_to_raw_')
    try:
        with os.fdopen(tmp_fd, 'w') as f:
            for rec in records:
                f.write(json.dumps(rec) + '\n')

        print('Written {0} records to temp file.'.format(len(records)))
        sys.stdout.flush()

        # Ensure HDFS parent directory exists
        parent = output_path.rsplit('/', 1)[0]
        subprocess.call(['hdfs', 'dfs', '-mkdir', '-p', parent])

        # Upload (overwrite any existing file)
        dest = output_path.rstrip('/') + '/part-00000.json'
        ret = subprocess.call(['hdfs', 'dfs', '-put', '-f', tmp_path, dest])
        if ret != 0:
            raise RuntimeError('hdfs dfs -put failed with exit code {0}'.format(ret))

        print('Data written to HDFS: {0}'.format(dest))
        sys.stdout.flush()
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    job = parse_args(sys.argv[1:])

    sheet_id    = job.get('sheet_id', '')
    worksheet   = job.get('worksheet', '')
    output_path = job.get('output_path', '')
    log_date    = job.get('logDate', '')
    cred_file   = job.get('cred_file', 'tsn-data-0e06f020fc9b.json')
    skip_empty  = job.get('skip_empty', 'true').lower() != 'false'

    if not sheet_id:
        print('ERROR: sheet_id is required', file=sys.stderr)
        sys.exit(1)
    if not worksheet:
        print('ERROR: worksheet is required', file=sys.stderr)
        sys.exit(1)
    if not output_path:
        print('ERROR: output_path is required', file=sys.stderr)
        sys.exit(1)

    output_path = substitute_path(output_path, job)

    # Resolve credentials file
    creds_dir = os.environ.get('CREDENTIALS_DIR', '/tmp/gsbkk_creds')
    cred_path = os.path.join(creds_dir, cred_file)
    if not os.path.exists(cred_path):
        print('ERROR: Credential file not found: {0}'.format(cred_path), file=sys.stderr)
        sys.exit(1)

    print('========================================================')
    print('Google Sheets to RAW Extractor')
    print('========================================================')
    print('Sheet ID    : ' + sheet_id)
    print('Worksheet   : ' + worksheet)
    print('Output path : ' + output_path)
    print('Log date    : ' + log_date)
    print('========================================================')
    sys.stdout.flush()

    # Read from Google Sheets
    print('Reading worksheet "{0}" ...'.format(worksheet))
    sys.stdout.flush()

    headers, rows = read_sheet(sheet_id, worksheet, cred_path)

    if not headers:
        print('WARNING: No data found in worksheet "{0}". Nothing to write.'.format(worksheet))
        sys.stdout.flush()
        sys.exit(0)

    print('Headers ({0}): {1}'.format(len(headers), ', '.join(headers)))
    print('Data rows   : {0}'.format(len(rows)))
    sys.stdout.flush()

    # Convert rows to dicts, padding short rows with empty strings
    records = []
    for row in rows:
        # Pad row to match header length
        padded = list(row) + [''] * (len(headers) - len(row))
        rec = dict(zip(headers, padded))

        if skip_empty:
            # Skip rows where all values are empty strings
            if all(v == '' for v in rec.values()):
                continue

        records.append(rec)

    print('Records after filtering: {0}'.format(len(records)))
    sys.stdout.flush()

    write_to_hdfs(records, output_path)

    print('Done.')
    sys.stdout.flush()


if __name__ == '__main__':
    main()
