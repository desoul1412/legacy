#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
api_to_raw.py - Generic API extractor (Python 2.7 compatible)

Fetches data from any HTTP endpoint and writes raw JSON lines to HDFS.
Replaces the hardcoded logic in api_extractor.py with a templated approach:
  adding a new API source only requires adding one DAG task, no Python changes.

Usage (key=value positional args, modelled after spark_processor.py):
    python api_to_raw.py \\
        url_template="https://api.sensortower.com/v1/ios/apps/app_ids?category=6014&start_date={{ logDate }}&limit=10000" \\
        cred_file=cred_tsn.json \\
        cred_key=sensortower \\
        response_key=ids \\
        paginate=true \\
        output_path="hdfs://c0s/user/gsbkk-workspace-yc9t6/raw/sensortower/new_games_ios/{logDate}" \\
        logDate=2024-01-15

Required args:
    url_template        URL to call; may contain {{ logDate }} Jinja2 placeholders
    output_path         HDFS path; may contain {logDate} substitution tokens
    logDate             Date variable used for both Jinja2 and path substitution

Optional args:
    url                 Alias for url_template (pre-rendered URL)
    params              JSON-encoded dict of extra query params, e.g. '{"limit":"500"}'
    cred_file           Credential filename in CREDENTIALS_DIR (e.g. cred_tsn.json)
    cred_key            Key within the credential file   (e.g. sensortower)
    auth_param          Query param name for the token   (default: auth_token)
    auth_header         Header name for the token (alternative to auth_param)
    auth_value_key      Key in credentials dict that holds the token (default: api_key)
    response_key        Dot-path into the JSON response to reach the data list
                        e.g. "ids"  or  "data.list"  (leave empty to use root)
    paginate            true / false: follow offset-based pagination (default: false)
    page_param          Param name for the page offset   (default: offset)
    page_size           Records per page                 (default: 10000)
    method              HTTP method: GET or POST         (default: GET)
    timeout             Request timeout in seconds       (default: 60)
    max_retries         Max retry attempts on 429/5xx    (default: 3)
"""
from __future__ import print_function

import json
import os
import subprocess
import sys
import tempfile
import time

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


# ---------------------------------------------------------------------------
# Template / path helpers
# ---------------------------------------------------------------------------

def render_template(text, vars_dict):
    """Render a Jinja2 template string using the job vars dict."""
    from jinja2 import Environment
    env = Environment()
    tpl = env.from_string(text)
    render_vars = dict(vars_dict)
    if 'logDate' in render_vars:
        render_vars['log_date'] = render_vars['logDate']
    return tpl.render(**render_vars)


def substitute_path(path, job):
    """Replace {key} placeholders in a string from the job args dict."""
    for k, v in job.items():
        path = path.replace('{' + k + '}', v)
    return path


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def load_credentials(cred_file, cred_key):
    """
    Load credentials from {CREDENTIALS_DIR}/{cred_file}, return the value
    at the top-level key cred_key.
    """
    creds_dir = os.environ.get('CREDENTIALS_DIR', '/tmp/gsbkk_creds')
    cred_path = os.path.join(creds_dir, cred_file)
    if not os.path.exists(cred_path):
        raise IOError('Credential file not found: {0}'.format(cred_path))
    with open(cred_path, 'r') as f:
        all_creds = json.load(f)
    if cred_key not in all_creds:
        raise KeyError('Key "{0}" not found in {1}'.format(cred_key, cred_path))
    return all_creds[cred_key]


# ---------------------------------------------------------------------------
# HTTP request
# ---------------------------------------------------------------------------

def make_request(session, method, url, params, headers, timeout, max_retries):
    """
    Make an HTTP request with exponential back-off on 429 / 5xx errors.
    Returns the parsed JSON response.
    """
    import requests as req_lib

    delay = 1.0
    for attempt in range(max_retries + 1):
        try:
            time.sleep(0.3)  # polite rate limiting between calls
            if method == 'POST':
                resp = session.post(url, headers=headers, json=params, timeout=timeout)
            else:
                resp = session.get(url, headers=headers, params=params, timeout=timeout)

            if resp.status_code == 429:
                if attempt < max_retries:
                    print('Rate limited (429). Retrying in {0}s ...'.format(int(delay)))
                    sys.stdout.flush()
                    time.sleep(delay)
                    delay *= 2
                    continue
                resp.raise_for_status()

            resp.raise_for_status()
            return resp.json()

        except Exception as exc:
            if attempt < max_retries:
                print('Request failed: {0}. Retrying in {1}s ...'.format(exc, int(delay)))
                sys.stdout.flush()
                time.sleep(delay)
                delay *= 2
            else:
                raise


# ---------------------------------------------------------------------------
# Response extraction
# ---------------------------------------------------------------------------

def extract_records(data, response_key):
    """
    Navigate a dot-path into the JSON response and return a list of records.
    If response_key is empty the root is used (wrapped in a list if not already).
    """
    if not response_key:
        return data if isinstance(data, list) else [data]

    for part in response_key.split('.'):
        if isinstance(data, dict):
            data = data.get(part, [])
        else:
            data = []
            break

    return data if isinstance(data, list) else ([data] if data else [])


# ---------------------------------------------------------------------------
# HDFS write
# ---------------------------------------------------------------------------

def write_to_hdfs(records, output_path):
    """
    Serialise records as JSON lines to a temp file, then upload to HDFS.
    Each record occupies one line (compatible with spark.read.json()).
    The output file is placed at output_path/part-00000.json.
    """
    if not records:
        print('No records to write. Skipping HDFS upload.')
        sys.stdout.flush()
        return

    tmp_fd, tmp_path = tempfile.mkstemp(suffix='.json', prefix='api_to_raw_')
    try:
        with os.fdopen(tmp_fd, 'w') as f:
            for rec in records:
                if isinstance(rec, dict):
                    line = json.dumps(rec)
                else:
                    line = json.dumps({'value': rec})
                f.write(line + '\n')

        print('Written {0} records to temp file: {1}'.format(len(records), tmp_path))
        sys.stdout.flush()

        # Create parent directory (idempotent)
        parent = output_path.rsplit('/', 1)[0]
        subprocess.call(['hdfs', 'dfs', '-mkdir', '-p', parent])

        # Upload, overwriting any previous file
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
    import requests as req_lib

    job = parse_args(sys.argv[1:])

    # ---- Required args ----
    url_tpl     = job.get('url_template', job.get('url', ''))
    output_path = job.get('output_path', '')
    log_date    = job.get('logDate', '')

    if not url_tpl:
        print('ERROR: url_template is required', file=sys.stderr)
        sys.exit(1)
    if not output_path:
        print('ERROR: output_path is required', file=sys.stderr)
        sys.exit(1)

    # ---- Optional args ----
    params_raw     = job.get('params', '{}')
    cred_file      = job.get('cred_file', '')
    cred_key       = job.get('cred_key', '')
    auth_param     = job.get('auth_param', 'auth_token')
    auth_header    = job.get('auth_header', '')
    auth_value_key = job.get('auth_value_key', 'api_key')
    response_key   = job.get('response_key', '')
    paginate       = job.get('paginate', 'false').lower() == 'true'
    page_param     = job.get('page_param', 'offset')
    page_size      = int(job.get('page_size', '10000'))
    method         = job.get('method', 'GET').upper()
    timeout        = int(job.get('timeout', '60'))
    max_retries    = int(job.get('max_retries', '3'))

    # ---- Resolve templates and paths ----
    url         = render_template(url_tpl, job)
    output_path = substitute_path(output_path, job)
    params      = json.loads(params_raw) if params_raw and params_raw != '{}' else {}

    print('========================================================')
    print('API to RAW Extractor')
    print('========================================================')
    print('URL         : ' + url)
    print('Output path : ' + output_path)
    print('Log date    : ' + log_date)
    print('Paginate    : ' + str(paginate))
    print('Response key: ' + (response_key or '(root)'))
    print('========================================================')
    sys.stdout.flush()

    # ---- Inject credentials ----
    headers = {}
    if cred_file and cred_key:
        creds = load_credentials(cred_file, cred_key)
        token = (creds.get(auth_value_key)
                 or creds.get('api_key')
                 or creds.get('token', ''))
        if token:
            if auth_header:
                headers[auth_header] = token
            else:
                params[auth_param] = token

    # ---- Fetch data ----
    session = req_lib.Session()
    all_records = []

    if not paginate:
        print('Fetching single page ...')
        sys.stdout.flush()
        data = make_request(session, method, url, params, headers, timeout, max_retries)
        records = extract_records(data, response_key)
        all_records.extend(records)
        print('Fetched {0} record(s).'.format(len(records)))
        sys.stdout.flush()
    else:
        # Offset-based pagination
        offset = 0
        while True:
            page_params = dict(params)
            page_params[page_param] = offset
            page_params['limit']    = page_size

            print('Fetching page offset={0} ...'.format(offset))
            sys.stdout.flush()
            data = make_request(session, method, url, page_params, headers, timeout, max_retries)
            records = extract_records(data, response_key)

            if not records:
                print('No more records. Pagination complete.')
                sys.stdout.flush()
                break

            all_records.extend(records)
            print('offset={0}: got {1} records (running total: {2})'.format(
                offset, len(records), len(all_records)))
            sys.stdout.flush()

            if len(records) < page_size:
                break  # Last page — fewer records than requested

            offset += page_size
            time.sleep(0.5)  # brief pause between pages

    print('Total records: {0}'.format(len(all_records)))
    sys.stdout.flush()

    # ---- Write to HDFS ----
    write_to_hdfs(all_records, output_path)

    print('Done.')
    sys.stdout.flush()


if __name__ == '__main__':
    main()
