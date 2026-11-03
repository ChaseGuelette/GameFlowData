"""Explicit historical attachments; filesystem-only planning, never source certification."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date
from pathlib import Path

from src.models.features.nba.contracts import NBA_FEATURE_FAMILIES
from src.models.features.nba.historical_policy import HistoricalPolicyBlockedError, policy_from_json, policy_payload
from src.models.nba_artifacts import canonical_sha256

# Additional implementation identities for the historical route, not legacy v1.
HISTORICAL_SOURCE_FILES = (
    "src/backtesting/nba/historical_execution.py",
    "src/backtesting/nba/historical_quote_adapter.py",
    "src/backtesting/nba/historical_replay.py",
    "src/models/features/nba/contracts.py",
    "src/models/features/nba/historical_policy.py",
    "src/models/features/nba/historical_observation_adapters.py",
    "src/models/features/nba/observation_policy.py",
    "src/models/features/nba/quote_selection.py",
    "src/models/features/nba/historical_sources.py",
    "src/models/features/nba/historical_loader.py",
    "src/models/features/nba/requests.py",
    "src/models/features/nba/training_loader.py",
    "src/models/features/nba/date_batch_loader.py",
    "src/models/features/nba/date_range_loader.py",
    "src/models/features/nba/line_sources.py",
    "src/models/features/nba/injury_context.py",
)


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate historical JSON key')
        result[key] = value
    return result


def _json(raw):
    return json.loads(raw, object_pairs_hook=_unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite historical JSON')))


def _path(token):
    if type(token) is not str or not token or token != token.strip() or any(ord(c) < 32 for c in token):
        raise ValueError('Historical input requires an explicit path')
    return str(Path(token).resolve(strict=True))


def validate_saved_attachment(receipt):
    """Validate saved data in memory only; never resolve its recorded paths."""
    if type(receipt) is not dict or set(receipt) != {'schema_version', 'policy', 'source_metadata'}:
        raise ValueError('Malformed historical attachment')
    if type(receipt['schema_version']) is not int or receipt['schema_version'] != 1:
        raise ValueError('Unsupported historical attachment version')
    saved = receipt['policy']
    if type(saved) is not dict or set(saved) != {'path', 'sha256', 'payload', 'content_id'}:
        raise ValueError('Malformed historical policy receipt')
    policy = policy_from_json(json.dumps(saved['payload'], allow_nan=False))
    try:
        policy.require_execution_ready(required_families=tuple(sorted(NBA_FEATURE_FAMILIES)))
    except HistoricalPolicyBlockedError as exc:
        raise ValueError(str(exc)) from None
    if saved['content_id'] != policy.content_id or saved['payload'] != policy_payload(policy):
        raise ValueError('Historical policy identity mismatch')
    metadata = receipt['source_metadata']
    if type(metadata) is not dict or set(metadata) != {'mode', 'path', 'sha256'}:
        raise ValueError('Malformed historical metadata receipt')
    if metadata['mode'] not in ('embedded', 'sidecar'):
        raise ValueError('Unsupported historical metadata mode')
    inputs = [saved]
    if metadata['mode'] == 'embedded':
        if metadata['path'] is not None or metadata['sha256'] is not None:
            raise ValueError('Embedded metadata cannot claim a sidecar identity')
    else:
        inputs.append(metadata)
    for item in inputs:
        if (type(item['path']) is not str or not item['path'] or '\x00' in item['path']
                or type(item['sha256']) is not str or re.fullmatch('[0-9a-f]{64}', item['sha256']) is None):
            raise ValueError('Malformed historical file identity')
    return policy


def load_attachment(policy_path=None, metadata_path=None, *, local=False):
    if policy_path is None:
        if metadata_path is not None:
            raise ValueError('Historical source metadata requires a historical policy')
        return None
    if local is not True:
        raise ValueError('Historical execution requires explicit local targeting')
    path = _path(policy_path)
    raw = Path(path).read_bytes()
    policy = policy_from_json(raw.decode('utf-8'))
    metadata = {'mode': 'embedded', 'path': None, 'sha256': None}
    if metadata_path is not None:
        source = _path(metadata_path)
        content = Path(source).read_bytes()
        payload = _json(content.decode('utf-8'))
        if type(payload) is not dict or set(payload) != {'records'} or type(payload['records']) is not list:
            raise ValueError('Historical metadata requires a records array')
        keys = set()
        for record in payload['records']:
            if (type(record) is not dict or set(record) != {'table', 'key', 'metadata'}
                    or type(record['table']) is not str or not record['table']
                    or type(record['key']) is not list or not record['key']
                    or any(type(key) not in (str, int) for key in record['key'])
                    or type(record['metadata']) is not dict):
                raise ValueError('Malformed historical metadata record')
            key = (record['table'], tuple(record['key']))
            if key in keys:
                raise ValueError('Duplicate historical metadata record')
            keys.add(key)
        metadata = {'mode': 'sidecar', 'path': source, 'sha256': hashlib.sha256(content).hexdigest()}
    receipt = {'schema_version': 1,
               'policy': {'path': path, 'sha256': hashlib.sha256(raw).hexdigest(),
                          'payload': policy_payload(policy), 'content_id': policy.content_id},
               'source_metadata': metadata}
    validate_saved_attachment(receipt)
    return receipt


def recheck_attachment(receipt):
    if receipt is None:
        return None
    validate_saved_attachment(receipt)
    current = load_attachment(receipt['policy']['path'], receipt['source_metadata']['path'], local=True)
    if canonical_sha256(current) != canonical_sha256(receipt):
        raise ValueError('Historical attachment changed since planning')
    return validate_saved_attachment(receipt)


def validate_window(receipt, start, end):
    """Replay must remain inside a declared evaluation window, never train/calibration."""
    if receipt is None:
        return
    validate_saved_attachment(receipt)
    cohort = receipt['policy']['payload']['authorities']['cohort_split']['value']
    windows = [(cohort['oos_start'], cohort['oos_end'])]
    if cohort['independent_validation'] is not None:
        independent = cohort['independent_validation']
        windows.append((independent['start'], independent['end']))
    if not any(date.fromisoformat(first) <= start <= end <= date.fromisoformat(last) for first, last in windows):
        raise ValueError('Historical replay lies outside declared evaluation windows')


def validate_artifact_window(inspection, start):
    if inspection.state != 'verified_manifest' or inspection.manifest is None:
        raise ValueError('Historical replay requires an exact nonlegacy manifested artifact')
    cutoff = inspection.manifest['calibration']['end_date_exclusive']
    if start < date.fromisoformat(cutoff):
        raise ValueError('Historical replay overlaps actual artifact calibration')


def source_policy(receipt):
    if receipt is None:
        return {'source_id': 'unknown', 'policy_id': 'unknown', 'watermark': None}
    validate_saved_attachment(receipt)
    return {'schema_version': 2, 'attachment': receipt,
            'policy_id': receipt['policy']['content_id'],
            'metadata_identity': canonical_sha256(receipt['source_metadata']), 'watermark': None}


def make_sources(engine, receipt):
    """Runtime-only adapter; caller creates the engine after preflight."""
    recheck_attachment(receipt)
    from src.models.features.nba.historical_sources import PostgresHistoricalSources

    reader = PostgresHistoricalSources(engine, metadata_path=receipt['source_metadata']['path'])
    recheck_attachment(receipt)
    return reader
