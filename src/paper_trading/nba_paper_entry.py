"""Small provenance/eligibility boundary for the flat-unit forward evaluation."""

import hashlib
import json
import math
from datetime import UTC, datetime

from sqlalchemy import text

from src.config.combo_config import STAT_TO_MARKET
from src.models.nba_artifacts import validate_artifact_bundle
from src.models.nba_paper_policy import EVALUATION_RUN_ID as EVALUATION_RUN_ID
from src.models.nba_paper_policy import NBA_PAPER_POLICY, PROMOTED_POLICY

FIXED_STAKE = NBA_PAPER_POLICY.fixed_stake


def artifact_snapshot(model_path):
    # Existing deployments may predate manifests. Hash actual files, label that
    # state honestly, and never claim legacy training metadata was verified.
    return validate_artifact_bundle(model_path, allow_legacy=True).to_plan_dict()


def model_snapshot(artifact, *, n_samples=NBA_PAPER_POLICY.n_samples):
    return {"artifact": artifact, "n_samples": n_samples,
            "produced_at": datetime.now(UTC).isoformat()}


def _time(value):
    value = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Missing timezone-aware quote clock")
    return value


def bind_model_samples(frame, samples, *, verify=False):
    """Bind artifact evidence to actual arrays across separate prediction/sample writes."""
    if 'model_snapshot' not in frame:
        return frame
    for idx, row in frame.iterrows():
        sample = samples.get((row['player_id'], row['game_id'], row['stat']))
        model = row.get('model_snapshot')
        if sample is None or not isinstance(model, dict):
            continue
        digest = hashlib.sha256(sample.astype('float64').tobytes()).hexdigest()
        if verify:
            if model.get('sample_sha256') != digest:
                frame.at[idx, 'model_snapshot'] = None
        else:
            frame.at[idx, 'model_snapshot'] = {**model, 'sample_sha256': digest}
    return frame


def annotate_quote_decisions(frame, samples, config):
    """Record actual empirical decision inputs without changing edge formulas."""
    if 'quote_snapshot' not in frame:
        return frame
    now = datetime.now(UTC).isoformat()
    for idx, row in frame.iterrows():
        quote = row.get('quote_snapshot')
        sample = samples.get((row['player_id'], row['game_id'], row['stat']))
        if not isinstance(quote, dict) or sample is None or len(sample) == 0:
            frame.at[idx, 'quote_snapshot'] = None
            continue
        frame.at[idx, 'quote_snapshot'] = {
            **quote, 'quote_read_at': quote['decision_time'], 'decision_time': now,
            'probability_source': 'empirical_samples', 'sample_count': len(sample),
            'bl_config': config,
        }
    return frame


def _valid_model(model):
    if not isinstance(model, dict):
        return False
    if 'components' in model:
        return bool(model['components']) and all(_valid_model(v) for v in model['components'].values())
    artifact = model.get('artifact')
    return isinstance(artifact, dict) and bool(artifact.get('identity')) and bool(artifact.get('file_hashes'))


def valid_snapshot(row):
    """Validate stored producer evidence, not a substitute latest-quote lookup."""
    quote, model = row.get("quote_snapshot"), row.get("model_snapshot")
    if not isinstance(quote, dict) or not isinstance(model, dict):
        return False
    if not _valid_model(model) or not model.get('sample_sha256'):
        return False
    if quote.get('probability_source') != 'empirical_samples' or not quote.get('sample_count'):
        return False
    if quote.get("bl_config") != NBA_PAPER_POLICY.bl_config():
        return False
    if PROMOTED_POLICY and model.get("artifact", {}).get("identity") != PROMOTED_POLICY["model_identity"]:
        return False
    try:
        decision = _time(quote["decision_time"])
        sides = {q["outcome_label"]: q for q in quote["rows"]}
        if len(quote["rows"]) != 2 or set(sides) != {"Over", "Under"}:
            return False
        over, under = sides["Over"], sides["Under"]
        capture = quote.get("capture")
        if capture is not None:
            if not capture.get("capture_id") or not (_time(over["source_snapshot_time"]) <= _time(capture["confirmed_at"]) <= decision):
                return False
            for key in ("market_last_update", "bookmaker_last_update"):
                if capture.get(key) and _time(capture[key]) > _time(capture["confirmed_at"]):
                    return False
        for key in ("api_game_id", "game_id", "player_id", "bookmaker", "market_key", "line", "source_snapshot_time", "commence_time"):
            if over[key] != under[key] or over[key] is None:
                return False
        if (over["game_id"] != row["game_id"] or over["player_id"] != row["player_id"]
                or over["market_key"] != STAT_TO_MARKET.get(row["stat"])
                or over["bookmaker"] != row.get("bookmaker") or float(over["line"]) != float(row["line"])):
            return False
        for side, q in sides.items():
            odds = float(q["odds_american"])
            if not math.isfinite(odds) or odds == 0 or odds != float(row[f"{side.lower()}_odds"]):
                return False
            start = _time(q["commence_time"])
            if not (_time(q["source_snapshot_time"]) <= _time(q["observed_at"]) <= decision < start):
                return False
            for clock in ("market_last_update", "bookmaker_last_update"):
                if q.get(clock) and _time(q[clock]) > decision:
                    return False
        return True
    except (KeyError, TypeError, ValueError):
        return False


def eligible_at_boundary(conn, row):
    """Lock only the selected fixture and two quote rows; no network fallback.

    Reuses the producer's 48-hour quote and 90-minute current-day / 48-hour
    future-day schedule semantics. The INSERT also checks the wall clock.
    """
    if not valid_snapshot(row):
        return False
    from src.processing.nba_quote_capture import has_capture_surface, pair_in_capture

    quotes = row["quote_snapshot"]["rows"]
    migrated = has_capture_surface(conn)
    latest = None
    if migrated:
        # Same event lock as the writer; hold it through the placement transaction.
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                     {"key": "nba-live:" + quotes[0]["api_game_id"]})
        latest = conn.execute(text("""SELECT capture_id, received_at, groups, commence_time
            FROM nba_props_live_captures WHERE api_game_id=:api_game_id
            ORDER BY received_at DESC, capture_id DESC LIMIT 1"""), quotes[0]).fetchone()
        if latest is not None:
            if not pair_in_capture(latest[2], quotes):
                return False
            confirmed = _time(latest[1])
            if not (datetime.now(UTC).timestamp() - 48 * 3600 <= confirmed.timestamp() <= datetime.now(UTC).timestamp()):
                return False
            capture = row["quote_snapshot"].get("capture")
            if not capture:
                return False
            evidence = conn.execute(text("""SELECT groups FROM nba_props_live_captures
                WHERE capture_id=:capture_id AND api_game_id=:api_game_id
                  AND received_at=CAST(:confirmed_at AS timestamptz)
                  AND received_at<=CAST(:decision_time AS timestamptz)"""),
                {**capture, "api_game_id": quotes[0]["api_game_id"],
                 "decision_time": row["quote_snapshot"]["decision_time"]}).fetchone()
            if evidence is None or not pair_in_capture(evidence[0], quotes):
                return False
            group = next(g for g in evidence[0] if g["key"] == [quotes[0]["api_player_name"], quotes[0]["bookmaker"], quotes[0]["market_key"]])
            for field in ("market_last_update", "bookmaker_last_update"):
                stored, supplied = group.get(field), capture.get(field)
                if (stored is None) != (supplied is None) or (stored is not None and _time(stored) != _time(supplied)):
                    return False
        elif row["quote_snapshot"].get("capture"):
            return False
    schedule = conn.execute(text("""
        SELECT commence_time_utc FROM nba_game_schedule
        WHERE game_id = :game_id AND game_date = :game_date
          AND game_status = 1 AND source = 'nba_cdn'
          AND game_type IN ('002', '004', '005')
          AND home_team_id IS NOT NULL AND away_team_id IS NOT NULL
          AND commence_time_utc > clock_timestamp()
          AND COALESCE(postponed_status, 'N') IN ('', 'N')
          AND LOWER(COALESCE(status_text, '')) NOT SIMILAR TO '%(postpon|cancel)%'
          AND source_updated_at <= clock_timestamp() AND observed_at <= clock_timestamp()
          AND observed_at >= clock_timestamp() - make_interval(mins =>
              CASE WHEN game_date <= (clock_timestamp() AT TIME ZONE 'America/New_York')::date THEN 90 ELSE 2880 END)
          AND source_updated_at >= clock_timestamp() - make_interval(mins =>
              CASE WHEN game_date <= (clock_timestamp() AT TIME ZONE 'America/New_York')::date THEN 90 ELSE 2880 END)
        FOR SHARE
    """), row).fetchone()
    if schedule is None:
        return False
    if any(_time(q["commence_time"]) != schedule[0] for q in quotes):
        return False
    actual = conn.execute(text("""
        SELECT to_jsonb(q) AS quote FROM nba_player_props_inference q
        WHERE staging_id = ANY(:ids)
          AND (:confirmed OR observed_at >= clock_timestamp() - interval '48 hours')
          AND observed_at <= clock_timestamp() AND source_snapshot_time <= clock_timestamp()
          AND source_snapshot_time < commence_time AND observed_at < commence_time
          AND (market_last_update IS NULL OR market_last_update <= clock_timestamp())
          AND (bookmaker_last_update IS NULL OR bookmaker_last_update <= clock_timestamp())
        ORDER BY staging_id FOR SHARE
    """), {"ids": [q["staging_id"] for q in quotes], "confirmed": latest is not None}).fetchall()
    if sorted((r[0] for r in actual), key=lambda q: q["staging_id"]) != sorted(quotes, key=lambda q: q["staging_id"]):
        return False
    if latest is not None:
        return _time(latest[3]) == schedule[0]
    # An updated quote for the selected book/line must first flow through edge
    # calculation. Do not relabel it as the original prediction's evidence.
    newer = conn.execute(text("""
        SELECT 1 FROM nba_player_props_inference
        WHERE game_id = :game_id AND player_id = :player_id
          AND market_key = :market_key AND bookmaker = :bookmaker AND line = :line
          AND source_snapshot_time > :source_snapshot_time
          AND observed_at <= clock_timestamp() LIMIT 1
    """), quotes[0]).fetchone()
    return newer is None


def insert_bet(conn, bet):
    row = bet["entry_snapshot"]["prediction"]
    if not eligible_at_boundary(conn, row):
        return False
    params = {**bet, "entry_snapshot": json.dumps(bet["entry_snapshot"]),
              "start": row["quote_snapshot"]["rows"][0]["commence_time"]}
    inserted = conn.execute(text("""
        INSERT INTO paper_bets (
            prediction_id, game_date, game_id, player_id, player_name, stat_type,
            line, bet_direction, odds_at_bet, implied_prob, model_prob, edge,
            stake, kelly_fraction, status, evaluation_run_id, entry_snapshot, placed_at
        ) SELECT :prediction_id, :game_date, :game_id, :player_id, :player_name, :stat_type,
            :line, :bet_direction, :odds_at_bet, :implied_prob, :model_prob, :edge,
            10.0, 0, 'pending', :evaluation_run_id, CAST(:entry_snapshot AS jsonb), clock_timestamp()
        WHERE clock_timestamp() < CAST(:start AS timestamptz)
        ON CONFLICT DO NOTHING RETURNING id
    """), params).fetchone()
    return inserted is not None
