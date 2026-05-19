import pytest

from scripts import link_mlb_clv_snapshots as linker


def test_execute_requires_explicit_max_batches():
    parser = linker.build_parser()
    args = parser.parse_args(["--execute"])

    with pytest.raises(SystemExit):
        linker.validate_args(args)


def test_large_batch_count_requires_large_run_flag():
    parser = linker.build_parser()
    args = parser.parse_args(["--execute", "--max-batches", "101"])

    with pytest.raises(SystemExit):
        linker.validate_args(args)


def test_preflight_mode_is_default_and_non_writing():
    parser = linker.build_parser()
    args = parser.parse_args([])
    linker.validate_args(args)

    assert args.mode == "preflight"
    assert args.execute is False
    assert args.max_batches == 0
    assert args.sample_rows == 0


def test_chunk_update_sql_uses_bounded_batch_table_not_full_table_regex_update():
    sql = linker.build_player_update_sql("mlb_player_props_clv_snapshots", "id")

    assert "tmp_mlb_clv_batch_ids" in sql
    assert "JOIN tmp_mlb_clv_batch_ids" in sql or "FROM tmp_mlb_clv_batch_ids" in sql
    assert "c.id = b.id" in sql or 'c."id" = b.id' in sql
    assert "LIMIT" not in sql  # limit belongs in batch-id selection, not the UPDATE itself
    assert "c.player_id IS NULL" in sql


def test_batch_id_selection_is_bounded_and_ordered():
    sql = linker.build_batch_id_sql("mlb_player_props_clv_snapshots", "id", link_games=True, link_players=True)

    assert "LIMIT :batch_size" in sql
    assert "ORDER BY id" in sql or 'ORDER BY "id"' in sql
    assert "id > :last_id" in sql or '"id" > :last_id' in sql
    assert "FOR UPDATE SKIP LOCKED" in sql


def test_table_name_validation_rejects_injection():
    with pytest.raises(ValueError):
        linker.quote_ident("mlb_player_props_clv_snapshots; drop table x")


def test_event_map_uses_team_names_to_disambiguate_same_time_games():
    source = linker.create_event_map_for_batch.__code__.co_consts
    sql_text = "\n".join(str(value) for value in source if isinstance(value, str))

    assert "public.mlb_teams ht" in sql_text
    assert "ht.team_name = e.home_team" in sql_text
    assert "public.mlb_teams at" in sql_text
    assert "at.team_name = e.away_team" in sql_text
