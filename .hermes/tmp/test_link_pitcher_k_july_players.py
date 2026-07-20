from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).with_name("link_pitcher_k_july_players.py")
SPEC = importlib.util.spec_from_file_location("link_pitcher_k_july_players", MODULE_PATH)
assert SPEC and SPEC.loader
linker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(linker)


class NormalizeNameTests(unittest.TestCase):
    def test_strips_accents_and_normalizes_punctuation(self) -> None:
        self.assertEqual(linker.normalize_name("  José Álvarez-Jr.  "), "josealvarezjr")
        self.assertEqual(linker.normalize_name("D'Arnaud, Jr."), "darnaudjr")

    def test_none_and_punctuation_only_are_empty(self) -> None:
        self.assertEqual(linker.normalize_name(None), "")
        self.assertEqual(linker.normalize_name(" -- ' "), "")


class ResolveUniqueTests(unittest.TestCase):
    def test_resolves_only_one_normalized_candidate(self) -> None:
        players = [(10, "José Álvarez"), (20, "Other Pitcher")]
        result = linker.resolve_player_name("Jose Alvarez", players)
        self.assertEqual(result, linker.Resolution(10, "José Álvarez", "normalized_unique"))

    def test_rejects_ambiguous_normalized_candidates(self) -> None:
        players = [(10, "José Álvarez"), (11, "Jose Alvarez")]
        self.assertIsNone(linker.resolve_player_name("Jose Alvarez", players))

    def test_samuel_aldegheri_alias_resolves_unique_sam(self) -> None:
        players = [(42, "Sam Aldegheri"), (99, "Other Pitcher")]
        result = linker.resolve_player_name("Samuel Aldegheri", players)
        self.assertEqual(result, linker.Resolution(42, "Sam Aldegheri", "explicit_alias_unique"))

    def test_samuel_alias_rejects_duplicate_sam_candidates(self) -> None:
        players = [(42, "Sam Aldegheri"), (43, "Sam Aldegheri")]
        self.assertIsNone(linker.resolve_player_name("Samuel Aldegheri", players))

    def test_no_fuzzy_matching(self) -> None:
        players = [(42, "Sam Aldegheri")]
        self.assertIsNone(linker.resolve_player_name("Samuel Aldeghery", players))


if __name__ == "__main__":
    unittest.main()
