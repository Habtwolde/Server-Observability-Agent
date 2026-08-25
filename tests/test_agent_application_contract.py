from __future__ import annotations

import importlib.util
import os
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AgentApplicationContractTests(unittest.TestCase):
    def test_app_uses_agent_views_instead_of_dashboard_tabs(self):
        source = (ROOT / "app.py").read_text(encoding="utf-8")
        self.assertIn("fleet_priority_view", source)
        self.assertIn("server_diagnosis_view", source)
        self.assertNotIn("overview_tab", source)
        self.assertNotIn("expensive_queries_tab", source)
        self.assertNotIn("windows_events_tab", source)

    def test_source_contract_defaults_to_agent_relations(self):
        module_path = ROOT / "db" / "observability_sources.py"
        spec = importlib.util.spec_from_file_location("agent_sources_under_test", module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)

        self.assertIn("agent_ingestion_runs", module.RUNS_TABLE)
        self.assertIn("v_agent_top_server_findings", module.TOP_FINDINGS_VIEW)
        self.assertIn("v_agent_latest_server_health_summary", module.HEALTH_SUMMARY_VIEW)
        self.assertEqual(module.LEGACY_RELATION_REPLACEMENTS, {})

    def test_ai_prompt_is_explicitly_evidence_grounded(self):
        source = (ROOT / "services" / "agent_ai_service.py").read_text(encoding="utf-8")
        self.assertIn("Use only the supplied deterministic findings", source)
        self.assertIn("Never invent a", source)
        self.assertIn("If the data is incomplete", source)


if __name__ == "__main__":
    unittest.main()

