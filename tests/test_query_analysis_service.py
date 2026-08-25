import unittest

from services.query_analysis_service import analyze_query_hotspots, build_query_analysis


class QueryAnalysisServiceTests(unittest.TestCase):
    def test_classifies_logical_reads_with_plan_guardrails(self):
        result = analyze_query_hotspots(
            [
                {
                    "source_sheet": "Top Logical Reads Queries",
                    "bucket": "top_logical_reads",
                    "object_name": "dbo.GetOrders",
                    "database_name": "SalesDB",
                    "metric_name": "Total Logical Reads",
                    "metric_value": "2,500,000",
                    "total_logical_reads": "2500000",
                    "execution_count": "120",
                }
            ],
            context={
                "waits": [{"wait_type": "PAGEIOLATCH_SH", "pct": 42.0}],
                "utilization": {"ple_sec": 200},
                "configuration": {},
                "database_settings": {},
                "backup": {},
            },
        )

        hotspot = result["hotspots"][0]
        self.assertEqual(result["summary"]["primary_pressure_dimension"], "reads")
        self.assertEqual(hotspot["classification"]["primary_dimension"], "reads")
        self.assertIn("high_logical_reads", hotspot["classification"]["patterns"])
        self.assertIn("index_or_predicate_candidate", hotspot["classification"]["patterns"])
        self.assertIn("actual_plan_required", hotspot["classification"]["patterns"])
        self.assertTrue(any("actual execution plan" in x.lower() for x in result["summary"]["limitations"]))

    def test_adds_parallelism_and_cpu_guidance_from_context(self):
        result = analyze_query_hotspots(
            [
                {
                    "source_sheet": "Top Worker Time Queries",
                    "bucket": "top_worker_time",
                    "object_name": "dbo.CalculateInventory",
                    "metric_name": "Total Worker Time",
                    "metric_value": "8000000",
                    "total_worker_time": 8000000,
                    "query_plan": "<ShowPlanXML />",
                }
            ],
            context={
                "waits": [{"wait_type": "CXPACKET", "pct": 30.0}],
                "utilization": {"max_cpu_pct": 92},
                "configuration": {"maxdop": 0, "cost_threshold": 5},
                "database_settings": {},
                "backup": {},
            },
        )

        hotspot = result["hotspots"][0]
        patterns = hotspot["classification"]["patterns"]
        self.assertEqual(hotspot["classification"]["primary_dimension"], "cpu")
        self.assertIn("high_cpu", patterns)
        self.assertIn("parallelism_candidate", patterns)
        self.assertIn("cpu_pressure_correlated", patterns)
        self.assertNotIn("actual_plan_required", patterns)
        methods = {r["methodology"] for r in hotspot["recommendations"]}
        self.assertTrue(any("Glenn Berry" in method or "Brent Ozar" in method for method in methods))

    def test_build_query_analysis_alias_matches_public_api(self):
        hotspots = [{"object_name": "dbo.AliasCheck", "total_worker_time": 5000}]

        direct = analyze_query_hotspots(hotspots)
        alias = build_query_analysis(hotspots)

        self.assertEqual(direct["summary"], alias["summary"])
        self.assertEqual(direct["hotspots"][0]["object_name"], alias["hotspots"][0]["object_name"])
        self.assertEqual(direct["methodology_matrix"], alias["methodology_matrix"])

    def test_empty_hotspots_returns_low_confidence_limitations(self):
        result = analyze_query_hotspots([])
        self.assertEqual(result["summary"]["total_hotspots_analyzed"], 0)
        self.assertEqual(result["summary"]["confidence"], "low")
        self.assertEqual(result["hotspots"], [])
        self.assertEqual(len(result["methodology_matrix"]), 4)


if __name__ == "__main__":
    unittest.main()
