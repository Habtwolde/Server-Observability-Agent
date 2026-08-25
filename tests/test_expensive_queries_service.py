import unittest
from unittest.mock import patch

from services.expensive_queries_service import (
    analyze_expensive_query_with_llm,
    build_expensive_query_analysis_messages,
    build_query_analysis_payload,
)


class ExpensiveQueriesServicePromptTests(unittest.TestCase):
    def _payload(self):
        return build_query_analysis_payload(
            server_name="sql-prod-01",
            ingestion_date="2026-06-26",
            snapshot="2026-06-26T00:00:00Z",
            query_type_label="Most expensive queries by IO (Logical Reads)",
            sheet_name="Top Logical Reads Queries",
            query_kind="io",
            query_text="SELECT * FROM dbo.Orders WHERE CONVERT(date, OrderDate) = @OrderDate",
            row={"Total Logical Reads": 2500000, "Execution Count": 42},
            metric_col="Total Logical Reads",
            sort_value=2500000,
            row_index=0,
            total_rows=10,
        )

    def test_analysis_prompt_requires_optimized_query_and_modern_sql_server_rationale(self):
        messages = build_expensive_query_analysis_messages(self._payload())
        combined = "\n".join(m["content"] for m in messages)

        self.assertIn("## Recommended optimized query", combined)
        self.assertIn("fenced ```sql block", combined)
        self.assertIn("## Detailed recommendation rationale", combined)
        self.assertIn("Parameter Sensitive Plan", combined)
        self.assertIn("Query Store", combined)
        self.assertIn("memory grant feedback", combined)
        self.assertIn("CONVERT(date, OrderDate)", combined)

    def test_analyze_expensive_query_uses_enhanced_prompt_and_larger_budget(self):
        with patch("services.expensive_queries_service.chat_completion", return_value="analysis") as chat:
            result = analyze_expensive_query_with_llm(
                server_name="sql-prod-01",
                ingestion_date="2026-06-26",
                snapshot="2026-06-26T00:00:00Z",
                query_type_label="Most expensive queries by CPU (Worker Time)",
                sheet_name="Top Worker Time Queries",
                query_kind="cpu",
                query_text="SELECT COUNT(*) FROM dbo.Orders WHERE CustomerId = @CustomerId",
                row={"Total Worker Time": 999999},
                metric_col="Total Worker Time",
                sort_value=999999,
                row_index=1,
                total_rows=20,
            )

        self.assertEqual(result, "analysis")
        _, kwargs = chat.call_args
        self.assertEqual(kwargs["max_tokens"], 2600)
        sent_prompt = "\n".join(m["content"] for m in chat.call_args.args[0])
        self.assertIn("Recommended optimized query", sent_prompt)
        self.assertIn("Detailed recommendation rationale", sent_prompt)


if __name__ == "__main__":
    unittest.main()
