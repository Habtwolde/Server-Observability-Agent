import unittest

from ui.assistant_panel import _message_content_html, _remove_assistant_markdown_artifacts


class AssistantPanelRenderingTests(unittest.TestCase):
    def test_removes_common_markdown_asterisks_from_assistant_output(self):
        html = _message_content_html("**Highest risk**\n* Check waits\n* Review top queries")

        self.assertIn("Highest risk", html)
        self.assertIn("Check waits", html)
        self.assertIn("Review top queries", html)
        self.assertNotIn("**", html)
        self.assertNotIn("* Check", html)

    def test_preserves_sql_wildcards_and_count_star(self):
        text = "Run SELECT * FROM sys.dm_exec_requests and compare COUNT(*) by status."

        self.assertEqual(_remove_assistant_markdown_artifacts(text), text)

    def test_escapes_html_after_cleanup(self):
        html = _message_content_html("**Risk** <script>alert(1)</script>")

        self.assertIn("Risk", html)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", html)
        self.assertNotIn("<script>", html)


if __name__ == "__main__":
    unittest.main()
