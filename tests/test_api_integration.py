from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app.main import app
from app.models import SearchJobStatus
from tests.test_platform_features import _sample_response


class ApiIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_search_markdown_endpoint_returns_markdown(self) -> None:
        fake_pipeline = AsyncMock()
        fake_pipeline.run = AsyncMock(return_value=_sample_response())

        with patch("app.main.get_pipeline", return_value=fake_pipeline):
            response = self.client.get("/api/search.md", params={"query": "open source database tools"})

        self.assertEqual(response.status_code, 200)
        self.assertIn("# Agentic Search Report: open source database tools", response.text)
        self.assertEqual(response.headers["content-type"].split(";")[0], "text/markdown")

    def test_job_endpoints_return_mocked_status(self) -> None:
        fake_manager = AsyncMock()
        created_status = SearchJobStatus(
            job_id="job-123",
            query="open source database tools",
            status="queued",
            stage="queued",
            progress=0.0,
            message="Queued",
            created_at="2026-04-04T00:00:00+00:00",
            updated_at="2026-04-04T00:00:00+00:00",
        )
        completed_status = SearchJobStatus(
            job_id="job-123",
            query="open source database tools",
            status="completed",
            stage="completed",
            progress=1.0,
            message="Done",
            created_at="2026-04-04T00:00:00+00:00",
            updated_at="2026-04-04T00:00:01+00:00",
            result=_sample_response(),
        )
        fake_manager.create_job = AsyncMock(return_value=created_status)
        fake_manager.get_job = AsyncMock(return_value=completed_status)

        with patch("app.main.get_job_manager", return_value=fake_manager):
            create_response = self.client.post("/api/jobs", params={"query": "open source database tools"})
            status_response = self.client.get("/api/jobs/job-123")

        self.assertEqual(create_response.status_code, 202)
        self.assertEqual(create_response.json()["job_id"], "job-123")
        self.assertEqual(status_response.status_code, 200)
        self.assertEqual(status_response.json()["status"], "completed")
        self.assertEqual(status_response.json()["result"]["query"], "open source database tools")


if __name__ == "__main__":
    unittest.main()
