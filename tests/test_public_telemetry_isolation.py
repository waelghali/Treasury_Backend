# tests/test_public_telemetry_isolation.py
"""
Unit tests verifying that local/non-production environments are isolated from
production GCS `public_demo_chat_logs/` and that production rejects localhost/loopback
public telemetry beacons.
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints import public_demo_chat as public_demo_chat_ep
from app.services import public_product_expert_service as ppes


def _clean_env_subset(**overrides: str):
    """Creates a clean environment dict with only specified variables set."""
    env = {}
    env.update(overrides)
    return env


class TestPublicTelemetryIsolation(unittest.TestCase):
    def test_is_production_env_false_for_non_prod_indicators(self):
        non_prod_cases = [
            _clean_env_subset(DATABASE_URL="postgresql://u:p@localhost:5432/grow"),
            _clean_env_subset(DATABASE_URL="postgresql://u:p@db-staging.internal/grow_staging", RENDER="true"),
            _clean_env_subset(FLASK_ENV="development", RENDER="true"),
            _clean_env_subset(RENDER_SERVICE_NAME="grow-staging", RENDER="true"),
            _clean_env_subset(ENVIRONMENT="production", PUBLIC_TELEMETRY_GCS_SYNC="false"),
            _clean_env_subset(),  # safe default when nothing indicates production
        ]
        for env_map in non_prod_cases:
            with self.subTest(env=env_map):
                with patch.dict(os.environ, env_map, clear=True):
                    self.assertFalse(ppes.is_production_public_telemetry_env())

    def test_is_production_env_true_for_production_indicators(self):
        prod_cases = [
            _clean_env_subset(
                DATABASE_URL="postgresql://u:p@dpg-xxx/grow_xl8z",
                RENDER="true",
            ),
            _clean_env_subset(
                DATABASE_URL="postgresql://u:p@dpg-xxx/grow_xl8z",
                RENDER="true",
                ENVIRONMENT="production",
            ),
        ]
        for env_map in prod_cases:
            with self.subTest(env=env_map):
                with patch.dict(os.environ, env_map, clear=True):
                    self.assertTrue(ppes.is_production_public_telemetry_env())

    def test_get_gcs_bucket_returns_none_when_not_production(self):
        logger_inst = ppes.PublicDemoChatLogger()
        with patch.dict(
            os.environ,
            {
                "GCS_BUCKET_NAME": "lg_custody_bucket",
                "DATABASE_URL": "postgresql://u:p@localhost:5432/grow",
                "FLASK_ENV": "development",
            },
            clear=True,
        ):
            self.assertIsNone(logger_inst._get_gcs_bucket())

    def test_telemetry_endpoint_ignores_localhost_origin_and_loopback_ip_in_prod(self):
        mini_app = FastAPI()
        mini_app.include_router(public_demo_chat_ep.router, prefix="/api/v1/public-demo")
        client = TestClient(mini_app, client=("127.0.0.1", 50000))

        payload = {
            "event_type": "cta_click",
            "session_id": "pub_test_sess",
            "visitor_id": "vis_test",
            "page": "free_trial",
            "path": "/free-trial-register",
            "metadata": {"target": "free_trial_submit"},
        }

        with patch.object(public_demo_chat_ep, "is_production_public_telemetry_env", return_value=True), \
             patch.object(public_demo_chat_ep.public_demo_chat_logger, "log_telemetry_event") as mock_log:
            # 1. Loopback client IP ("127.0.0.1") even without localhost Origin
            resp_loopback = client.post(
                "/api/v1/public-demo/telemetry",
                json=payload,
                headers={"Origin": "https://www.growbusinessdevelopment.com"},
            )
            self.assertEqual(resp_loopback.status_code, 200)
            self.assertEqual(resp_loopback.json(), {"ok": True})
            mock_log.assert_not_called()

            # 2. Public client IP via X-Forwarded-For, but localhost Origin
            resp_origin = client.post(
                "/api/v1/public-demo/telemetry",
                json=payload,
                headers={
                    "X-Forwarded-For": "197.55.12.34",
                    "Origin": "http://localhost:3000",
                },
            )
            self.assertEqual(resp_origin.status_code, 200)
            self.assertEqual(resp_origin.json(), {"ok": True})
            mock_log.assert_not_called()

            # 3. Public client IP + production Origin -> recorded
            resp_valid = client.post(
                "/api/v1/public-demo/telemetry",
                json=payload,
                headers={
                    "X-Forwarded-For": "197.55.12.34",
                    "Origin": "https://www.growbusinessdevelopment.com",
                },
            )
            self.assertEqual(resp_valid.status_code, 200)
            self.assertEqual(resp_valid.json(), {"ok": True})
            mock_log.assert_called_once()

    def test_local_dev_tagging_and_production_filtering(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(ppes, "PUBLIC_CHAT_LOG_DIR", tmpdir):
                logger_inst = ppes.PublicDemoChatLogger()

                # Write one event in local dev mode (tagged is_local_dev=True)
                with patch.object(ppes, "is_production_public_telemetry_env", return_value=False):
                    logger_inst.log_telemetry_event(
                        event_type="cta_click",
                        session_id="local_sess",
                        visitor_id="local_vis",
                        page="free_trial",
                        path="/free-trial-register",
                        metadata={"target": "free_trial_submit"},
                        ip_hash=ppes.hash_client_ip("197.55.12.99"),
                    )

                date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                ev_file = os.path.join(tmpdir, f"public_events_{date_str}.jsonl")
                with open(ev_file, "r", encoding="utf-8") as fh:
                    rows = [json.loads(line) for line in fh if line.strip()]
                self.assertEqual(len(rows), 1)
                self.assertTrue(rows[0].get("is_local_dev"))

                # Append a pre-existing legacy loopback entry without is_local_dev and one real prod entry
                with open(ev_file, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps({
                        "event_id": "legacy_loopback",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "event_type": "cta_click",
                        "session_id": "old_local_sess",
                        "visitor_id": "old_local_vis",
                        "page": "free_trial",
                        "path": "/free-trial-register",
                        "metadata": {"target": "free_trial_submit"},
                        "ip_hash": ppes.hash_client_ip("127.0.0.1"),
                        "is_bot": False,
                    }) + "\n")
                    fh.write(json.dumps({
                        "event_id": "real_prod",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "event_type": "cta_click",
                        "session_id": "prod_sess",
                        "visitor_id": "prod_vis",
                        "page": "free_trial",
                        "path": "/free-trial-register",
                        "metadata": {"target": "free_trial_submit"},
                        "ip_hash": ppes.hash_client_ip("41.42.43.44"),
                        "country": "EG",
                        "is_bot": False,
                    }) + "\n")

                # Production analytics must ignore both is_local_dev and loopback ip_hash, leaving only 1 trial submission
                with patch.object(ppes, "is_production_public_telemetry_env", return_value=True), \
                     patch.object(logger_inst, "_hydrate_local_file_from_gcs_if_missing", return_value=None):
                    summary = logger_inst.compute_public_analytics(days=1)
                    self.assertEqual(summary["kpis"]["trial_submissions"], 1)
                    self.assertEqual(summary["kpis"]["unique_visitors"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
