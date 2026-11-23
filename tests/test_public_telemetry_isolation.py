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
from datetime import datetime, timedelta, timezone
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

    def test_owner_visitor_exclusion_retroactive_and_forward(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(ppes, "PUBLIC_CHAT_LOG_DIR", tmpdir), \
                 patch.object(ppes, "is_production_public_telemetry_env", return_value=True), \
                 patch.object(ppes.PublicDemoChatLogger, "_get_gcs_bucket", return_value=None):
                logger_inst = ppes.PublicDemoChatLogger()

                # 1. Owner visits '/' before logging in (recorded initially)
                logger_inst.log_telemetry_event(
                    event_type="page_view",
                    session_id="owner_sess_morning",
                    visitor_id="vis_owner_chrome",
                    page="landing",
                    path="/",
                    referrer_source="Direct",
                    device_type="desktop",
                    country="SA",
                    ip_hash=ppes.hash_client_ip("51.36.10.20"),
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130.0",
                )
                # Plus an old chat question from that same owner session
                logger_inst.log_exchange(
                    session_id="owner_sess_morning",
                    visitor_id="",
                    page_context="landing",
                    lang="en",
                    question="Test owner question",
                    answer="Test answer",
                    topic="general",
                    source_mode="curated_kb",
                    actions=[],
                    show_commercial_cta=False,
                    ip_hash=ppes.hash_client_ip("51.36.10.20"),
                    user_agent="Mozilla/5.0 Chrome/130.0",
                    latency_ms=25.0,
                )

                # 2. Real external prospect visits '/' and '/demo' across 2 sessions
                for s_id, pg, pth in [
                    ("prospect_sess_1", "landing", "/"),
                    ("prospect_sess_2", "demo", "/demo"),
                ]:
                    logger_inst.log_telemetry_event(
                        event_type="page_view",
                        session_id=s_id,
                        visitor_id="vis_real_prospect",
                        page=pg,
                        path=pth,
                        referrer_source="LinkedIn",
                        device_type="desktop",
                        country="AE",
                        ip_hash=ppes.hash_client_ip("94.200.10.50"),
                        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) Safari/605.1",
                    )

                # Before registering exclusion: 2 visitors (owner + prospect)
                before = logger_inst.compute_public_analytics(days=1)
                self.assertEqual(before["kpis"]["unique_visitors"], 2)

                # Register owner's visitor_id -> retroactively removes morning visit + chat, and blocks future writes
                self.assertTrue(logger_inst.register_excluded_visitor("vis_owner_chrome"))
                logger_inst.log_telemetry_event(
                    event_type="page_view",
                    session_id="owner_sess_afternoon",
                    visitor_id="vis_owner_chrome",
                    page="landing",
                    path="/",
                    ip_hash=ppes.hash_client_ip("51.36.10.20"),
                    user_agent="Mozilla/5.0 Chrome/130.0",
                )
                # Explicit is_internal=True also blocked even with a brand-new visitor_id
                logger_inst.log_telemetry_event(
                    event_type="page_view",
                    session_id="internal_sess",
                    visitor_id="vis_other_staff",
                    page="landing",
                    path="/",
                    ip_hash=ppes.hash_client_ip("51.36.10.99"),
                    user_agent="Mozilla/5.0 Chrome/130.0",
                    is_internal=True,
                )

                after = logger_inst.compute_public_analytics(days=1)
                self.assertEqual(after["kpis"]["unique_visitors"], 1)
                self.assertEqual(after["kpis"]["total_sessions"], 2)
                self.assertEqual(after["kpis"]["total_pageviews"], 2)
                self.assertEqual(after["kpis"]["total_chat_questions"], 0)
                # Unified journey row: 1 row for the 1 prospect, showing session_count == 2
                self.assertEqual(len(after["recent_sessions"]), 1)
                self.assertEqual(after["recent_sessions"][0]["session_count"], 2)
                self.assertEqual(after["source_breakdown"], [{"source": "LinkedIn", "sessions": 1}])
                self.assertEqual(after["device_breakdown"], [{"device": "desktop", "sessions": 1}])
                self.assertEqual(after["language_breakdown"], {"en": 1, "ar": 0})

    def test_expanded_bot_filtering_and_rolling_24h_window(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(ppes, "PUBLIC_CHAT_LOG_DIR", tmpdir), \
                 patch.object(ppes, "is_production_public_telemetry_env", return_value=True), \
                 patch.object(ppes.PublicDemoChatLogger, "_get_gcs_bucket", return_value=None):
                logger_inst = ppes.PublicDemoChatLogger()

                # Bot UAs that previously slipped past _BOT_UA_RE (e.g. BingPreview, AdsBot-Google, GPTBot)
                for bot_ua in (
                    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534+ (KHTML, like Gecko) BingPreview/1.0b",
                    "AdsBot-Google (+http://www.google.com/adsbot.html)",
                    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; GPTBot/1.1",
                    "Mozilla/5.0 Chrome-Lighthouse",
                ):
                    self.assertTrue(ppes.is_bot_user_agent(bot_ua), f"Expected bot match for {bot_ua}")
                    logger_inst.log_telemetry_event(
                        event_type="page_view",
                        session_id="bot_sess",
                        visitor_id="vis_bot",
                        page="landing",
                        path="/",
                        referrer_source="Bing Search",
                        device_type="tablet",
                        country="KW",
                        ip_hash=ppes.hash_client_ip("65.55.210.10"),
                        user_agent=bot_ua,
                    )

                now = datetime.now(timezone.utc)
                yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
                yesterday_file = os.path.join(tmpdir, f"public_events_{yesterday_str}.jsonl")
                with open(yesterday_file, "w", encoding="utf-8") as yf:
                    # 1. Event 6 hours ago (even if in yesterday's UTC file) -> MUST count in 24h window
                    yf.write(json.dumps({
                        "event_id": "recent_24h_ev",
                        "timestamp": (now - timedelta(hours=6)).isoformat(),
                        "event_type": "page_view",
                        "session_id": "sess_6h_ago",
                        "visitor_id": "vis_6h_ago",
                        "page": "landing",
                        "path": "/",
                        "referrer_source": "Google Search",
                        "device_type": "mobile",
                        "lang": "ar",
                        "country": "EG",
                        "ip_hash": ppes.hash_client_ip("41.44.55.66"),
                        "is_bot": False,
                    }) + "\n")
                    # 2. Event 30 hours ago -> MUST NOT count in 24h window (days=1), MUST count in 7d window
                    yf.write(json.dumps({
                        "event_id": "older_30h_ev",
                        "timestamp": (now - timedelta(hours=30)).isoformat(),
                        "event_type": "page_view",
                        "session_id": "sess_30h_ago",
                        "visitor_id": "vis_30h_ago",
                        "page": "landing",
                        "path": "/",
                        "referrer_source": "Direct",
                        "device_type": "desktop",
                        "lang": "en",
                        "country": "SA",
                        "ip_hash": ppes.hash_client_ip("51.39.11.22"),
                        "is_bot": False,
                    }) + "\n")

                win_24h = logger_inst.compute_public_analytics(days=1)
                self.assertEqual(win_24h["kpis"]["unique_visitors"], 1)
                self.assertEqual(win_24h["kpis"]["bot_events_filtered"], 4)
                self.assertEqual(win_24h["recent_sessions"][0]["visitor_id"], "vis_6h_ago")

                win_7d = logger_inst.compute_public_analytics(days=7)
                self.assertEqual(win_7d["kpis"]["unique_visitors"], 2)

    def test_admin_analytics_endpoint_with_exclude_visitor_id(self):
        mini_app = FastAPI()
        mini_app.include_router(public_demo_chat_ep.router, prefix="/api/v1/public-demo")
        mini_app.dependency_overrides[public_demo_chat_ep.get_current_system_owner] = lambda: MagicMock(
            user_id=1, role="system_owner"
        )
        client = TestClient(mini_app)

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_logger = ppes.PublicDemoChatLogger()
            with patch.object(ppes, "PUBLIC_CHAT_LOG_DIR", tmpdir), \
                 patch.object(ppes.PublicDemoChatLogger, "_get_gcs_bucket", return_value=None), \
                 patch.object(public_demo_chat_ep, "public_demo_chat_logger", temp_logger):
                resp = client.get(
                    "/api/v1/public-demo/admin/analytics?days=14&exclude_visitor_id=vis_mwkyqxga_msw8onifv"
                )
                self.assertEqual(resp.status_code, 200)
                body = resp.json()
                self.assertEqual(body["kpis"]["excluded_internal_visitors"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
