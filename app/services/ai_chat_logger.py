# app/services/ai_chat_logger.py
"""
AI Chat Logger Service
Automatically logs user questions, AI responses, architectural levels, intents, and metadata.
Saves logs to:
1. Local lightweight text/jsonl files (Immediate, 100% Offline, Non-Blocking).
2. Google Cloud Storage (GCS) (Asynchronous background worker, never blocks HTTP thread, silent offline fallback).
"""

import os
import json
import logging
import threading
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

LOCAL_LOG_DIR = os.path.join(os.getcwd(), "uploads", "ai_chat_logs")


class AIChatLogger:
    """
    Manages dual-tier chat logging (Local Files + Asynchronous Background GCS Sync).
    """

    def __init__(self):
        self._ensure_local_dir()

    def _ensure_local_dir(self):
        try:
            os.makedirs(LOCAL_LOG_DIR, exist_ok=True)
        except Exception as e:
            logger.warning(f"Could not create local chat log directory: {e}")

    def log_interaction(
        self,
        customer_id: int,
        user_id: int,
        question: str,
        answer: str,
        level: int,
        intent: Optional[str] = None,
        source_awareness: Optional[str] = None,
        card_id: Optional[str] = None,
        references: Optional[List[Dict[str, Any]]] = None,
        error: Optional[str] = None,
        execution_time_ms: Optional[float] = None
    ) -> None:
        """
        Logs a single chat interaction instantly locally, and schedules GCS sync asynchronously without blocking.
        """
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        timestamp_iso = now.isoformat()

        log_payload = {
            "timestamp": timestamp_iso,
            "customer_id": customer_id,
            "user_id": user_id,
            "card_id": card_id,
            "question": question,
            "answer": answer,
            "level": level,
            "intent": intent,
            "source_awareness": source_awareness,
            "references_count": len(references) if references else 0,
            "error": error,
            "execution_time_ms": execution_time_ms
        }

        # 1. Write to Local JSONL and TXT files (Instant <1ms local disk I/O)
        try:
            self._write_local_logs(date_str, timestamp_iso, customer_id, user_id, card_id, question, answer, level, intent, log_payload)
        except Exception as e:
            logger.error(f"Failed to write local chat log: {e}")

        # 2. Upload/Sync to Google Cloud Storage (GCS) in a background thread to NEVER block HTTP response
        try:
            thread = threading.Thread(
                target=self._upload_to_gcs_safe,
                args=(customer_id, date_str, log_payload),
                daemon=True
            )
            thread.start()
        except Exception as e:
            logger.debug(f"Could not spawn background GCS logger thread: {e}")

    def _write_local_logs(
        self,
        date_str: str,
        timestamp_iso: str,
        customer_id: int,
        user_id: int,
        card_id: Optional[str],
        question: str,
        answer: str,
        level: int,
        intent: Optional[str],
        log_payload: Dict[str, Any]
    ):
        self._ensure_local_dir()

        # A. JSONL file (machine-readable, line-delimited JSON)
        jsonl_path = os.path.join(LOCAL_LOG_DIR, f"chat_logs_{date_str}.jsonl")
        with open(jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_payload, ensure_ascii=False) + "\n")

        # B. TXT file (human-readable transcript for quick user review)
        txt_path = os.path.join(LOCAL_LOG_DIR, f"chat_transcript_{date_str}.txt")
        level_label = {0: "L0 System Only", 1: "L1 Simple AI + System", 2: "L2 Complex AI (Tokenized)", 3: "L3 General Treasury AI", 4: "L4 System Knowledge"}.get(level, f"L{level}")
        query_display = question if question else f"[Quick Action Card: {card_id}]"

        transcript_entry = (
            f"\n{'=' * 80}\n"
            f"[{timestamp_iso}] Customer ID: {customer_id} | User ID: {user_id}\n"
            f"Architecture: {level_label} | Intent: {intent or 'N/A'}\n"
            f"Question: {query_display}\n"
            f"{'-' * 80}\n"
            f"AI Assistant Response:\n{answer or '[Error / No Response]'}\n"
            f"{'=' * 80}\n"
        )
        with open(txt_path, "a", encoding="utf-8") as f:
            f.write(transcript_entry)

    def _upload_to_gcs_safe(self, customer_id: int, date_str: str, log_payload: Dict[str, Any]):
        """
        Background worker that syncs log payload to Google Cloud Storage.
        Fails silently and immediately if offline without impacting API responsiveness.
        """
        try:
            import socket
            try:
                socket.gethostbyname("oauth2.googleapis.com")
            except Exception:
                return

            from google.cloud import storage
            credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            bucket_name = os.getenv("GCS_BUCKET_NAME", "lg_custody_bucket")

            if credentials_path and os.path.exists(credentials_path):
                client = storage.Client.from_service_account_json(credentials_path)
            else:
                client = storage.Client()

            bucket = client.bucket(bucket_name)
            blob_name = f"ai_chat_logs/customer_{customer_id}/chat_logs_{date_str}.jsonl"
            blob = bucket.blob(blob_name)

            new_line = json.dumps(log_payload, ensure_ascii=False) + "\n"

            if blob.exists(timeout=2):
                existing_content = blob.download_as_text(encoding="utf-8", timeout=3)
                updated_content = existing_content + new_line
                blob.upload_from_string(updated_content, content_type="application/x-jsonlines", timeout=3)
            else:
                blob.upload_from_string(new_line, content_type="application/x-jsonlines", timeout=3)

            logger.info(f"Successfully synced chat log to GCS: gs://{bucket_name}/{blob_name}")
        except Exception as e:
            logger.debug(f"GCS chat log upload skipped or failed: {e}")


ai_chat_logger = AIChatLogger()
