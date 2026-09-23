import sys
import logging
from sqlalchemy import text
from app.database import engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migrate_scheduled_release")

def migrate():
    statements = [
        "ALTER TABLE quotation_rfqs ADD COLUMN IF NOT EXISTS scheduled_release_at TIMESTAMP WITH TIME ZONE;",
        "ALTER TABLE quotation_rfqs ADD COLUMN IF NOT EXISTS scheduled_release_job_id VARCHAR(100);",
        "ALTER TABLE quotation_rfqs ADD COLUMN IF NOT EXISTS is_dispatched BOOLEAN DEFAULT FALSE;",
        "ALTER TABLE quotation_rfqs ADD COLUMN IF NOT EXISTS dispatched_at TIMESTAMP WITH TIME ZONE;",
        # Mark all past approved/active/completed RFQs as already dispatched
        """
        UPDATE quotation_rfqs 
        SET is_dispatched = TRUE, dispatched_at = COALESCE(admin_reviewed_at, created_at)
        WHERE status IN ('PENDING', 'OPEN', 'EVALUATING', 'COMPLETED', 'REJECTED') 
          AND is_dispatched = FALSE;
        """
    ]
    with engine.begin() as conn:
        for stmt in statements:
            logger.info(f"Executing: {stmt.strip()[:60]}...")
            conn.execute(text(stmt))
    logger.info("Migration completed successfully.")

if __name__ == "__main__":
    migrate()
