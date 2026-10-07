from __future__ import annotations

from typing import Any


def apply(con: Any) -> None:
    """Finish main workflows that were waiting only for an optional cover."""
    con.execute("""
        UPDATE book_items
        SET workflow_status = 'done',
            workflow_current_node = 'workflow_done',
            workflow_action = NULL,
            workflow_needs_review = FALSE,
            workflow_review_reason = NULL,
            pipeline_stage = 'done',
            updated_at = CURRENT_TIMESTAMP
        WHERE COALESCE(form_status, 'not_started') <> 'consolidated'
          AND COALESCE(catalog_status, '') IN ('built', 'partial', 'manual')
          AND (
              COALESCE(pipeline_stage, '') IN ('cover', 'running:cover')
              OR COALESCE(workflow_current_node, '') IN ('cover', 'retry_cover')
              OR (
                  workflow_needs_review = TRUE
                  AND lower(COALESCE(workflow_review_reason, '')) LIKE 'cover:%'
              )
          )
        """)
