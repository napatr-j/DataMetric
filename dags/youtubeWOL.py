"""
YouTube Without Login (WOL) — daily channel metadata pipeline.

Schedule: 08:00 UTC every day.

Flow
────
  scrape_youtube  →  store_minio  →  send_email_notification
                                          ↑
                                    trigger_rule=ALL_DONE
                                    (runs on both success and failure)

Data is passed between tasks via XCom (Airflow TaskFlow API).
No intermediate files are written to disk.

Required environment variables
───────────────────────────────
  YOUTUBE_ACCOUNT     Full channel URL, e.g. https://www.youtube.com/@Google
  MINIO_ENDPOINT      MinIO host:port, e.g. minio:9000
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
  EMAIL_HOST
  EMAIL_USERNAME
  EMAIL_PASSWORD
  EMAIL_RECEIVER

Optional environment variables
───────────────────────────────
  MINIO_BUCKET              default "social-media-data"
  MINIO_SECURE              default "false"
  MINIO_AUTO_CREATE_BUCKET  default "true"
  EMAIL_PORT                default 587
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DAG_ID = "youtube_without_login"

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id=DAG_ID,
    default_args=_DEFAULT_ARGS,
    description="Daily YouTube channel metadata scraping → MinIO → email summary",
    schedule="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "social-media", "scraping"],
)
def youtube_wol_dag() -> None:

    # ── Task 1: Scrape ──────────────────────────────────────────────────────

    @task(task_id="scrape_youtube")
    def scrape_youtube() -> dict:
        """
        Launch Playwright, scrape the YouTube channel, and return metadata dict.

        Reads YOUTUBE_ACCOUNT from the environment.
        Raises ValueError if the variable is unset.
        """
        from services.youtubeScraper import YouTubeScraperError, run_scraper

        channel_url = os.environ.get("YOUTUBE_ACCOUNT", "").strip()
        if not channel_url:
            raise ValueError(
                "YOUTUBE_ACCOUNT environment variable is not set or is empty"
            )

        log.info("Task scrape_youtube started | channel=%s", channel_url)

        try:
            result = run_scraper(channel_url)
        except YouTubeScraperError as exc:
            log.exception("scrape_youtube failed: %s", exc)
            raise

        log.info(
            "Task scrape_youtube completed | name=%s | subscribers=%s",
            result.get("channel", {}).get("name"),
            result.get("channel", {}).get("subscribers"),
        )
        return result

    # ── Task 2: Store ───────────────────────────────────────────────────────

    @task(task_id="store_minio")
    def store_minio(scraped_data: dict) -> dict:
        """
        Upload the scraped JSON to MinIO and return upload metadata.

        The account folder name is derived from the channel username/name
        or falls back to the raw YOUTUBE_ACCOUNT env var value.
        """
        from services.storeMinIO import MinIOUploadError, upload_json

        platform = scraped_data.get("platform", "youtube")
        channel = scraped_data.get("channel", {})

        # Prefer handle (@Google) → display name → raw env var
        account_name = (
            channel.get("username")
            or channel.get("name")
            or os.environ.get("YOUTUBE_ACCOUNT", "unknown")
        )

        log.info(
            "Task store_minio started | platform=%s | account=%s",
            platform,
            account_name,
        )

        try:
            result = upload_json(
                platform=platform,
                account_name=account_name,
                json_data=scraped_data,
            )
        except MinIOUploadError as exc:
            log.exception("store_minio failed: %s", exc)
            raise

        log.info(
            "Task store_minio completed | path=%s | size=%d bytes",
            result.get("full_path"),
            result.get("size_bytes", 0),
        )
        return result

    # ── Task 3: Notify ──────────────────────────────────────────────────────

    @task(task_id="send_email_notification", trigger_rule=TriggerRule.ALL_DONE)
    def send_email_notification(upload_info: Optional[dict]) -> None:
        """
        Send an HTML summary email regardless of whether upstream tasks succeeded.

        Uses trigger_rule=ALL_DONE so this task always executes.
        Determines SUCCESS vs FAILED by inspecting XCom return values:
          - scraped_data None  → scrape_youtube failed
          - upload_info  None  → store_minio failed (or was skipped)
        """
        from services.sendEmail import EmailSendError, send_pipeline_email

        context = get_current_context()
        ti = context["ti"]
        dag_run = context["dag_run"]
        dag_id: str = context["dag"].dag_id

        execution_ts = datetime.now(timezone.utc).isoformat()

        # Compute elapsed wall-clock time for the whole DAG run
        elapsed_seconds = 0.0
        if dag_run and dag_run.start_date:
            start_dt = dag_run.start_date
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
            elapsed_seconds = (
                datetime.now(timezone.utc) - start_dt
            ).total_seconds()

        # Pull scrape result; upload_info is already the argument
        scraped_data: Optional[dict] = ti.xcom_pull(
            task_ids="scrape_youtube", key="return_value"
        )

        log.info(
            "Task send_email_notification | dag_id=%s | scraped=%s | uploaded=%s",
            dag_id,
            scraped_data is not None,
            upload_info is not None,
        )

        try:
            if scraped_data is not None and upload_info is not None:
                log.info("Pipeline succeeded — sending SUCCESS email")
                send_pipeline_email(
                    dag_id=dag_id,
                    status="SUCCESS",
                    execution_ts=execution_ts,
                    elapsed_seconds=elapsed_seconds,
                    upload_info=upload_info,
                    scraped_data=scraped_data,
                )
            else:
                failed_task = (
                    "scrape_youtube" if scraped_data is None else "store_minio"
                )
                log.warning(
                    "Pipeline FAILED at task '%s' — sending FAILURE email", failed_task
                )
                send_pipeline_email(
                    dag_id=dag_id,
                    status="FAILED",
                    execution_ts=execution_ts,
                    elapsed_seconds=elapsed_seconds,
                    failed_task=failed_task,
                    error_message=(
                        f"Task '{failed_task}' failed or was skipped. "
                        "Check Airflow task logs for the full exception and traceback."
                    ),
                )
        except EmailSendError as exc:
            # Log but do not fail the DAG over an email delivery problem
            log.exception("send_email_notification could not send email: %s", exc)

    # ── Wire tasks ──────────────────────────────────────────────────────────

    scraped = scrape_youtube()
    uploaded = store_minio(scraped)
    send_email_notification(uploaded)


youtube_wol_dag()
