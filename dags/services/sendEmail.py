"""
SMTP email service for DataMetric pipeline notifications.

Sends an HTML summary after each DAG run.  Supports SUCCESS and FAILED
statuses, including full traceback in failure emails.

Required environment variables
───────────────────────────────
    EMAIL_HOST      SMTP server hostname
    EMAIL_USERNAME  Sender address / login
    EMAIL_PASSWORD  SMTP password
    EMAIL_RECEIVER  Recipient address

Optional environment variables
───────────────────────────────
    EMAIL_PORT   default 587  (STARTTLS)
"""
from __future__ import annotations

import logging
import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

logger = logging.getLogger(__name__)

def _get_config() -> tuple[str, int, str, str, str]:
    return (
        os.environ.get("EMAIL_HOST", ""),
        int(os.environ.get("EMAIL_PORT", "587")),
        os.environ.get("EMAIL_USERNAME", ""),
        os.environ.get("EMAIL_PASSWORD", ""),
        os.environ.get("EMAIL_RECEIVER", ""),
    )


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------

class EmailSendError(Exception):
    """Raised when the pipeline summary email cannot be sent."""


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def _validate_config(host: str, username: str, password: str, receiver: str) -> None:
    """
    Verify all required email environment variables are present.

    Raises:
        EmailSendError: If any variable is missing.
    """
    missing = [
        name
        for name, val in [
            ("EMAIL_HOST", host),
            ("EMAIL_USERNAME", username),
            ("EMAIL_PASSWORD", password),
            ("EMAIL_RECEIVER", receiver),
        ]
        if not val
    ]
    if missing:
        raise EmailSendError(
            f"Missing required email environment variable(s): {', '.join(missing)}"
        )


# ---------------------------------------------------------------------------
# HTML email builders
# ---------------------------------------------------------------------------

_TABLE_STYLE = (
    'border="1" cellpadding="7" cellspacing="0" '
    'style="border-collapse:collapse;font-family:monospace;font-size:13px;"'
)
_TH_STYLE = 'style="background:#f0f0f0;text-align:left;padding:6px 10px;"'
_TD_STYLE = 'style="padding:6px 10px;"'


def _row(label: str, value: object) -> str:
    return (
        f"<tr>"
        f"<td {_TH_STYLE}>{label}</td>"
        f"<td {_TD_STYLE}>{value}</td>"
        f"</tr>"
    )


def _build_success_html(
    dag_id: str,
    execution_ts: str,
    elapsed_seconds: float,
    upload_info: dict,
    scraped_data: dict,
) -> str:
    channel = scraped_data.get("channel", {})
    rows = "".join(
        [
            _row("DAG", dag_id),
            _row("Status", '<span style="color:#27ae60;font-weight:bold;">SUCCESS</span>'),
            _row("Execution time (UTC)", execution_ts),
            _row("Elapsed", f"{elapsed_seconds:.1f} s"),
            _row("Platform", scraped_data.get("platform", "—")),
            _row("Channel name", channel.get("name") or "—"),
            _row("Username", channel.get("username") or "—"),
            _row("Subscribers", channel.get("subscribers") or "—"),
            _row("Videos", channel.get("videos") or "—"),
            _row("Views", channel.get("views") or "—"),
            _row("Joined", channel.get("joined_date") or "—"),
            _row("MinIO path", upload_info.get("full_path") or "—"),
            _row("Upload size", f'{upload_info.get("size_bytes", 0):,} bytes'),
            _row("Uploaded at (UTC)", upload_info.get("uploaded_at") or "—"),
        ]
    )
    return f"""
    <html>
    <body style="font-family:Arial,sans-serif;font-size:14px;color:#333;">
      <h2 style="color:#27ae60;">&#9989; Pipeline SUCCESS &mdash; {dag_id}</h2>
      <table {_TABLE_STYLE}>{rows}</table>
    </body>
    </html>
    """


def _build_failure_html(
    dag_id: str,
    execution_ts: str,
    elapsed_seconds: float,
    failed_task: str,
    error_message: str,
    traceback_str: Optional[str],
) -> str:
    tb_section = (
        f'<pre style="background:#fff3f3;border:1px solid #e74c3c;padding:12px;'
        f'overflow:auto;font-size:12px;">{traceback_str}</pre>'
        if traceback_str
        else "<p><em>No traceback available — check Airflow task logs.</em></p>"
    )
    rows = "".join(
        [
            _row("DAG", dag_id),
            _row("Status", '<span style="color:#e74c3c;font-weight:bold;">FAILED</span>'),
            _row("Execution time (UTC)", execution_ts),
            _row("Elapsed", f"{elapsed_seconds:.1f} s"),
            _row("Failed task", f"<strong>{failed_task}</strong>"),
            _row("Error", error_message),
        ]
    )
    return f"""
    <html>
    <body style="font-family:Arial,sans-serif;font-size:14px;color:#333;">
      <h2 style="color:#e74c3c;">&#10060; Pipeline FAILED &mdash; {dag_id}</h2>
      <table {_TABLE_STYLE}>{rows}</table>
      <h3 style="margin-top:20px;">Traceback</h3>
      {tb_section}
    </body>
    </html>
    """


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def send_pipeline_email(
    dag_id: str,
    status: str,
    execution_ts: str,
    elapsed_seconds: float,
    upload_info: Optional[dict] = None,
    scraped_data: Optional[dict] = None,
    failed_task: Optional[str] = None,
    error_message: Optional[str] = None,
    traceback_str: Optional[str] = None,
) -> None:
    """
    Send an HTML pipeline summary email via SMTP (STARTTLS on port 587).

    Args:
        dag_id:          Airflow DAG identifier.
        status:          ``"SUCCESS"`` or ``"FAILED"``.
        execution_ts:    ISO-8601 UTC timestamp of execution.
        elapsed_seconds: Total wall-clock duration of the pipeline run.
        upload_info:     Return value of ``storeMinIO.upload_json`` (SUCCESS only).
        scraped_data:    Return value of ``youtubeScraper.run_scraper`` (SUCCESS only).
        failed_task:     Task ID that failed (FAILED only).
        error_message:   Short human-readable error description (FAILED only).
        traceback_str:   Full traceback string (FAILED only, may be None).

    Raises:
        EmailSendError: If the email cannot be sent for any reason.
    """
    host, port, username, password, receiver = _get_config()
    _validate_config(host, username, password, receiver)

    date_part = execution_ts[:10]
    subject = f"[DataMetric] {status} — {dag_id} — {date_part}"

    if status == "SUCCESS" and upload_info and scraped_data:
        html_body = _build_success_html(
            dag_id, execution_ts, elapsed_seconds, upload_info, scraped_data
        )
    else:
        html_body = _build_failure_html(
            dag_id,
            execution_ts,
            elapsed_seconds,
            failed_task or "unknown",
            error_message or "No error message captured.",
            traceback_str,
        )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = username
    msg["To"] = receiver
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    logger.info(
        "Sending %s email | to=%s | subject=%s", status, receiver, subject
    )

    try:
        with smtplib.SMTP(host, port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(username, password)
            smtp.sendmail(username, [receiver], msg.as_string())

        logger.info("Email delivered successfully to %s", receiver)

    except smtplib.SMTPAuthenticationError as exc:
        logger.exception("SMTP authentication failed for user %s", username)
        raise EmailSendError(f"SMTP authentication failed: {exc}") from exc
    except smtplib.SMTPConnectError as exc:
        logger.exception("Cannot connect to SMTP server %s:%s", host, port)
        raise EmailSendError(f"SMTP connection failed ({host}:{port}): {exc}") from exc
    except smtplib.SMTPException as exc:
        logger.exception("SMTP error while sending email")
        raise EmailSendError(f"SMTP error: {exc}") from exc
    except Exception as exc:
        logger.exception("Unexpected error sending email")
        raise EmailSendError(f"Unexpected error sending email: {exc}") from exc
