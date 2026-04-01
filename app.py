#!/usr/bin/env python3
"""
ServiceM8 Webhook Server
========================
1.  Handles ServiceM8 ``inbox.message_received`` webhook events.
    When a new inbox message arrives the webhook schedules a one-shot
    check for 10 minutes later.  At that point the message is re-fetched;
    if it is still unarchived and not yet converted (i.e. no human has
    manually actioned it) it is converted to a Quote job, stamped with
    the standard template, and a Slack notification is posted to
    #new-enquiry-received.  If a human has already actioned it the
    auto-conversion is skipped.

2.  Runs a scheduled task **twice daily** (9 AM and 5 PM AEST) that finds
    Quote jobs whose sent quotes have expired, marks them Unsuccessful, and
    sends a Slack DM to Luke with a summary.

3.  Runs a scheduled task **daily at 5 PM AEST** that queries all jobs
    completed today, groups revenue by technician, and sends a Slack DM
    to Shannon Rowe with a summary.

4.  Runs a **fallback inbox scanner every 15 minutes** that catches any
    online-booking inbox messages older than 10 minutes that have not been
    archived or converted.  This is a safety net in case the webhook does
    not fire.

5.  Provides **Xero OAuth 2.0** integration endpoints:
    - GET /xero/login    — redirects to Xero authorisation
    - GET /xero/callback — handles the OAuth callback and stores tokens
    - GET /xero/status   — checks whether Xero is connected
    Tokens are refreshed automatically before expiry.

Environment variables (set in Render dashboard):
    SERVICEM8_API_KEY       – ServiceM8 API key  (required)
    SLACK_CHANNEL_ID        – Slack channel ID for new-enquiry notifications
    SLACK_DM_USER_ID        – Slack user ID to DM for expired-quote alerts (Luke)
    SLACK_DM_SHANNON_ID     – Slack user ID to DM for daily income report (Shannon)
    XERO_CLIENT_ID          – Xero OAuth 2.0 Client ID
    XERO_CLIENT_SECRET      – Xero OAuth 2.0 Client Secret
    XERO_REDIRECT_URI       – Xero OAuth 2.0 Redirect URI
    PORT                    – TCP port (Render sets this automatically)
"""

import base64
import json
import logging
import os
import re
import secrets
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, Response, redirect, request

# ── Configuration ──────────────────────────────────────────────────────────────
SERVICEM8_API_KEY = os.environ.get(
    "SERVICEM8_API_KEY", "smk-0fa743-cca53e4f1ad199f6-f991a3ae6a206763"
)
SERVICEM8_BASE_URL = "https://api.servicem8.com/api_1.0"
SERVICEM8_HEADERS = {
    "X-API-Key": SERVICEM8_API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "C0AQCK3SZUG")
SLACK_DM_USER_ID = os.environ.get("SLACK_DM_USER_ID", "U07B253U868")  # Luke
SLACK_DM_SHANNON_ID = os.environ.get("SLACK_DM_SHANNON_ID", "U07K1RUAE31")  # Shannon

# Slack @ mentions — included in messages so recipients receive proper pings.
LUKE_MENTION = f"<@{os.environ.get('SLACK_DM_USER_ID', 'U07B253U868')}>"
SHANNON_MENTION = f"<@{os.environ.get('SLACK_DM_SHANNON_ID', 'U07K1RUAE31')}>"

PORT = int(os.environ.get("PORT", 5000))

# ── Xero OAuth 2.0 configuration ─────────────────────────────────────────────
XERO_CLIENT_ID = os.environ.get(
    "XERO_CLIENT_ID", "E4BC3D0541D9431F8EC6E9396ABFF7AF"
)
XERO_CLIENT_SECRET = os.environ.get(
    "XERO_CLIENT_SECRET", "YPIJ7JiGGuhVh-ss-t3LSFl9XvdUOCFPQi3EUp63ECrR2Pfj"
)
XERO_REDIRECT_URI = os.environ.get(
    "XERO_REDIRECT_URI",
    "https://servicem8-webhook-server.onrender.com/xero/callback",
)
XERO_AUTH_URL = "https://login.xero.com/identity/connect/authorize"
XERO_TOKEN_URL = "https://identity.xero.com/connect/token"
XERO_CONNECTIONS_URL = "https://api.xero.com/connections"
XERO_SCOPES = (
    "openid profile email offline_access "
    "accounting.invoices.read accounting.payments.read "
    "accounting.banktransactions.read accounting.manualjournals.read "
    "accounting.contacts.read accounting.settings.read"
)

# Use the proper IANA timezone so AEDT (+11) / AEST (+10) transitions are
# handled automatically — a hardcoded +10:00 offset fires 1 hour late during
# daylight saving time (October–April).
AEST = ZoneInfo("Australia/Sydney")

# ── Technician UUID → display-name mapping ────────────────────────────────────
TECHNICIAN_MAP: dict[str, str] = {
    "d2c4e758-1f88-42cf-aaf6-20e5997a2c3b": "Adam Spinelli",
    "d228d76e-8ffa-449a-9ca4-23da2ed21f6b": "Chris Stephenson",
    "3f4584ed-74cf-4ee4-9890-2396dd877f5b": "Charlie Wright",
    "5a175074-5801-4b8c-96fc-23e6f0d2c00b": "Martin M",
    "b909dbb0-3bdd-4129-8e57-21d0b66873ab": "Wayne Googh",
    "b37f0d70-8dd1-4f2b-9610-23f41e9f12eb": "Darren",
}

# ── Boilerplate content ────────────────────────────────────────────────────────
WORK_DONE_DESCRIPTION = (
    "Peace of Mind Warranty:\n"
    "All our work comes with a 12-month workmanship warranty \u2014 conditions apply. "
    "You can rest easy knowing your investment is protected by experienced professionals "
    "who stand behind their work. Please note that items may be discounted if you request "
    "a quote for multiple items. If you remove any items, we will send a new quote, and "
    "prices may change. All quotes are pending site inspection.\n\n"
    "Pre-Work Site Inspection Notice\n\n"
    "Important Information About Your Booking\n\n"
    "All jobs are scheduled with a pre-set time allocation based on a \u201csite unseen\u201d "
    "basis. To ensure your job can be completed on the day, our technicians will conduct "
    "a pre-work site inspection upon arrival.\n\n"
    "Job Categories Following Inspection:\n"
    "Category 1 \u2013 Clear to Proceed: No visible issues; work is likely to be completed "
    "within the booked timeframe.\n"
    "Category 2 \u2013 Proceed with Caution: Some visible issues may affect completion; "
    "technician will attempt to complete the work, but if not possible, a site inspection "
    "charge of $120.00 + GST will apply and a re-quote or revisit may be required.\n"
    "Category 3 \u2013 Not Ready for Work: Clear issues prevent completion; a site inspection "
    "charge of $120.00 + GST applies and a re-quote will be provided.\n"
    "Category 4 \u2013 Outside of Scope: The job is too damaged or outside our scope; a site "
    "inspection fee of $120.00 + GST applies.\n\n"
    "Please note:\n"
    "Discounts may apply when quoting for multiple items\n"
    "If any items are removed from your quote, a revised quote will be issued and pricing "
    "may change\n"
    "All quotes are subject to on-site inspection and may be updated based on access or "
    "job complexity\n\n"
    "**PLEASE NOTE: Technicians are unable to accept cash. Payment can be made by card "
    "or bank transfer only**"
)

JOB_DESCRIPTION_TEMPLATE = """\
JOB TYPE: 

SERVICE: 

ORIGINAL CUSTOMER ENQUIRY: {enquiry_text}

IMPORTANT INFORMATION (FROM OFFICE):

CATAGORY AFTER SITE INSPECTION:

TECHNICIAN NOTES:

NOTES FOR QUOTING ( Return visit necessary )
How many technicians needed to complete job:
How many estimated hours to complete job:
What parts are required to complete job:
Are parts stocked in technician van:

OFFICE NOTES
Parts & material cost:
Are parts from Bunnings OR Special order:
"""

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ── Flask app ──────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", secrets.token_hex(32))

# Simple in-memory dedup set (resets on restart, which is acceptable)
_processed_uuids: set = set()
_processed_lock = threading.Lock()

# ── Xero token storage (in-memory; persists for the lifetime of the process) ──
_xero_token_lock = threading.Lock()
_xero_tokens: dict = {
    "access_token": None,
    "refresh_token": None,
    "expires_at": 0,         # Unix timestamp when access_token expires
    "tenant_id": None,
    "tenant_name": None,
}
_xero_oauth_state: str = ""   # CSRF state parameter


# ═══════════════════════════════════════════════════════════════════════════════
#  ServiceM8 helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _sm8_get(path: str) -> requests.Response:
    url = f"{SERVICEM8_BASE_URL}/{path}"
    logger.info("GET %s", url)
    resp = requests.get(url, headers=SERVICEM8_HEADERS, timeout=30)
    logger.info("GET %s -> %s", url, resp.status_code)
    return resp


def _sm8_post(path: str, json_data: dict | None = None) -> requests.Response:
    url = f"{SERVICEM8_BASE_URL}/{path}"
    logger.info("POST %s", url)
    resp = requests.post(url, headers=SERVICEM8_HEADERS, json=json_data, timeout=30)
    logger.info("POST %s -> %s", url, resp.status_code)
    return resp


# ═══════════════════════════════════════════════════════════════════════════════
#  Slack helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _slack_send(channel_id: str, message: str) -> None:
    """Send a Slack message (channel or DM) via the Manus MCP CLI."""
    payload = json.dumps({"channel_id": channel_id, "message": message})
    try:
        result = subprocess.run(
            [
                "manus-mcp-cli", "tool", "call", "slack_send_message",
                "--server", "slack",
                "--input", payload,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        logger.info("Slack send result: %s %s", result.stdout[:300], result.stderr[:200])
    except Exception as exc:
        logger.error("Slack send failed: %s", exc)


def _send_new_enquiry_slack(job_number: str, client_name: str, address: str,
                            enquiry_text: str, email: str, phone: str) -> None:
    """Post a new-enquiry notification to the #new-enquiry-received channel."""
    message = (
        f"{LUKE_MENTION} *New Online Enquiry Received* :incoming_envelope:\n\n"
        f"*Job Number:* {job_number}\n"
        f"*Client Name:* {client_name}\n"
        f"*Address:* {address}\n"
        f"*Enquiry:* {enquiry_text}\n"
        f"*Email:* {email}\n"
        f"*Phone:* {phone}"
    )
    _slack_send(SLACK_CHANNEL_ID, message)


# Delay (in minutes) before auto-converting an inbox message to a job.
# This gives staff time to manually action the message first.
INBOX_DELAY_MINUTES = int(os.environ.get("INBOX_DELAY_MINUTES", 10))

# Maximum age (in minutes) the fallback scanner will look back.
# CRITICAL: prevents the scanner from ever processing old/historical messages.
# Only messages received within this window are eligible for auto-conversion.
INBOX_MAX_AGE_MINUTES = int(os.environ.get("INBOX_MAX_AGE_MINUTES", 60))


# ═══════════════════════════════════════════════════════════════════════════════
#  Webhook processing — new enquiry pipeline
# ═══════════════════════════════════════════════════════════════════════════════

def _process_inbox_message(uuid: str) -> None:
    """
    Full processing pipeline for a single inbox message UUID.
    Called by the one-shot APScheduler job INBOX_DELAY_MINUTES after the
    webhook fires.  At this point we re-check whether the message has been
    manually actioned (archived or already converted) and skip if so.
    """
    try:
        logger.info("=== Processing inbox message %s (delayed check) ===", uuid)

        # 1. Fetch inbox message -----------------------------------------------
        resp = _sm8_get(f"inboxmessage/{uuid}.json")
        if resp.status_code != 200:
            logger.error("Could not fetch inbox message %s: %s", uuid, resp.status_code)
            return
        inbox = resp.json()
        logger.info(
            "Inbox message: is_archived=%s, archived_by=%s, converted_to_job_uuid=%s",
            inbox.get("is_archived"),
            inbox.get("archived_by_staff_uuid"),
            inbox.get("converted_to_job_uuid"),
        )

        # 2. Guard: skip if a human has already actioned the message -----------
        #    is_archived == "1"  → staff archived it (manually actioned)
        #    converted_to_job_uuid set → already converted (manually or by us)
        if str(inbox.get("is_archived", "0")) == "1":
            logger.info(
                "Message %s was archived by staff (%s) — skipping auto-conversion.",
                uuid, inbox.get("archived_by_staff_uuid", "unknown"),
            )
            return

        existing_job = inbox.get("converted_to_job_uuid", "") or ""
        if existing_job and existing_job not in ("", "0"):
            logger.info("Message %s already converted to job %s — skipping.", uuid, existing_job)
            return

        logger.info(
            "Message %s is still unactioned after %d minutes — proceeding with auto-conversion.",
            uuid, INBOX_DELAY_MINUTES,
        )

        # 3. Extract enquiry text ----------------------------------------------
        enquiry_text = (
            inbox.get("message_text")
            or inbox.get("message_body")
            or inbox.get("description")
            or inbox.get("subject")
            or inbox.get("body")
            or "No description provided"
        )
        logger.info("Enquiry text (first 200 chars): %s", enquiry_text[:200])

        # 4. Convert inbox message to job --------------------------------------
        logger.info("Converting inbox message %s to job…", uuid)
        convert_resp = _sm8_post(f"inboxmessage/{uuid}/convert-to-job.json")
        logger.info(
            "Convert response: %s — %s",
            convert_resp.status_code,
            convert_resp.text[:300],
        )
        # The endpoint may return 500 but still succeed; wait briefly then
        # re-fetch the inbox message to get the generated job UUID.
        time.sleep(3)

        resp2 = _sm8_get(f"inboxmessage/{uuid}.json")
        if resp2.status_code != 200:
            logger.error("Re-fetch of inbox message %s failed: %s", uuid, resp2.status_code)
            return
        inbox_updated = resp2.json()
        job_uuid = inbox_updated.get("converted_to_job_uuid", "") or ""

        # Fallback: try to parse job UUID from the convert response body
        if not job_uuid or job_uuid in ("", "0"):
            try:
                job_uuid = convert_resp.json().get("uuid", "") or ""
            except Exception:
                pass

        if not job_uuid or job_uuid in ("", "0"):
            logger.error("Could not determine job UUID for inbox message %s", uuid)
            return

        logger.info("Inbox message %s → job %s", uuid, job_uuid)

        # 5. Fetch the new job -------------------------------------------------
        resp3 = _sm8_get(f"job/{job_uuid}.json")
        if resp3.status_code != 200:
            logger.error("Could not fetch job %s: %s", job_uuid, resp3.status_code)
            return
        job = resp3.json()
        logger.info(
            "Job fetched: generated_job_id=%s, status=%s",
            job.get("generated_job_id"),
            job.get("status"),
        )

        # 6. Update job description + work_done_description --------------------
        new_description = JOB_DESCRIPTION_TEMPLATE.format(enquiry_text=enquiry_text)
        update_payload = {
            "job_description": new_description,
            "work_done_description": WORK_DONE_DESCRIPTION,
            "status": "Quote",
        }
        update_resp = requests.post(
            f"{SERVICEM8_BASE_URL}/job/{job_uuid}.json",
            headers=SERVICEM8_HEADERS,
            json=update_payload,
            timeout=30,
        )
        logger.info("Job update: %s — %s", update_resp.status_code, update_resp.text[:300])

        # 7. Collect contact details for Slack ---------------------------------
        job_number = job.get("generated_job_id", "N/A")
        address = job.get("job_address", "N/A")
        client_name = email = phone = "N/A"

        company_uuid = job.get("company_uuid", "")
        if company_uuid:
            comp_resp = _sm8_get(f"company/{company_uuid}.json")
            if comp_resp.status_code == 200:
                comp = comp_resp.json()
                client_name = comp.get("name") or "N/A"
                email = comp.get("email") or "N/A"
                phone = comp.get("phone") or comp.get("mobile") or "N/A"

        # Fall back to inbox message fields if company record is sparse
        if email == "N/A":
            email = (
                inbox.get("from_email")
                or inbox.get("sender_email")
                or inbox.get("email")
                or "N/A"
            )
        if phone == "N/A":
            phone = (
                inbox.get("sender_phone")
                or inbox.get("phone")
                or "N/A"
            )
        if client_name == "N/A":
            client_name = (
                inbox.get("from_name")
                or inbox.get("sender_name")
                or inbox.get("name")
                or "N/A"
            )

        # Try to extract phone from message_text if still missing
        if phone == "N/A" and inbox.get("message_text"):
            phone_match = re.search(r'Phone\s+(\d[\d\s]+)', inbox.get("message_text", ""))
            if phone_match:
                phone = phone_match.group(1).strip()

        # 8. Send Slack notification -------------------------------------------
        _send_new_enquiry_slack(
            job_number=job_number,
            client_name=client_name,
            address=address,
            enquiry_text=enquiry_text[:500],
            email=email,
            phone=phone,
        )

        logger.info(
            "=== Finished processing inbox message %s → Job %s (%s) ===",
            uuid, job_number, job_uuid,
        )

    except Exception:
        logger.exception("Unhandled error processing inbox message %s", uuid)


# ═══════════════════════════════════════════════════════════════════════════════
#  Scheduled task — fallback inbox scanner (every 15 minutes)
# ═══════════════════════════════════════════════════════════════════════════════

def fallback_inbox_scanner() -> None:
    """
    Safety-net scheduled job (every 15 minutes).
    Scans the ServiceM8 inbox for online_booking messages that are:
      - not archived  (is_archived == "0")
      - not converted (converted_to_job_uuid is empty)
      - older than INBOX_DELAY_MINUTES
    and auto-converts them via the standard pipeline.
    """
    logger.info("=== Fallback inbox scanner started ===")
    now = datetime.now(AEST)

    try:
        resp = _sm8_get("inboxmessage.json")
        if resp.status_code != 200:
            logger.error("Failed to fetch inbox messages: %s", resp.status_code)
            return

        raw_data = resp.json()
        # API returns {"messages": [...]} not a flat list
        if isinstance(raw_data, dict):
            messages = raw_data.get("messages", [])
        elif isinstance(raw_data, list):
            messages = raw_data
        else:
            logger.error("Unexpected inbox response type: %s", type(raw_data))
            return

        logger.info("Inbox messages fetched: %d", len(messages))

        candidates: list[dict] = []
        for msg in messages:
            # Only online bookings
            if msg.get("message_type") != "online_booking":
                continue
            # Not archived
            if str(msg.get("is_archived", "0")) == "1":
                continue
            # Not already converted
            converted = msg.get("converted_to_job_uuid", "") or ""
            if converted and converted not in ("", "0"):
                continue
            # Must be older than INBOX_DELAY_MINUTES
            ts_str = msg.get("timestamp", "") or ""
            if not ts_str or ts_str.startswith("0000"):
                continue
            try:
                msg_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                msg_dt = msg_dt.replace(tzinfo=AEST)
            except ValueError:
                continue
            age_minutes = (now - msg_dt).total_seconds() / 60.0
            if age_minutes < INBOX_DELAY_MINUTES:
                logger.info(
                    "Inbox %s is only %.1f min old (< %d) — skipping for now.",
                    msg["uuid"][:8], age_minutes, INBOX_DELAY_MINUTES,
                )
                continue

            # CRITICAL safety cap: never process messages older than INBOX_MAX_AGE_MINUTES.
            # This prevents the scanner from ever touching historical/old messages.
            if age_minutes > INBOX_MAX_AGE_MINUTES:
                logger.info(
                    "Inbox %s is %.1f min old (> %d max) — too old, skipping.",
                    msg["uuid"][:8], age_minutes, INBOX_MAX_AGE_MINUTES,
                )
                continue

            candidates.append(msg)

        logger.info(
            "Fallback scanner found %d unactioned inbox message(s) between %d and %d min old.",
            len(candidates), INBOX_DELAY_MINUTES, INBOX_MAX_AGE_MINUTES,
        )

        for msg in candidates:
            uuid = msg["uuid"]
            # Add to dedup set so the webhook-triggered job won't double-process
            with _processed_lock:
                if uuid in _processed_uuids:
                    logger.info("Fallback: %s already processed — skipping.", uuid[:8])
                    continue
                _processed_uuids.add(uuid)

            logger.info("Fallback: processing inbox message %s", uuid)
            # Run synchronously inside the scheduler thread (one at a time)
            _process_inbox_message(uuid)

        logger.info("=== Fallback inbox scanner complete ===")

    except Exception:
        logger.exception("Unhandled error in fallback inbox scanner")


# ═══════════════════════════════════════════════════════════════════════════════
#  Scheduled task — auto-expire quotes
# ═══════════════════════════════════════════════════════════════════════════════

def _get_client_name(company_uuid: str) -> str:
    """Fetch the client/company name from ServiceM8."""
    if not company_uuid:
        return "N/A"
    try:
        resp = _sm8_get(f"company/{company_uuid}.json")
        if resp.status_code == 200:
            return resp.json().get("name") or "N/A"
    except Exception:
        pass
    return "N/A"


def check_expired_quotes() -> None:
    """
    Scheduled job: find all Quote jobs with a sent quote whose
    queue_expiry_date has passed, mark them Unsuccessful, and DM Luke.
    """
    logger.info("=== Expired-quote check started ===")
    now = datetime.now(AEST)

    try:
        # 1. Fetch all Quote jobs ---------------------------------------------
        resp = _sm8_get("job.json?%24filter=status%20eq%20%27Quote%27")
        if resp.status_code != 200:
            logger.error("Failed to fetch Quote jobs: %s", resp.status_code)
            return
        jobs = resp.json()
        logger.info("Total Quote jobs fetched: %d", len(jobs))

        expired_jobs: list[dict] = []

        for job in jobs:
            # Only consider quotes that have actually been sent to the customer
            if not job.get("quote_sent"):
                continue

            expiry_str = job.get("queue_expiry_date", "0000-00-00 00:00:00") or ""
            if not expiry_str or expiry_str == "0000-00-00 00:00:00":
                continue

            try:
                expiry_dt = datetime.strptime(expiry_str, "%Y-%m-%d %H:%M:%S")
                expiry_dt = expiry_dt.replace(tzinfo=AEST)
            except ValueError:
                continue

            if expiry_dt < now:
                expired_jobs.append(job)

        logger.info("Expired Quote jobs found: %d", len(expired_jobs))

        if not expired_jobs:
            logger.info("No expired quotes — nothing to do.")
            return

        # 2. Mark each expired job as Unsuccessful and collect details ---------
        dm_lines: list[str] = []

        for job in expired_jobs:
            job_uuid = job["uuid"]
            job_number = job.get("generated_job_id", "N/A")
            address = job.get("job_address", "N/A")
            client_name = _get_client_name(job.get("company_uuid", ""))

            logger.info(
                "Marking job %s (%s) as Unsuccessful — quote expired %s",
                job_number, job_uuid, job.get("queue_expiry_date"),
            )

            update_resp = _sm8_post(f"job/{job_uuid}.json", json_data={"status": "Unsuccessful"})
            logger.info("Update result for job %s: %s", job_number, update_resp.status_code)

            dm_lines.append(
                f"\u2022 *Job {job_number}* \u2014 {client_name} \u2014 {address}\n"
                f"  Quote expired: {job.get('queue_expiry_date', 'N/A')}"
            )

        # 3. Send a single DM to Luke with the full summary -------------------
        header = (
            f"{LUKE_MENTION} :warning: *Expired Quotes — Auto-marked Unsuccessful*\n"
            f"_{now.strftime('%A %-d %B %Y, %-I:%M %p')} AEST_\n\n"
            f"The following {len(dm_lines)} job(s) had expired quotes and have "
            f"been automatically set to *Unsuccessful*:\n\n"
        )
        dm_body = "\n\n".join(dm_lines)
        _slack_send(SLACK_DM_USER_ID, header + dm_body)

        logger.info("=== Expired-quote check complete — %d jobs updated ===", len(expired_jobs))

    except Exception:
        logger.exception("Unhandled error in expired-quote check")


# ═══════════════════════════════════════════════════════════════════════════════
#  Scheduled task — daily technician income report
# ═══════════════════════════════════════════════════════════════════════════════

def daily_technician_income_report() -> None:
    """
    Scheduled job (5 PM AEST daily): query all jobs completed today,
    group invoice totals by technician, and DM Shannon Rowe with a summary.
    """
    logger.info("=== Daily technician income report started ===")
    now = datetime.now(AEST)
    today_str = now.strftime("%Y-%m-%d")

    try:
        # 1. Fetch all Completed jobs -----------------------------------------
        resp = _sm8_get("job.json?%24filter=status%20eq%20%27Completed%27")
        if resp.status_code != 200:
            logger.error("Failed to fetch Completed jobs: %s", resp.status_code)
            return
        all_jobs = resp.json()
        logger.info("Total Completed jobs fetched: %d", len(all_jobs))

        # 2. Filter to jobs completed today (AEST) ----------------------------
        todays_jobs: list[dict] = []
        for job in all_jobs:
            comp_str = job.get("completion_date", "") or ""
            if not comp_str or comp_str.startswith("0000"):
                continue
            # completion_date is like "2026-03-28 12:47:45"
            if comp_str[:10] == today_str:
                todays_jobs.append(job)

        logger.info("Jobs completed today (%s): %d", today_str, len(todays_jobs))

        # 3. Group revenue by technician (completion_actioned_by_uuid) --------
        tech_totals: dict[str, float] = {}       # uuid -> total
        tech_job_counts: dict[str, int] = {}      # uuid -> count
        tech_job_details: dict[str, list] = {}    # uuid -> list of (job_id, amount)

        for job in todays_jobs:
            tech_uuid = job.get("completion_actioned_by_uuid", "") or ""
            if not tech_uuid:
                tech_uuid = "unassigned"

            amount = 0.0
            try:
                amount = float(job.get("total_invoice_amount", 0) or 0)
            except (ValueError, TypeError):
                pass

            tech_totals[tech_uuid] = tech_totals.get(tech_uuid, 0.0) + amount
            tech_job_counts[tech_uuid] = tech_job_counts.get(tech_uuid, 0) + 1

            job_id = job.get("generated_job_id", "?")
            if tech_uuid not in tech_job_details:
                tech_job_details[tech_uuid] = []
            tech_job_details[tech_uuid].append((job_id, amount))

        # 4. Build the Slack DM message ----------------------------------------
        grand_total = sum(tech_totals.values())
        total_jobs = len(todays_jobs)

        lines: list[str] = []
        lines.append(f"{SHANNON_MENTION} :moneybag: *Daily Technician Income Report*")
        lines.append(f"_{now.strftime('%A %-d %B %Y')}_ — {total_jobs} job(s) completed\n")

        if not todays_jobs:
            lines.append("No jobs were completed today.")
        else:
            lines.append("```")
            lines.append(f"{'Technician':<22} {'Jobs':>5}  {'Revenue':>12}")
            lines.append(f"{'-' * 22} {'-' * 5}  {'-' * 12}")

            # Sort by revenue descending
            sorted_techs = sorted(tech_totals.items(), key=lambda x: x[1], reverse=True)

            for tech_uuid, total in sorted_techs:
                name = TECHNICIAN_MAP.get(tech_uuid, None)
                if name is None:
                    # Try to look up the staff name from ServiceM8
                    if tech_uuid and tech_uuid != "unassigned":
                        try:
                            staff_resp = _sm8_get(f"staff/{tech_uuid}.json")
                            if staff_resp.status_code == 200:
                                s = staff_resp.json()
                                name = f"{s.get('first', '')} {s.get('last', '')}".strip()
                                if not name or name == ".":
                                    name = tech_uuid[:12]
                            else:
                                name = tech_uuid[:12]
                        except Exception:
                            name = tech_uuid[:12]
                    else:
                        name = "Unassigned"

                count = tech_job_counts.get(tech_uuid, 0)
                lines.append(f"{name:<22} {count:>5}  ${total:>11,.2f}")

            lines.append(f"{'-' * 22} {'-' * 5}  {'-' * 12}")
            lines.append(f"{'TOTAL':<22} {total_jobs:>5}  ${grand_total:>11,.2f}")
            lines.append("```")

            # Add per-technician job breakdown
            lines.append("")
            lines.append("*Job Breakdown:*")
            for tech_uuid, total in sorted_techs:
                name = TECHNICIAN_MAP.get(tech_uuid, None)
                if name is None:
                    if tech_uuid and tech_uuid != "unassigned":
                        try:
                            staff_resp = _sm8_get(f"staff/{tech_uuid}.json")
                            if staff_resp.status_code == 200:
                                s = staff_resp.json()
                                name = f"{s.get('first', '')} {s.get('last', '')}".strip()
                                if not name or name == ".":
                                    name = tech_uuid[:12]
                            else:
                                name = tech_uuid[:12]
                        except Exception:
                            name = tech_uuid[:12]
                    else:
                        name = "Unassigned"

                details = tech_job_details.get(tech_uuid, [])
                detail_parts = [f"#{jid} (${amt:,.2f})" for jid, amt in details]
                lines.append(f"\u2022 *{name}:* {', '.join(detail_parts)}")

        message = "\n".join(lines)
        _slack_send(SLACK_DM_SHANNON_ID, message)

        logger.info(
            "=== Daily income report sent — %d jobs, $%.2f total ===",
            total_jobs, grand_total,
        )

    except Exception:
        logger.exception("Unhandled error in daily technician income report")


# ═════════════════════════════════════════════════════════════════════════════
#  Xero OAuth 2.0 helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _xero_basic_auth_header() -> str:
    """Return the Base64-encoded Basic auth header for Xero token requests."""
    raw = f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}"
    return "Basic " + base64.b64encode(raw.encode()).decode()


def _xero_exchange_code(code: str) -> dict:
    """Exchange an authorisation code for access + refresh tokens."""
    resp = requests.post(
        XERO_TOKEN_URL,
        headers={
            "Authorization": _xero_basic_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": XERO_REDIRECT_URI,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _xero_refresh_access_token() -> bool:
    """
    Refresh the Xero access token using the stored refresh token.
    Returns True on success, False on failure.
    """
    with _xero_token_lock:
        refresh_token = _xero_tokens.get("refresh_token")
    if not refresh_token:
        logger.warning("No Xero refresh token available — cannot refresh.")
        return False

    try:
        resp = requests.post(
            XERO_TOKEN_URL,
            headers={
                "Authorization": _xero_basic_auth_header(),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            timeout=30,
        )
        resp.raise_for_status()
        token_data = resp.json()

        with _xero_token_lock:
            _xero_tokens["access_token"] = token_data["access_token"]
            _xero_tokens["refresh_token"] = token_data.get(
                "refresh_token", refresh_token
            )
            _xero_tokens["expires_at"] = time.time() + token_data.get(
                "expires_in", 1800
            )

        logger.info("Xero access token refreshed successfully.")
        return True
    except Exception:
        logger.exception("Failed to refresh Xero access token.")
        return False


def xero_get_valid_token() -> str | None:
    """
    Return a valid Xero access token, refreshing if necessary.
    Returns None if Xero is not connected.
    """
    with _xero_token_lock:
        access_token = _xero_tokens.get("access_token")
        expires_at = _xero_tokens.get("expires_at", 0)

    if not access_token:
        return None

    # Refresh if token expires within 5 minutes
    if time.time() > (expires_at - 300):
        if not _xero_refresh_access_token():
            return None
        with _xero_token_lock:
            access_token = _xero_tokens.get("access_token")

    return access_token


def _xero_fetch_connections(access_token: str) -> list[dict]:
    """Fetch the list of Xero tenants the user has authorised."""
    resp = requests.get(
        XERO_CONNECTIONS_URL,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _xero_store_tokens(token_data: dict) -> None:
    """Persist token data and fetch the first connected tenant."""
    with _xero_token_lock:
        _xero_tokens["access_token"] = token_data["access_token"]
        _xero_tokens["refresh_token"] = token_data.get("refresh_token")
        _xero_tokens["expires_at"] = time.time() + token_data.get(
            "expires_in", 1800
        )

    # Fetch tenants and store the first one
    try:
        connections = _xero_fetch_connections(token_data["access_token"])
        if connections:
            with _xero_token_lock:
                _xero_tokens["tenant_id"] = connections[0]["tenantId"]
                _xero_tokens["tenant_name"] = connections[0].get("tenantName", "")
            logger.info(
                "Xero connected to tenant: %s (%s)",
                connections[0].get("tenantName"),
                connections[0]["tenantId"],
            )
    except Exception:
        logger.exception("Failed to fetch Xero connections after token exchange.")


# ═════════════════════════════════════════════════════════════════════════════
#  Flask routes
# ═════════════════════════════════════════════════════════════════════════════

APP_VERSION = "2.0.0-xero"  # bump to force Render redeploy


@app.route("/", methods=["GET"])
def health() -> Response:
    """Simple health-check endpoint — returns version and registered routes."""
    routes = sorted([r.rule for r in app.url_map.iter_rules()])
    body = f"ServiceM8 Webhook Server v{APP_VERSION} is running.\nRoutes: {', '.join(routes)}"
    return Response(body, status=200, content_type="text/plain")


@app.route("/version", methods=["GET"])
def version() -> Response:
    """Returns the current app version and all registered routes."""
    routes = sorted([r.rule for r in app.url_map.iter_rules()])
    return Response(
        json.dumps({"version": APP_VERSION, "routes": routes}),
        status=200,
        content_type="application/json",
    )


# ── Xero OAuth 2.0 routes ─────────────────────────────────────────────────────

@app.route("/xero/login", methods=["GET"])
def xero_login():
    """Redirect the user to Xero's authorisation page."""
    global _xero_oauth_state
    _xero_oauth_state = secrets.token_urlsafe(32)

    params = {
        "response_type": "code",
        "client_id": XERO_CLIENT_ID,
        "redirect_uri": XERO_REDIRECT_URI,
        "scope": XERO_SCOPES,
        "state": _xero_oauth_state,
    }
    auth_url = f"{XERO_AUTH_URL}?{urlencode(params)}"
    logger.info("Redirecting to Xero authorisation: %s", auth_url)
    return redirect(auth_url)


@app.route("/xero/callback", methods=["GET"])
def xero_callback():
    """Handle the Xero OAuth callback — exchange code for tokens."""
    error = request.args.get("error")
    if error:
        logger.error("Xero auth error: %s — %s", error, request.args.get("error_description", ""))
        return Response(
            f"Xero authorisation failed: {error}",
            status=400,
            content_type="text/plain",
        )

    code = request.args.get("code")
    state = request.args.get("state")

    if not code:
        return Response("Missing authorisation code.", status=400, content_type="text/plain")

    if state != _xero_oauth_state:
        logger.warning("Xero OAuth state mismatch: expected=%s, got=%s", _xero_oauth_state, state)
        return Response("Invalid state parameter.", status=400, content_type="text/plain")

    try:
        token_data = _xero_exchange_code(code)
        _xero_store_tokens(token_data)
        logger.info("Xero OAuth tokens stored successfully.")

        with _xero_token_lock:
            tenant_name = _xero_tokens.get("tenant_name", "Unknown")

        return Response(
            f"Xero connected successfully! Tenant: {tenant_name}\n"
            f"You can close this window.",
            status=200,
            content_type="text/plain",
        )
    except Exception as exc:
        logger.exception("Xero token exchange failed.")
        return Response(
            f"Xero token exchange failed: {exc}",
            status=500,
            content_type="text/plain",
        )


@app.route("/xero/status", methods=["GET"])
def xero_status():
    """Check whether Xero is connected and tokens are valid."""
    with _xero_token_lock:
        access_token = _xero_tokens.get("access_token")
        refresh_token = _xero_tokens.get("refresh_token")
        expires_at = _xero_tokens.get("expires_at", 0)
        tenant_id = _xero_tokens.get("tenant_id")
        tenant_name = _xero_tokens.get("tenant_name")

    if not access_token:
        return Response(
            json.dumps({
                "connected": False,
                "message": "Xero is not connected. Visit /xero/login to authorise.",
            }),
            status=200,
            content_type="application/json",
        )

    now = time.time()
    token_valid = now < expires_at
    expires_in_seconds = max(0, int(expires_at - now))

    return Response(
        json.dumps({
            "connected": True,
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "token_valid": token_valid,
            "access_token_expires_in_seconds": expires_in_seconds,
            "has_refresh_token": bool(refresh_token),
        }),
        status=200,
        content_type="application/json",
    )


@app.route("/xero/token", methods=["GET"])
def xero_token():
    """Return a valid Xero access token and tenant ID for API calls."""
    token = xero_get_valid_token()
    if not token:
        return Response(
            json.dumps({"error": "Xero not connected or token refresh failed"}),
            status=401,
            content_type="application/json",
        )
    with _xero_token_lock:
        tenant_id = _xero_tokens.get("tenant_id", "")
    return Response(
        json.dumps({"access_token": token, "tenant_id": tenant_id}),
        status=200,
        content_type="application/json",
    )


@app.route("/webhook", methods=["POST"])
def webhook() -> Response:
    """
    Main webhook endpoint.

    Handles:
      - ServiceM8 subscription verification  (mode=subscribe + challenge)
      - inbox.message_received event payloads
    """
    raw = request.get_data(as_text=True)
    content_type = request.content_type or ""
    logger.info("Webhook received | Content-Type: %s | body: %s", content_type, raw[:800])

    # Parse body — try JSON first, then form data, then raw JSON parse
    data: dict = {}
    if "application/json" in content_type:
        data = request.get_json(force=False, silent=True) or {}
    if not data:
        data = request.form.to_dict() or {}
    if not data:
        try:
            data = json.loads(raw)
        except Exception:
            data = {}

    # ── Subscription verification challenge ───────────────────────────────────
    mode = data.get("mode", "")
    challenge = data.get("challenge", "")
    if mode == "subscribe" and challenge:
        logger.info("Verification challenge — responding with: %s", challenge)
        return Response(challenge, status=200, content_type="text/plain")

    # Kill switch — enquiry processing disabled
    if not ENQUIRY_PROCESSING_ENABLED:
        logger.warning("!! ENQUIRY PROCESSING DISABLED — ignoring webhook payload: %s", raw[:200])
        return Response("OK", status=200)

    # ── Extract UUID from eventArgs.entry[0].uuid ─────────────────────────────
    uuid: str | None = None

    event_args = data.get("eventArgs", {})
    if isinstance(event_args, dict):
        entries = event_args.get("entry", [])
        if isinstance(entries, list) and entries:
            uuid = entries[0].get("uuid")
        if not uuid:
            uuid = event_args.get("uuid")

    # Top-level fallbacks
    if not uuid:
        entries = data.get("entry", [])
        if isinstance(entries, list) and entries:
            uuid = entries[0].get("uuid")
    if not uuid:
        uuid = data.get("uuid")

    if not uuid:
        logger.warning("No UUID found in payload: %s", raw[:400])
        return Response("OK", status=200)

    logger.info("Event UUID: %s", uuid)

    # Dedup check — prevent scheduling the same UUID twice
    with _processed_lock:
        if uuid in _processed_uuids:
            logger.info("UUID %s already queued/processed — skipping.", uuid)
            return Response("OK", status=200)
        _processed_uuids.add(uuid)

    # Schedule a one-shot delayed check instead of processing immediately.
    # After INBOX_DELAY_MINUTES the job re-fetches the message; if a human
    # has archived or converted it in the meantime, it skips auto-conversion.
    run_at = datetime.now(AEST) + timedelta(minutes=INBOX_DELAY_MINUTES)
    job_id = f"inbox_{uuid}"
    scheduler.add_job(
        _process_inbox_message,
        trigger="date",
        run_date=run_at,
        args=[uuid],
        id=job_id,
        replace_existing=True,
    )
    logger.info(
        "Scheduled delayed processing of inbox message %s at %s AEST (%d min delay).",
        uuid, run_at.strftime("%H:%M:%S"), INBOX_DELAY_MINUTES,
    )

    return Response("OK", status=200)


# ═══════════════════════════════════════════════════════════════════════════════
#  Scheduler setup
# ═══════════════════════════════════════════════════════════════════════════════

# Set to False to disable all scheduled jobs and webhook processing (emergency kill switch).
AUTOMATIONS_ENABLED = True

# !! ENQUIRY PROCESSING DISABLED — webhook and fallback scanner are OFF !!
# Set to True to re-enable enquiry auto-conversion.
ENQUIRY_PROCESSING_ENABLED = False

scheduler = BackgroundScheduler(timezone=AEST)

if AUTOMATIONS_ENABLED:
    # Fallback inbox scanner: DISABLED via ENQUIRY_PROCESSING_ENABLED flag
    if ENQUIRY_PROCESSING_ENABLED:
        scheduler.add_job(fallback_inbox_scanner, "interval", minutes=15,
                          id="fallback_inbox_scanner")
        logger.info("Fallback inbox scanner enabled (lookback: %d-%d min).",
                    INBOX_DELAY_MINUTES, INBOX_MAX_AGE_MINUTES)
    else:
        logger.warning("!! FALLBACK INBOX SCANNER DISABLED — ENQUIRY_PROCESSING_ENABLED=False !!")

    # Expired-quote checks: 9 AM and 5 PM AEST daily — ALWAYS ON
    scheduler.add_job(check_expired_quotes, "cron", hour=9, minute=0, id="expire_quotes_9am")
    scheduler.add_job(check_expired_quotes, "cron", hour=17, minute=0, id="expire_quotes_5pm")

    # Daily technician income report: 5 PM AEST daily — ALWAYS ON
    scheduler.add_job(daily_technician_income_report, "cron", hour=17, minute=0,
                      id="daily_income_report_5pm")

    scheduler.start()
    logger.info(
        "APScheduler started — expired-quote checks at 9 AM & 5 PM AEST, "
        "daily income report at 5 PM AEST. "
        "Enquiry processing: %s.",
        "ENABLED" if ENQUIRY_PROCESSING_ENABLED else "DISABLED",
    )
else:
    logger.warning(
        "!! ALL AUTOMATIONS DISABLED !! Set AUTOMATIONS_ENABLED=True to re-enable."
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  Entry point (development only — Render uses gunicorn)
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logger.info("Starting development server on port %s…", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=False)
