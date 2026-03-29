# ServiceM8 Webhook Server

A lightweight Flask webhook server that listens for ServiceM8 **`inbox.message_received`** events and automatically processes new online enquiries.

## What it does

When a new inbox message arrives in ServiceM8 this server will:

1. Respond to ServiceM8's subscription verification challenge instantly.
2. Fetch the full inbox message details from the ServiceM8 API.
3. Skip any message that is already archived or already converted to a job.
4. Convert the inbox message to a **Quote** job using the Standard job template.
5. Update the job description with the standard template, inserting the customer's enquiry into the `ORIGINAL CUSTOMER ENQUIRY` field.
6. Set the `work_done_description` to the standard warranty / site-inspection boilerplate.
7. Post a Slack notification to **#new-enquiry-received** with the job number, client name, address, enquiry text, email, and phone number.

All heavy processing runs in a background thread so the webhook endpoint returns **HTTP 200 immediately**.

---

## Deploy to Render (recommended)

### Option A — render.yaml (auto-detected)

1. Fork or clone this repository to your own GitHub account.
2. Go to [Render](https://render.com) → **New** → **Web Service** → **Public Git Repository**.
3. Paste your GitHub repo URL and click **Connect**.
4. Render will detect `render.yaml` and pre-fill the settings.
5. Confirm and click **Create Web Service**.

### Option B — manual settings

| Setting | Value |
|---|---|
| **Runtime** | Python 3 |
| **Build Command** | `pip install -r requirements.txt` |
| **Start Command** | `gunicorn server:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120` |

### Environment variables

Set these in Render's **Environment** tab (or they fall back to the defaults already in the code):

| Variable | Description | Default (fallback) |
|---|---|---|
| `SERVICEM8_API_KEY` | ServiceM8 API key | hardcoded in code |
| `SLACK_CHANNEL_ID` | Slack channel ID for notifications | `C0AQCK3SZUG` |

---

## Register the webhook with ServiceM8

After your Render service is live, register the webhook once:

```bash
curl -X POST https://api.servicem8.com/webhook_subscriptions/event \
  -H "X-API-Key: YOUR_SERVICEM8_API_KEY" \
  -d "event=inbox.message_received" \
  -d "callback_url=https://YOUR-RENDER-URL.onrender.com/webhook"
```

Expected response: `{"success":true}`

---

## Local development

```bash
pip install -r requirements.txt
SERVICEM8_API_KEY=your_key SLACK_CHANNEL_ID=your_channel_id python server.py
```

The server starts on **port 5000** by default. Use [ngrok](https://ngrok.com) or similar to expose it publicly for local testing.

---

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Health check — returns `200 OK` |
| `POST` | `/webhook` | ServiceM8 webhook receiver |

---

## Job description template

```
JOB TYPE: 

SERVICE: 

ORIGINAL CUSTOMER ENQUIRY: <enquiry text inserted here>

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
```
