import smtplib
import imaplib
import requests
import time
import gspread
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import urllib.parse
import threading
import os

def heartbeat():
    while True:
        print("❤️ Heartbeat: worker alive...", flush=True)
        time.sleep(10)

threading.Thread(target=heartbeat, daemon=True).start()

# === CONFIGURATION ===
SERVICE_ACCOUNT_FILE = "/etc/secrets/credentials.json"
SHEET_ID = "1Mm-v9NE1rycySiQaKG3Lr2heRcEtlc1XQbuCrOOqT8I"
LEADS_TAB = "Email-campaigns"
TEMPLATES_TAB = "Templates"

SMTP_SERVER = "mail.southamptonbusinessexpo.com"
SMTP_PORT = 587
IMAP_SERVER = "mail.southamptonbusinessexpo.com"
SENDER_EMAIL = "mike@southamptonbusinessexpo.com"
SENDER_PASSWORD = "Geecon0404"

UNSUBSCRIBE_API = "https://unsubscribe-uofn.onrender.com/get_unsubscribes"
TRACKING_BASE = "https://tracking-enfw.onrender.com"
UNSUBSCRIBE_BASE = "https://unsubscribe-uofn.onrender.com"

MAX_WORKERS = 15
BATCH_SIZE = 1000
SHEET_WRITE_SPLIT = 500
UK_TZ = ZoneInfo("Europe/London")

USE_UK_TIME_WINDOW = False

# === GOOGLE SHEETS SETUP ===
creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)
leads_sheet = gc.open_by_key(SHEET_ID).worksheet(LEADS_TAB)
templates_sheet = gc.open_by_key(SHEET_ID).worksheet(TEMPLATES_TAB)

# === GLOBAL FLAGS ===
is_sending = False
last_unsub_write = 0

# ====== SEND ORDER TOGGLE STORAGE ======
ORDER_FLAG_FILE = "/tmp/campaign_order_flag.txt"

def get_send_order_flag():
    """Reads stored order flag. Default = 'normal' (top→bottom)."""
    try:
        if os.path.exists(ORDER_FLAG_FILE):
            with open(ORDER_FLAG_FILE, "r") as f:
                flag = f.read().strip()
                if flag in ("normal", "reverse"):
                    return flag
    except:
        pass
    return "reverse"

def toggle_send_order_flag():
    """Flip between 'normal' and 'reverse'."""
    current = get_send_order_flag()
    new_flag = "reverse" if current == "normal" else "normal"
    try:
        with open(ORDER_FLAG_FILE, "w") as f:
            f.write(new_flag)
        print(f"🔁 Next campaign order will be: {new_flag}", flush=True)
    except Exception as e:
        print(f"⚠️ Could not update order flag: {e}", flush=True)
    return new_flag

# ===================================================

def fetch_unsubscribed():
    try:
        res = requests.get(UNSUBSCRIBE_API, timeout=10)
        res.raise_for_status()
        unsub_data = res.json().get("unsubscribed", [])
        print(f"📭 {len(unsub_data)} unsubscribed emails fetched.", flush=True)
        return set(email.lower() for email in unsub_data)
    except Exception as e:
        print(f"❌ Failed to fetch unsubscribed list: {e}", flush=True)
        return set()

def mark_unsubscribed_in_sheet(unsubscribed_set):
    try:
        global last_unsub_write
        now = time.time()

        if now - last_unsub_write < 600:
            print("⏳ Skipping unsubscribe check (limit: 1 per 10 min)", flush=True)
            return

        last_unsub_write = now

        all_rows = leads_sheet.get_all_values()
        headers = all_rows[0]

        if "Email" not in headers:
            print("⚠️ 'Email' column missing.")
            return

        email_idx = headers.index("Email") + 1

        updates = []
        count = 0

        for i, row in enumerate(all_rows[1:], start=2):
            sheet_email = (row[email_idx - 1] or "").strip().lower()
            if sheet_email in unsubscribed_set:
                updates.append({"range": f"C{i}", "values": [["Unsubscribed"]]})
                count += 1

        if updates:
            leads_sheet.batch_update(updates)
            print(f"🚫 Marked {count} unsubscribes.", flush=True)
        else:
            print("✅ No new unsubscribes.", flush=True)

    except Exception as e:
        print(f"❌ Failed unsub write: {e}", flush=True)

# ============================ EMAIL SEND ============================

def save_to_sent_folder(raw_msg):
    try:
        with imaplib.IMAP4_SSL(IMAP_SERVER, 993) as imap:
            imap.login(SENDER_EMAIL, SENDER_PASSWORD)
            imap.append(
                "INBOX.Sent",
                "",
                imaplib.Time2Internaldate(time.time()),
                raw_msg.encode("utf-8")
            )
            imap.logout()
    except Exception as e:
        print(f"⚠️ IMAP save failed: {e}", flush=True)

def send_email(recipient, first_name, subject, html_body):
    msg = MIMEMultipart("alternative")
    msg["From"] = formataddr(("Mike Randell", SENDER_EMAIL))
    msg["To"] = recipient
    msg["Subject"] = subject

    encoded_email = urllib.parse.quote_plus(recipient)
    encoded_subject = urllib.parse.quote_plus(subject)
    encoded_event_url = urllib.parse.quote_plus(
        "https://SouthamptonBusinessShow29Jan26.eventbrite.co.uk/?aff=EMAILCAMPAIGNS"
    )

    tracking_link = f"{TRACKING_BASE}/track/click?email={encoded_email}&url={encoded_event_url}&subject={encoded_subject}"
    tracking_pixel = f'<img src="{TRACKING_BASE}/track/open?email={encoded_email}&subject={encoded_subject}" width="1" height="1"/>'
    unsubscribe_link = f"{UNSUBSCRIBE_BASE}/unsubscribe?email={encoded_email}"

    first_name = first_name or "there"
    html_body = html_body.replace("{%name%}", first_name)

    email_html = f"""
    <html><body>
        <p>Hi {first_name},</p>
        <p>{html_body}</p>
        <a href="{tracking_link}">Book your ticket</a>
        <hr>
        <a href="{unsubscribe_link}">Unsubscribe</a>
        {tracking_pixel}
    </body></html>
    """

    msg.attach(MIMEText(email_html, "html"))
    raw = msg.as_string()

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipient, raw)
        save_to_sent_folder(raw)
        return True
    except:
        return False

# ============================ SEND LOGIC ============================

def send_to_lead(row, row_index, templates, unsub_set):
    email = (row.get("Email") or "").strip().lower()
    first = row.get("First_Name", "")
    status = row.get("Status", "").lower()

    if not email or status == "unsubscribed":
        return (row_index, None, None, None, "")

    if email in unsub_set:
        return (row_index, "Unsubscribed", None, None, "")

    count = int(row.get("Followup_Count") or 0)
    next_num = count + 1

    template = next((x for x in templates if str(x.get("Template")) == str(next_num)), None)
    if not template:
        return (row_index, None, None, None, "")

    subject = template["Subject Line"]
    body = template["HTML Body"]

    sent = send_email(email, first, subject, body)
    now = datetime.now(UK_TZ).strftime("%Y-%m-%d %H:%M:%S")

    if sent:
        return (row_index, f"Email Sent - {next_num}", now, next_num, "")
    else:
        return (row_index, "Not Delivered", now, next_num, "")


def send_batch(batch, start_row, templates, unsub_set):
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [
            ex.submit(send_to_lead, row, start_row + idx, templates, unsub_set)
            for idx, row in enumerate(batch)
        ]
        for f in as_completed(futures):
            results.append(f.result())
    return results

# ============================ CAMPAIGN ============================

def run_campaign():
    global is_sending
    is_sending = True

    print("\n🚀 Running campaign...", flush=True)

    unsub_set = fetch_unsubscribed()
    leads = leads_sheet.get_all_records()
    templates = templates_sheet.get_all_records()

    total = len(leads)
    print(f"📊 Leads: {total}", flush=True)

    # ---------- APPLY SEND ORDER ----------
    order_flag = get_send_order_flag()
    print(f"📌 Current send order: {order_flag}", flush=True)

    if order_flag == "reverse":
        print("🔽 Sending from bottom → top", flush=True)
        leads = list(reversed(leads))
    else:
        print("🔼 Sending from top → bottom", flush=True)

    # ---------- PROCESS BATCHES ----------
    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = leads[batch_start:batch_end]

        print(f"\n📦 Batch {batch_start+1}-{batch_end}", flush=True)

        # Real sheet row index depends on reversed order handling:
        if order_flag == "reverse":
            start_row = 2 + (total - batch_end)
        else:
            start_row = 2 + batch_start

        results = send_batch(batch, start_row, templates, unsub_set)

        updates = []
        for row_i, status, timestamp, count, _ in results:
            if status:
                updates.append({"range": f"C{row_i}", "values": [[status]]})
            if timestamp:
                updates.append({"range": f"D{row_i}", "values": [[timestamp]]})
            if count:
                updates.append({"range": f"E{row_i}", "values": [[count]]})

        if updates:
            leads_sheet.batch_update(updates)
            print(f"📝 Updated {len(updates)} cells", flush=True)

        time.sleep(3)

    print("🎉 Campaign finished.", flush=True)

    # After finishing: flip order for next time
    toggle_send_order_flag()

    is_sending = False


# ============================ SCHEDULER ============================

def scheduler_loop():
    last_sent = None
    last_unsub_check = datetime.now(UK_TZ) - timedelta(hours=2)

    while True:
        now = datetime.now(UK_TZ)
        today = now.strftime("%Y-%m-%d")

        if (now - last_unsub_check).total_seconds() >= 900:
            unsub = fetch_unsubscribed()
            if unsub:
                mark_unsubscribed_in_sheet(unsub)
            last_unsub_check = now

        should_run = True

        if should_run and last_sent != today:
            run_campaign()
            last_sent = today
        else:
            print(f"⏳ Waiting... {now.strftime('%H:%M:%S')}", flush=True)

        time.sleep(600)

# ===== ENTRY POINT =====
if __name__ == "__main__":
    scheduler_loop()
