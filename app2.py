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

def heartbeat():
    while True:
        print("❤️ Heartbeat: worker alive...", flush=True)
        time.sleep(10)

# Start heartbeat thread
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
SENDER_PASSWORD = "bi,dEd4qir.p"

UNSUBSCRIBE_API = "https://unsubscribe-uofn.onrender.com/get_unsubscribes"
TRACKING_BASE = "https://tracking-enfw.onrender.com"
UNSUBSCRIBE_BASE = "https://unsubscribe-uofn.onrender.com"

MAX_WORKERS = 4
BATCH_SIZE = 1000
SHEET_WRITE_SPLIT = 500
UK_TZ = ZoneInfo("Europe/London")

# ✅ Toggle UK time restriction ON/OFF
USE_UK_TIME_WINDOW = False  # True = Only run 8:00–9:00 UK | False = Run anytime once/day

# === GOOGLE SHEETS SETUP ===
creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)
leads_sheet = gc.open_by_key(SHEET_ID).worksheet(LEADS_TAB)
templates_sheet = gc.open_by_key(SHEET_ID).worksheet(TEMPLATES_TAB)

# === GLOBAL FLAG ===
is_sending = False  # ensures unsubscribe check pauses while sending
last_unsub_write = 0

# === UTILS ===
def fetch_unsubscribed():
    """Fetch unsubscribed list from API"""
    try:
        res = requests.get(UNSUBSCRIBE_API, timeout=10)
        res.raise_for_status()
        unsub_data = res.json().get("unsubscribed", [])
        print(f"📭 {len(unsub_data)} unsubscribed emails fetched.", flush=True)
        return set(email.lower() for email in unsub_data)
    except Exception as e:
        print(f"❌ Failed to fetch unsubscribed list: {e}", flush=True)
        return set()

PUBLIC_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "outlook.com",
    "hotmail.com", "live.com", "msn.com", "icloud.com", "me.com",
    "aol.com", "proton.me", "zoho.com", "gmx.com", "ymail.com"
}
def mark_unsubscribed_in_sheet(unsubscribed_set):
    """Mark ONLY exact unsubscribed emails. Bot logic removed completely."""
    try:
        global last_unsub_write
        now = time.time()

        if now - last_unsub_write < 600:
            print("⏳ Skipping unsubscribe check (limit: 1 per 10 min)", flush=True)
            return

        last_unsub_write = now

        # Load sheet
        all_rows = leads_sheet.get_all_values()
        headers = all_rows[0]

        if "Email" not in headers:
            print("⚠️ 'Email' column not found in sheet headers.")
            return

        email_idx = headers.index("Email") + 1

        updates = []
        marked_exact = 0

        # Scan sheet rows
        for i, row in enumerate(all_rows[1:], start=2):
            sheet_email = (row[email_idx - 1] or "").strip().lower()

            if sheet_email and sheet_email in unsubscribed_set:
                updates.append({"range": f"C{i}", "values": [["Unsubscribed"]]})
                marked_exact += 1

        # Write results
        if updates:
            leads_sheet.batch_update(updates)
            print(f"🚫 Marked {marked_exact} exact unsubscribes.", flush=True)
        else:
            print("✅ No new unsubscribes found.", flush=True)

    except Exception as e:
        print(f"❌ Failed to process unsubscribes: {e}", flush=True)

def save_to_sent_folder(raw_msg):
    """Save sent email to the correct IMAP Sent folder (INBOX.Sent)"""
    try:
        with imaplib.IMAP4_SSL(IMAP_SERVER, 993) as imap:
            imap.login(SENDER_EMAIL, SENDER_PASSWORD)
            sent_folder = "INBOX.Sent"
            imap.append(
                sent_folder,
                "",
                imaplib.Time2Internaldate(time.time()),
                raw_msg.encode("utf-8")
            )
            print(f"📥 Successfully saved email in '{sent_folder}' folder.", flush=True)
    except Exception as e:
        pass

def send_email(recipient, first_name, subject, html_body):
    """Send personalized email and save to Sent folder"""
    msg = MIMEMultipart("alternative")
    msg["From"] = formataddr(("Mike Randell", SENDER_EMAIL))
    msg["To"] = recipient
    msg["Subject"] = subject

    encoded_email = urllib.parse.quote_plus(recipient)
    encoded_subject = urllib.parse.quote_plus(subject)
    encoded_event_url = urllib.parse.quote_plus(
        "https://SOXPO26.eventbrite.co.uk/?aff=EMAILCAMPAIGNS"
    )

    tracking_link = f"{TRACKING_BASE}/track/click?email={encoded_email}&url={encoded_event_url}&subject={encoded_subject}"
    tracking_pixel = f'<img src="{TRACKING_BASE}/track/open?email={encoded_email}&subject={encoded_subject}" width="1" height="1" style="display:block;margin:0 auto;" alt="." />'
    unsubscribe_link = f"{UNSUBSCRIBE_BASE}/unsubscribe?email={encoded_email}"

    first_name = (first_name or "").strip() or "there"
    html_body = html_body.replace("{%name%}", first_name)

    unsubscribe_section = f"""
    <hr style="margin-top:30px;border:0;border-top:1px solid #ccc;">
    <div style="text-align:center;margin-top:10px;">
        <a href="{unsubscribe_link}" style="color:#d93025;text-decoration:none;font-size:12px;">Unsubscribe</a>
    </div>"""

    email_html = f"""
    <html><body style="font-family: Arial, sans-serif; color: #333; line-height:1.6;">
        <div style="max-width:600px;margin:auto;border:1px solid #ddd;border-radius:8px;padding:20px;">
          <p>Hi {first_name},</p>
          <p>{html_body}</p>
          
          {unsubscribe_section}
          {tracking_pixel}
        </div>
    </body></html>"""

    msg.attach(MIMEText(email_html, "html"))
    raw_msg = msg.as_string()

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=90) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipient, raw_msg)
        print(f"✅ Sent: {recipient}", flush=True)
        save_to_sent_folder(raw_msg)
        return True
    except Exception as e:
        print(f"❌ Failed {recipient}: {e}", flush=True)
        return False


def send_to_lead(row, i, templates_data, unsubscribed_set):
    """Send one email in sequence"""
    row_lower = {k.strip().lower(): v for k, v in row.items()}
    email = str(row_lower.get("email") or "").strip().lower()
    first_name = str(row_lower.get("first_name") or "").strip()
    status = str(row_lower.get("status") or "").strip()

    raw_count = row_lower.get("followup_count")
    try:
        if raw_count is None:
            count = 0
        else:
            raw_count = str(raw_count).strip()
            count = int(raw_count) if raw_count.isdigit() else 0
    except:
        count = 0

    if not email or status.lower() == "unsubscribed":
        return (i, None, None, None, f"⏭️ Skipped {email}")
    if email in unsubscribed_set:
        return (i, "Unsubscribed", None, None, f"🚫 {email} unsubscribed")

    next_num = count + 1
    template_row = next((t for t in templates_data if str(t.get("Template")) == str(next_num)), None)
    if not template_row:
        return (i, None, None, None, f"⚠️ Template {next_num} not found")

    subject = (template_row.get("Subject Line") or f"Update {next_num}").strip()
    body = (template_row.get("HTML Body") or "").strip()
    sent_ok = send_email(email, first_name, subject, body)

    time.sleep(1.2)
    now_str = datetime.now(UK_TZ).strftime("%Y-%m-%d %H:%M:%S")

    if sent_ok:
        return (i, f"Email Sent - {next_num}", now_str, str(next_num), f"✅ Sent {email}")
    else:
        return (i, "Not Delivered", now_str, str(next_num), f"❌ Failed {email}")

def send_batch(leads_batch, start_index, templates_data, unsubscribed_set):
    """Send a single 1k batch"""
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(send_to_lead, row, start_index + i, templates_data, unsubscribed_set)
            for i, row in enumerate(leads_batch)
        ]
        for f in as_completed(futures):
            results.append(f.result())
    return results


def run_campaign():
    """Send all leads in 10k batches"""
    global is_sending
    is_sending = True
    print("\n🚀 Running daily email campaign...", flush=True)
    unsubscribed_set = fetch_unsubscribed()

    leads_data = leads_sheet.get_all_records()
    templates_data = templates_sheet.get_all_records()
    total = len(leads_data)
    print(f"🧩 Templates: {len(templates_data)} | Leads: {total}", flush=True)

    for batch_start in range(0, total, BATCH_SIZE):
        print("❤️ Heartbeat inside campaign loop...", flush=True)
        print("⏳ Still working... starting batch", flush=True)
        batch_end = min(batch_start + BATCH_SIZE, total)
        leads_batch = leads_data[batch_start:batch_end]
        print(f"\n📦 Sending batch {batch_start+1}-{batch_end} ({len(leads_batch)} leads)...", flush=True)

        results = send_batch(leads_batch, batch_start + 2, templates_data, unsubscribed_set)

        def write_to_sheet(result_half):
            batch_updates = []
            for (row_i, status, timestamp, count, log) in result_half:
                if status:
                    batch_updates.append({"range": f"C{row_i}", "values": [[status]]})
                    if timestamp:
                        batch_updates.append({"range": f"D{row_i}", "values": [[timestamp]]})
                    if count:
                        batch_updates.append({"range": f"E{row_i}", "values": [[count]]})
            if batch_updates:
                leads_sheet.batch_update(batch_updates)
                print(f"📝 Updated {len(batch_updates)} cells.", flush=True)
            
        write_to_sheet(results)
        print("🔄 Running unsubscribe check before 5 sec wait...", flush=True)
        unsub_set_after_batch = fetch_unsubscribed()
        if unsub_set_after_batch:
           mark_unsubscribed_in_sheet(unsub_set_after_batch)


        print("🔄 Quick cool-down before next batch...", flush=True)

        print("✅ Batch complete. Sleeping 5 seconds before next batch...", flush=True)
        time.sleep(5)

    print("🎉 All batches completed.", flush=True)
    is_sending = False


def scheduler_loop():
    """Main scheduler loop"""
    global is_sending
    last_sent_date = None
    last_unsub_check = datetime.now(UK_TZ) - timedelta(hours=2)

    print("🕒 Scheduler started (checks every 10 min)...", flush=True)

    while True:
        try:
            now_uk = datetime.now(UK_TZ)
            today_str = now_uk.strftime("%Y-%m-%d")

            # Every 15 min unsubscribe check
            if not is_sending and (now_uk - last_unsub_check).total_seconds() >= 900:
                unsubscribed_set = fetch_unsubscribed()
                if unsubscribed_set:
                    mark_unsubscribed_in_sheet(unsubscribed_set)
                last_unsub_check = now_uk

            # UK time window (11:00–12:00 UK)
            campaign_start = now_uk.replace(hour=8, minute=0, second=0, microsecond=0)
            campaign_end = now_uk.replace(hour=9, minute=0, second=0, microsecond=0)

            should_run = False
            if USE_UK_TIME_WINDOW:
                if campaign_start <= now_uk < campaign_end:
                    should_run = True
            else:
                should_run = True  # no restriction

            if last_sent_date != today_str and should_run:
                if USE_UK_TIME_WINDOW:
                    print(f"⏰ UK time window matched ({now_uk.strftime('%H:%M')} UK) — starting campaign.", flush=True)
                else:
                    print(f"🚀 UK time window OFF — starting campaign immediately.", flush=True)
                run_campaign()
                last_sent_date = today_str
            else:
                print(f"🕓 Current time: {now_uk.strftime('%H:%M')} UK — waiting...", flush=True)

            time.sleep(600)

        except Exception as e:
            print(f"⚠️ Scheduler error: {e}", flush=True)
            time.sleep(600)


# === ENTRY POINT ===
if __name__ == "__main__":
    scheduler_loop()
