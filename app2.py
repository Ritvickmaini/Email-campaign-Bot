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

# === CONFIGURATION ===
SERVICE_ACCOUNT_FILE = "/etc/secrets/credentials.json"
SHEET_ID = "1Mm-v9NE1rycySiQaKG3Lr2heRcEtlc1XQbuCrOOqT8I"
LEADS_TAB = "Email-campaigns"
TEMPLATES_TAB = "Templates"
# === FEATURE TOGGLE ===
USE_UK_TIME_WINDOW = True  # 🔄 Set to False to send instantly (ignore UK time restriction)


SMTP_SERVER = "mail.southamptonbusinessexpo.com"
SMTP_PORT = 587
IMAP_SERVER = "mail.southamptonbusinessexpo.com"
SENDER_EMAIL = "mike@southamptonbusinessexpo.com"
SENDER_PASSWORD = "Geecon0404"

UNSUBSCRIBE_API = "https://unsubscribe-uofn.onrender.com/get_unsubscribes"
TRACKING_BASE = "https://tracking-enfw.onrender.com"
UNSUBSCRIBE_BASE = "https://unsubscribe-uofn.onrender.com"

MAX_WORKERS = 20
BATCH_SIZE = 10000
SHEET_WRITE_SPLIT = 5000
UK_TZ = ZoneInfo("Europe/London")

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


def mark_unsubscribed_in_sheet(unsubscribed_set):
    """Mark unsubscribed users by exact email or domain (excluding gmail/outlook/yahoo)"""
    try:
        # Extract domains from unsubscribed emails
        unsubscribed_domains = {
            email.split("@")[1].lower().strip()
            for email in unsubscribed_set
            if "@" in email
        }

        skip_domains = {"gmail.com", "outlook.com", "yahoo.com", "hotmail.com", "live.com"}

        all_emails = leads_sheet.col_values(2)
        updates = []
        marked_exact = 0
        marked_domain = 0

        for i, email in enumerate(all_emails[1:], start=2):
            email = (email or "").strip().lower()
            if not email or "@" not in email:
                continue

            domain = email.split("@")[1]

            # === Rule 1: Exact email match ===
            if email in unsubscribed_set:
                updates.append({"range": f"C{i}", "values": [["Unsubscribed"]]})
                marked_exact += 1
                continue

            # === Rule 2: Domain match (only if not a free provider) ===
            if (
                domain not in skip_domains
                and domain in unsubscribed_domains
            ):
                updates.append({"range": f"C{i}", "values": [["Unsubscribed"]]})
                marked_domain += 1

        # === Apply updates ===
        if updates:
            leads_sheet.batch_update(updates)
            print(f"🚫 Marked {len(updates)} unsubscribed users — {marked_exact} exact, {marked_domain} by domain.", flush=True)
        else:
            print("✅ No new unsubscribes to mark.", flush=True)

    except Exception as e:
        print(f"❌ Failed to mark unsubscribed users: {e}", flush=True)



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
            imap.logout()
    except Exception as e:
        print(f"⚠️ IMAP save failed: {e}", flush=True)


def send_email(recipient, first_name, subject, html_body):
    """Send personalized email and save to Sent folder"""
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
    tracking_pixel = f'<img src="{TRACKING_BASE}/track/open?email={encoded_email}&subject={encoded_subject}" width="1" height="1" style="display:block;margin:0 auto;" alt="." />'
    unsubscribe_link = f"{UNSUBSCRIBE_BASE}/unsubscribe?email={encoded_email}"

    first_name = (first_name or "").strip() or "there"
    html_body = html_body.replace("{%name%}", first_name)

    cta_button = f"""
    <div style="text-align:left;margin:30px 0;">
        <a href="{tracking_link}" 
           style="background-color:#d93025;color:white;padding:12px 28px;
                  text-decoration:none;border-radius:6px;display:inline-block;
                  font-weight:bold;font-size:16px;">
            🎟️ Book Your Visitor Ticket
        </a>
    </div>"""

    signature_block = """
    <br><br>
    <div style="color:#000;font-weight:bold;">
        Best regards,<br>
        <strong>Mike Randell</strong><br>
        Marketing Executive | B2B Growth Expo<br>
        <a href="mailto:mike@southamptionbusinessshow.com" style="color:#000;text-decoration:none;">mike@southamptionbusinessshow.com</a><br>
        (+44) 2034517166
    </div>"""

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
          {cta_button}
          {signature_block}
          {unsubscribe_section}
          {tracking_pixel}
        </div>
    </body></html>"""

    msg.attach(MIMEText(email_html, "html"))
    raw_msg = msg.as_string()

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
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
    """Process and send email for a single lead safely."""
    try:
        # === Utility helpers ===
        def safe_str(value):
            return str(value).strip() if value is not None else ""

        def safe_int(value):
            try:
                if isinstance(value, (int, float)):
                    return int(value)
                return int(str(value).strip() or 0)
            except Exception:
                return 0

        # === Normalize row keys ===
        row_lower = {str(k).strip().lower(): v for k, v in row.items()}

        # === Safely extract values ===
        email = safe_str(row_lower.get("email")).lower()
        first_name = safe_str(row_lower.get("first_name"))
        status = safe_str(row_lower.get("status"))
        count = safe_int(row_lower.get("followup_count"))
        last_followup = safe_str(row_lower.get("last-followup-date"))
        reply_status = safe_str(row_lower.get("reply status"))

        # === Skip invalid or unsubscribed ===
        if not email or email in unsubscribed_set:
            return (i, email, None, None, "⏭️ Skipped (Invalid or Unsubscribed)")

        # === Get follow-up template ===
        body, subject = templates_data.get(count + 1, (None, None))
        if not body or not subject:
            return (i, email, None, None, "⏭️ No template found")

        # === Send the email ===
        send_email(email, first_name, subject, body)

        # === Return update info ===
        now_str = datetime.now(ZoneInfo("Europe/London")).strftime("%Y-%m-%d %H:%M:%S")
        return (i, email, now_str, count + 1, "✅ Email Sent")

    except Exception as e:
        print(f"⚠️ Data error in row {i}: {e} | Row data: {row}", flush=True)
        return (i, None, None, None, f"⚠️ Skipped due to error: {e}")

def send_batch(leads_batch, start_index, templates_data, unsubscribed_set):
    """Send a single 10k batch"""
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

        half = len(results) // 2
        print("💾 Writing first half...", flush=True)
        write_to_sheet(results[:half])
        print("💾 Writing second half...", flush=True)
        write_to_sheet(results[half:])

        print("✅ Batch complete. Sleeping 30 minutes before next batch...", flush=True)
        time.sleep(1800)

    print("🎉 All batches completed.", flush=True)
    is_sending = False


def scheduler_loop():
    """Main scheduler loop with optional UK time restriction"""
    global is_sending
    last_sent_date = None
    last_unsub_check = datetime.now(UK_TZ) - timedelta(hours=2)

    print(f"🕒 Scheduler started (checks every 10 min)...", flush=True)
    print(f"⏳ UK Time Restriction: {'ON (11:00–12:00 UK only)' if USE_UK_TIME_WINDOW else 'OFF (Send instantly)'}", flush=True)

    while True:
        try:
            now_uk = datetime.now(UK_TZ)
            today_str = now_uk.strftime("%Y-%m-%d")

            # === Unsubscribe check every hour ===
            if not is_sending and (now_uk - last_unsub_check).total_seconds() >= 3600:
                unsubscribed_set = fetch_unsubscribed()
                if unsubscribed_set:
                    mark_unsubscribed_in_sheet(unsubscribed_set)
                last_unsub_check = now_uk

            # === Campaign control ===
            if USE_UK_TIME_WINDOW:
                campaign_start = now_uk.replace(hour=11, minute=0, second=0, microsecond=0)
                campaign_end = now_uk.replace(hour=12, minute=0, second=0, microsecond=0)

                if (
                    last_sent_date != today_str
                    and campaign_start <= now_uk < campaign_end
                ):
                    print(f"⏰ Time window matched ({now_uk.strftime('%H:%M')} UK) — starting campaign.", flush=True)
                    run_campaign()
                    last_sent_date = today_str
                else:
                    print(f"🕓 Current time: {now_uk.strftime('%H:%M')} UK — waiting for 11:00 UK window...", flush=True)
            else:
                if not is_sending:
                    print("🚀 Instant send mode enabled — starting campaign immediately.", flush=True)
                    run_campaign()
                    last_sent_date = today_str
                else:
                    print("⏳ Campaign already running...", flush=True)

            time.sleep(600)

        except Exception as e:
            print(f"⚠️ Scheduler error: {e}", flush=True)
            time.sleep(600)

# === ENTRY POINT ===
if __name__ == "__main__":
    scheduler_loop()
