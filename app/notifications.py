import smtplib
import os
from email.message import EmailMessage
from concurrent.futures import ThreadPoolExecutor

_executor = ThreadPoolExecutor(max_workers=2)


def _send_email(to_email: str, subject: str, body: str):
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")

    if not smtp_host or not smtp_user or not smtp_pass:
        # SMTP not configured; log to console
        print("[notifications] SMTP not configured, would send:", to_email, subject)
        print(body)
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as s:
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.send_message(msg)
    except Exception as e:
        print("[notifications] Failed sending email:", e)


def send_email_async(to_email: str, subject: str, body: str):
    _executor.submit(_send_email, to_email, subject, body)


def send_sms_async(phone: str, message: str):
    # Stub: integrate with Twilio or other SMS provider
    def _send(phone, message):
        print(f"[notifications] SMS to {phone}: {message}")
    _executor.submit(_send, phone, message)
