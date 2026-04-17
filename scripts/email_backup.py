#!/usr/bin/env python3
"""Email a backup file as an attachment via Gmail SMTP.

Environment variables required:
  BACKUP_EMAIL_TO      recipient address (usually same as sender)
  BACKUP_EMAIL_USER    Gmail address used for SMTP auth
  BACKUP_EMAIL_APP_PW  16-char app-password (NOT the account password)

Exits non-zero on any failure so systemd OnFailure= can alert.
"""
import os
import sys
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: email_backup.py <file>", file=sys.stderr)
        return 2

    path = Path(sys.argv[1])
    if not path.is_file():
        print(f"ERROR: not a file: {path}", file=sys.stderr)
        return 2

    try:
        to = os.environ["BACKUP_EMAIL_TO"]
        user = os.environ["BACKUP_EMAIL_USER"]
        pw = os.environ["BACKUP_EMAIL_APP_PW"]
    except KeyError as exc:
        print(f"ERROR: missing env var {exc}", file=sys.stderr)
        return 2

    size_mb = path.stat().st_size / 1_000_000
    if size_mb > 24:
        # Gmail limits attachments to 25MB; fail loudly rather than
        # silently truncating. Caller can switch to link-sharing if
        # the DB outgrows this.
        print(f"ERROR: {path.name} is {size_mb:.1f} MB, > 24 MB Gmail limit", file=sys.stderr)
        return 3

    msg = EmailMessage()
    msg["Subject"] = f"[value-invest] backup {path.name} ({size_mb:.1f} MB)"
    msg["From"] = user
    msg["To"] = to
    msg.set_content(
        f"Automated daily backup.\n\n"
        f"File: {path.name}\n"
        f"Size: {path.stat().st_size:,} bytes ({size_mb:.2f} MB)\n"
        f"Host: {os.uname().nodename}\n"
    )
    msg.add_attachment(
        path.read_bytes(),
        maintype="application",
        subtype="gzip" if path.suffix == ".gz" else "octet-stream",
        filename=path.name,
    )

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx, timeout=30) as s:
            s.login(user, pw)
            s.send_message(msg)
    except Exception as exc:
        print(f"ERROR: SMTP failure: {exc}", file=sys.stderr)
        return 4

    print(f"OK emailed {path.name} to {to} ({size_mb:.2f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
