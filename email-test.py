#!/usr/bin/env python3
import smtplib
import logging
from email.mime.text import MIMEText
import sys

# --- CONFIGURATION ---
SMTP_HOST     = "smtp.gmail.com"
SMTP_PORT     = 587
SMTP_SENDER   = "dhaneshdhakrey@gmail.com"
# To force a failure for testing, set this to an incorrect password:
SMTP_PASSWORD = "****"
RECIPIENT     = "dsddhakrey421@gmail.com"

# --- SET UP LOGGING ---
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

# Root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# File handler for script events
file_handler = logging.FileHandler("email_script.log", mode="a")
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

# Stream handler for console output
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

logger.info("=== Starting email send process ===")

server = None
smtp_debug_log = None

try:
    # Prepare message
    msg = MIMEText("This is a test email sent directly from a Python script to check SMTP settings.")
    msg["Subject"] = "Direct SMTP Test from Python"
    msg["From"]    = SMTP_SENDER
    msg["To"]      = RECIPIENT

    # Open SMTP debug file
    smtp_debug_log = open("smtp_debug.log", "a")
    logger.debug("Opened smtp_debug.log for SMTP protocol output")

    # Connect to SMTP server
    logger.info(f"Connecting to {SMTP_HOST}:{SMTP_PORT}")
    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10)
    server.debugfile     = smtp_debug_log
    server.set_debuglevel(1)

    # Start TLS
    logger.info("Starting TLS")
    server.starttls()

    # Login
    logger.info(f"Logging in as {SMTP_SENDER}")
    server.login(SMTP_SENDER, SMTP_PASSWORD)

    # Send the email
    logger.info(f"Sending email to {RECIPIENT}")
    server.sendmail(SMTP_SENDER, [RECIPIENT], msg.as_string())

    logger.info("Email sent successfully!")

except Exception:
    # Captures traceback into email_script.log
    logger.exception("FAILED to send email")

finally:
    # Clean up SMTP connection
    if server:
        try:
            server.quit()
            logger.debug("SMTP connection closed cleanly")
        except Exception:
            logger.warning("Error when quitting SMTP connection", exc_info=True)
    # Close debug log file
    if smtp_debug_log:
        smtp_debug_log.close()
        logger.debug("smtp_debug.log file handle closed")

    logger.info("=== Email script finished ===")
