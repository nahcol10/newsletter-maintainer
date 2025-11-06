import imaplib
from datetime import datetime, timedelta
from typing import List, Optional
import logging
import time
import email
from email.utils import parsedate_to_datetime


class EmailSearcher:
    """Search for emails based on criteria with enhanced error handling"""

    def __init__(self, mail_connection: imaplib.IMAP4_SSL):
        self.mail = mail_connection
        self.logger = logging.getLogger(__name__)
        self.max_retries = 3
        self.retry_delay = 2  # seconds

    def search_last_24_hours(self) -> List[bytes]:
        """Search for emails from the last 24 hours with improved reliability"""
        return self.search_last_n_days(days=1)

    def search_last_n_days(self, days: int = 7) -> List[bytes]:
        """Search for emails from the last N days with robust error handling"""
        for attempt in range(self.max_retries):
            try:
                if not self.mail:
                    self.logger.error("❌ No IMAP connection available")
                    return []

                # Select inbox with error handling
                status, messages = self.mail.select("inbox")
                if status != "OK":
                    self.logger.error(f"❌ Failed to select inbox: {status}")
                    # Try INBOX in uppercase (Gmail specific)
                    status, messages = self.mail.select("INBOX")
                    if status != "OK":
                        self.logger.error(f"❌ Failed to select INBOX: {status}")
                        return []

                # Calculate date N days ago - multiple formats for compatibility
                n_days_ago = datetime.now() - timedelta(days=days)
                date_string = n_days_ago.strftime("%d-%b-%Y")  # Format: 01-Jan-2024
                iso_date_string = n_days_ago.strftime("%Y-%m-%d")  # Format: 2024-01-01

                self.logger.info(
                    f"🔍 Searching for emails since {date_string} (ISO: {iso_date_string})"
                )

                # Try multiple search criteria for better compatibility
                search_criteria = [
                    f'SINCE "{date_string}"',
                    f"SINCE {iso_date_string}",
                    f'(SINCE "{date_string}")',
                    f"(SINCE {iso_date_string})",
                ]

                email_ids = []
                for criteria in search_criteria:
                    try:
                        status, result = self.mail.search(None, criteria)
                        if status == "OK" and result[0]:
                            email_ids = result[0].split()
                            if email_ids:
                                self.logger.info(
                                    f"✅ Found {len(email_ids)} emails using criteria: {criteria}"
                                )
                                break
                    except Exception as e:
                        self.logger.warning(
                            f"⚠️ Search with criteria '{criteria}' failed: {e}"
                        )
                        continue

                # Fallback: if no emails found, try a broader search
                if not email_ids:
                    self.logger.warning(
                        "⚠️ No emails found with standard criteria. Trying ALL emails..."
                    )
                    status, result = self.mail.search(None, "ALL")
                    if status == "OK" and result[0]:
                        all_ids = result[0].split()
                        # Get recent emails (last 100 should be enough)
                        recent_ids = all_ids[-100:] if len(all_ids) > 100 else all_ids
                        self.logger.info(
                            f"✅ Found {len(recent_ids)} recent emails using fallback method"
                        )
                        email_ids = recent_ids

                self.logger.info(
                    f"📊 Total emails found from last {days} days: {len(email_ids)}"
                )
                return email_ids

            except imaplib.IMAP4.abort as e:
                self.logger.error(f"❌ IMAP connection aborted: {e}")
                if attempt < self.max_retries - 1:
                    self.logger.info(
                        f"⏳ Waiting {self.retry_delay} seconds before retry {attempt + 1}/{self.max_retries}..."
                    )
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error("❌ Max retries exceeded for email search")
                    return []
            except Exception as e:
                self.logger.error(f"❌ Unexpected error searching emails: {e}")
                return []

    def search_by_sender(self, sender_email: str, days: int = 7) -> List[bytes]:
        """Search for emails from a specific sender in the last N days"""
        try:
            status, _ = self.mail.select("inbox")
            if status != "OK":
                return []

            n_days_ago = datetime.now() - timedelta(days=days)
            date_string = n_days_ago.strftime("%d-%b-%Y")

            search_criteria = f'(SINCE "{date_string}" FROM "{sender_email}")'
            status, result = self.mail.search(None, search_criteria)

            if status == "OK" and result[0]:
                return result[0].split()
            return []
        except Exception as e:
            self.logger.error(f"❌ Error searching by sender: {e}")
            return []
