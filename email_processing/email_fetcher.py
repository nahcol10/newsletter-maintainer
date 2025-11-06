from typing import List, Dict, Optional
import logging
from .imap_connector import ImapConnector
from .email_searcher import EmailSearcher
from .email_parser import EmailParser
import time


class EmailFetcher:
    """Main email fetcher that orchestrates email processing with enhanced reliability"""

    def __init__(
        self,
        email_address: str,
        password: str,
        imap_server: str = "imap.gmail.com",
        port: int = 993,
        use_ssl: bool = True,
        timeout: int = 30,
    ):
        self.connector = ImapConnector(
            email_address, password, imap_server, port, use_ssl, timeout
        )
        self.searcher = None
        self.parser = None
        self.logger = logging.getLogger(__name__)
        self.connected = False

    def connect(self) -> bool:
        """Connect to email server with comprehensive error handling"""
        try:
            if self.connected:
                self.logger.info("✅ Already connected to email server")
                return True

            if not self.connector.connect():
                self.logger.error("❌ Failed to connect to IMAP server")
                return False

            connection = self.connector.get_connection()
            if not connection:
                self.logger.error("❌ No valid IMAP connection returned")
                return False

            self.searcher = EmailSearcher(connection)
            self.parser = EmailParser(connection)
            self.connected = True
            self.logger.info("✅ Email fetcher successfully initialized")
            return True

        except Exception as e:
            self.logger.error(f"❌ Error during connection setup: {e}")
            self.disconnect()
            return False

    def _ensure_connected(self) -> bool:
        """Ensure we have a valid connection, reconnecting if necessary"""
        if self.connected and self.connector.get_connection():
            return True

        self.logger.info("🔄 Reconnecting to email server...")
        self.disconnect()
        return self.connect()

    def fetch_emails_from_last_24_hours(self) -> List[Dict]:
        """Fetch emails from the last 24 hours with robust error handling"""
        if not self._ensure_connected():
            self.logger.error("❌ Email fetcher not connected")
            return []

        return self._fetch_emails_with_timeframe(1, "last 24 hours")

    def fetch_emails_from_last_7_days(self) -> List[Dict]:
        """Fetch emails from the past 7 days with robust error handling"""
        if not self._ensure_connected():
            self.logger.error("❌ Email fetcher not connected")
            return []

        return self._fetch_emails_with_timeframe(7, "last 7 days")

    def _fetch_emails_with_timeframe(
        self, days: int, timeframe_desc: str
    ) -> List[Dict]:
        """Generic method to fetch emails from a specific timeframe"""
        if not self.searcher or not self.parser:
            self.logger.error("❌ Email fetcher not properly initialized")
            return []

        try:
            self.logger.info(f"📧 Fetching emails from {timeframe_desc}...")

            # Search for emails
            email_ids = self.searcher.search_last_n_days(days)
            if not email_ids:
                self.logger.info(f"📭 No emails found from {timeframe_desc}")
                return []

            self.logger.info(f"📊 Found {len(email_ids)} emails from {timeframe_desc}")

            # Parse emails with progress tracking
            emails = []
            failed_count = 0

            for i, email_id in enumerate(email_ids, 1):
                self.logger.debug(
                    f"📝 Parsing email {i}/{len(email_ids)} (ID: {email_id.decode()})"
                )

                parsed_email = self.parser.parse_email(email_id)
                if parsed_email:
                    emails.append(parsed_email)
                else:
                    failed_count += 1
                    self.logger.warning(
                        f"⚠️ Failed to parse email {i}/{len(email_ids)} (ID: {email_id.decode()})"
                    )

                # Progress update every 10 emails
                if i % 10 == 0 or i == len(email_ids):
                    self.logger.info(
                        f"📈 Progress: {i}/{len(email_ids)} emails processed ({failed_count} failed)"
                    )

            self.logger.info(
                f"✅ Successfully fetched {len(emails)} emails from {timeframe_desc} ({failed_count} failed)"
            )
            return emails

        except Exception as e:
            self.logger.error(f"❌ Error fetching emails from {timeframe_desc}: {e}")
            return []

    def disconnect(self):
        """Disconnect from email server safely"""
        try:
            self.connector.disconnect()
            self.connected = False
            self.searcher = None
            self.parser = None
            self.logger.info("✅ Email fetcher disconnected successfully")
        except Exception as e:
            self.logger.warning(f"⚠️ Error during disconnect: {e}")

    def get_email_by_id(self, email_id: str) -> Optional[Dict]:
        """Get a single email by ID"""
        if not self._ensure_connected():
            return None

        try:
            parsed_email = self.parser.parse_email(email_id.encode())
            return parsed_email
        except Exception as e:
            self.logger.error(f"❌ Error fetching email by ID {email_id}: {e}")
            return None

    def test_connection(self) -> bool:
        """Test the email connection and basic functionality"""
        self.logger.info("🧪 Testing email connection...")

        if not self.connect():
            return False

        try:
            # Test basic search
            test_emails = self.searcher.search_last_n_days(1)
            self.logger.info(
                f"✅ Connection test successful! Found {len(test_emails)} recent emails"
            )
            return True
        except Exception as e:
            self.logger.error(f"❌ Connection test failed: {e}")
            return False
