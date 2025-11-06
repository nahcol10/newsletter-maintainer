import os
import logging
from typing import List, Dict, Optional
from datetime import datetime
from dotenv import load_dotenv
from email_processing.email_fetcher import EmailFetcher
from processors.email_filters import EmailFilters
import sys


class EmailDailyProcessor:
    """Process daily emails with enhanced logging and error handling"""

    def __init__(self):
        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)

        # Load environment variables
        load_dotenv()

        # Get credentials with validation
        self.email_address = os.getenv("EMAIL_ADDRESS")
        self.email_password = os.getenv("EMAIL_PASSWORD")

        if not self.email_address or not self.email_password:
            self.logger.error(
                "❌ EMAIL_ADDRESS and EMAIL_PASSWORD must be set in environment variables"
            )
            raise ValueError("Missing required environment variables")

        # Initialize components
        self.fetcher = EmailFetcher(
            self.email_address,
            self.email_password,
            timeout=60,  # Longer timeout for daily processing
            use_ssl=True,
        )
        self.filters = EmailFilters()
        self.processed_count = 0
        self.failed_count = 0

    def _setup_logging(self):
        """Setup logging configuration"""
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_file = os.path.join(
            log_dir, f"daily_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
        )

    def process_daily_emails(self) -> List[Dict]:
        """Main method to process daily emails with comprehensive error handling"""
        start_time = datetime.now()
        self.logger.info("🚀 Starting daily email processing...")
        self.logger.info(f"📅 Processing emails from last 24 hours")
        self.logger.info(f"📧 Using email account: {self.email_address}")

        try:
            # Step 1: Connect to IMAP
            if not self._connect_to_email():
                self.logger.error("❌ Failed to connect to email server")
                return []

            # Step 2: Fetch daily emails (last 24 hours only)
            daily_emails = self._fetch_daily_emails()
            if not daily_emails:
                self.logger.info("📭 No emails found from last 24 hours")
                return []

            # Step 3: Apply primitive filtering (unsubscribe detection)
            newsletter_candidates = self._apply_primitive_filtering(daily_emails)

            # Summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            self.logger.info(
                f"✅ Daily email processing completed in {duration:.2f} seconds"
            )
            self.logger.info(f"📊 Summary:")
            self.logger.info(f"   • Total emails fetched: {len(daily_emails)}")
            self.logger.info(
                f"   • Newsletter candidates: {len(newsletter_candidates)}"
            )
            self.logger.info(
                f"   • Processing success rate: {len(newsletter_candidates)}/{len(daily_emails)} ({len(newsletter_candidates) / len(daily_emails) * 100:.1f}%)"
            )

            return newsletter_candidates

        except Exception as e:
            self.logger.error(f"❌ Critical error in daily email processing: {e}")
            import traceback

            self.logger.error(traceback.format_exc())
            return []
        finally:
            # Always disconnect
            self._disconnect_from_email()

    def _connect_to_email(self) -> bool:
        """Step 1: Connect to IMAP - Gmail connection with authentication"""
        self.logger.info("🔐 Connecting to Gmail IMAP server...")
        try:
            if self.fetcher.connect():
                self.logger.info("✅ Successfully connected to Gmail IMAP")
                return True
            else:
                self.logger.error("❌ Failed to connect to Gmail IMAP")
                return False
        except Exception as e:
            self.logger.error(f"❌ IMAP connection error: {e}")
            return False

    def _fetch_daily_emails(self) -> List[Dict]:
        """Step 2: Fetch daily emails - Get emails from last 24 hours only"""
        self.logger.info("📧 Fetching emails from last 24 hours...")
        try:
            emails = self.fetcher.fetch_emails_from_last_24_hours()

            if emails:
                # Log some sample email info for debugging
                sample_emails = emails[:3]  # First 3 emails
                self.logger.info(f"📋 Sample emails found:")
                for i, email in enumerate(sample_emails, 1):
                    subject = email.get("subject", "No Subject")[:50]
                    sender = email.get("sender", "No Sender")[:50]
                    date = email.get("date", "No Date")
                    self.logger.info(
                        f"   {i}. From: {sender}, Subject: {subject}, Date: {date}"
                    )

            return emails
        except Exception as e:
            self.logger.error(f"❌ Error fetching daily emails: {e}")
            return []

    def _apply_primitive_filtering(self, emails: List[Dict]) -> List[Dict]:
        """Step 3: Apply primitive filtering - Filter for newsletter candidates using unsubscribe detection"""
        self.logger.info("🔍 Applying primitive filtering (unsubscribe detection)...")
        try:
            # CORRECTED: Use the apply_primitive_filtering method which takes a list of emails
            filtered_emails = self.filters.apply_primitive_filtering(emails)

            self.logger.info(
                f"📊 Primitive filtering results: {len(emails)} → {len(filtered_emails)} newsletter candidates"
            )
            return filtered_emails

        except Exception as e:
            self.logger.error(f"❌ Error applying primitive filtering: {e}")
            import traceback

            self.logger.error(traceback.format_exc())
            # Return original list if filtering fails
            return emails

    def _get_filter_rejection_reason(self, email: Dict) -> str:
        """Get detailed reason why an email was filtered out"""
        subject = email.get("subject", "").lower()
        body = email.get("body", "").lower()
        has_unsubscribe = email.get("has_unsubscribe", False)

        # Check common rejection reasons
        transactional_keywords = [
            "verification code",
            "confirm your",
            "reset your password",
            "your account has been",
            "account verification",
            "please verify",
            "confirm your email",
            "activate your account",
            "password reset",
            "login attempt",
            "security alert",
            "suspicious activity",
            "invoice #",
            "receipt #",
            "payment confirmation",
            "order confirmation",
            "shipment",
            "delivery notification",
            "transaction completed",
            "payment failed",
            "card declined",
        ]

        for keyword in transactional_keywords:
            if keyword in subject or keyword in body:
                return f"Contains transactional keyword: '{keyword}'"

        if not has_unsubscribe:
            return "No unsubscribe option found (likely not a newsletter)"

        return "Other filtering criteria"

    def _disconnect_from_email(self):
        """Disconnect from email server"""
        try:
            self.fetcher.disconnect()
            self.logger.info("📤 Disconnected from email server")
        except Exception as e:
            self.logger.warning(f"⚠️ Error disconnecting: {e}")


def main():
    """Test the daily email processor"""
    try:
        processor = EmailDailyProcessor()
        newsletter_candidates = processor.process_daily_emails()

        print(f"\n📋 Summary:")
        print(f"Newsletter candidates found: {len(newsletter_candidates)}")

        if newsletter_candidates:
            print("\nFirst 5 newsletter candidates:")
            for i, email in enumerate(newsletter_candidates[:5], 1):
                subject = email.get("subject", "No Subject")
                sender = email.get("sender", "No Sender")
                date = email.get("date", "No Date")
                print(f"{i}. {subject} - {sender} ({date})")

        return newsletter_candidates

    except Exception as e:
        print(f"❌ Error in main: {e}")
        import traceback

        traceback.print_exc()
        return []


if __name__ == "__main__":
    main()
