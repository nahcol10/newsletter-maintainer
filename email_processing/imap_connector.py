import imaplib
import ssl
from typing import Optional
import logging
from datetime import datetime
import time


class ImapConnector:
    """Handle IMAP server connections with enhanced Gmail support"""

    def __init__(
        self,
        email_address: str,
        password: str,
        imap_server: str = "imap.gmail.com",
        port: int = 993,
        use_ssl: bool = True,
        timeout: int = 30,
    ):
        self.email_address = email_address
        self.password = password
        self.imap_server = imap_server
        self.port = port
        self.use_ssl = use_ssl
        self.timeout = timeout
        self.mail: Optional[imaplib.IMAP4_SSL] = None
        self.connected_at = None
        self.logger = logging.getLogger(__name__)
        self.max_retries = 3
        self.retry_delay = 2  # seconds between retries

    def connect(self) -> bool:
        """Connect to IMAP server with Gmail-specific authentication support"""
        for attempt in range(self.max_retries):
            try:
                self.logger.info(
                    f"🔍 Connecting to {self.imap_server}:{self.port} (SSL: {self.use_ssl})"
                )

                # Create proper SSL context for Gmail
                context = ssl.create_default_context()
                context.minimum_version = ssl.TLSVersion.TLSv1_2
                context.set_ciphers("DEFAULT:@SECLEVEL=1")

                # Create connection with timeout
                self.mail = imaplib.IMAP4_SSL(
                    self.imap_server,
                    self.port,
                    ssl_context=context,
                    timeout=self.timeout,
                )

                # Attempt login
                self.logger.info(f"🔐 Attempting login for: {self.email_address}")
                self.mail.login(self.email_address, self.password)

                self.connected_at = datetime.now()
                self.logger.info(
                    f"✅ Successfully connected to IMAP server: {self.email_address}"
                )
                return True

            except imaplib.IMAP4.error as e:
                error_msg = str(e).lower()
                self.logger.error(f"❌ IMAP protocol error: {e}")

                # Gmail-specific error handling
                if (
                    "authenticationfailed" in error_msg
                    or "invalid credentials" in error_msg
                ):
                    self._handle_gmail_auth_failure()
                elif "too many login failures" in error_msg:
                    self.logger.error(
                        "⚠️ Too many login failures. Gmail may be blocking connections."
                    )
                    self.logger.error("   • Wait 5-10 minutes before retrying")
                    self.logger.error(
                        "   • Check if you're using the correct App Password"
                    )

                if attempt < self.max_retries - 1:
                    self.logger.info(
                        f"⏳ Waiting {self.retry_delay} seconds before retry {attempt + 1}/{self.max_retries}..."
                    )
                    time.sleep(self.retry_delay)
                else:
                    return False

            except TimeoutError:
                self.logger.error(f"❌ Connection timeout after {self.timeout} seconds")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    return False

            except ssl.SSLError as e:
                self.logger.error(f"❌ SSL/TLS error: {e}")
                self.logger.error("   • Gmail requires TLS 1.2 or higher")
                self.logger.error(
                    "   • Ensure your system has updated SSL certificates"
                )
                return False

            except Exception as e:
                self.logger.error(f"❌ Unexpected connection error: {e}")
                return False

        return False

    def _handle_gmail_auth_failure(self):
        """Provide helpful guidance for Gmail authentication failures"""
        self.logger.error(
            "❌ Gmail authentication failed. This is common with modern Gmail security."
        )
        self.logger.error("🔧 Here are the solutions:")
        self.logger.error("   1️⃣ If you have 2-Step Verification enabled (recommended):")
        self.logger.error("      • Go to: https://myaccount.google.com/apppasswords")
        self.logger.error("      • Create an App Password for 'Mail'")
        self.logger.error(
            "      • Use that 16-digit password instead of your regular password"
        )
        self.logger.error("   2️⃣ If you don't have 2-Step Verification:")
        self.logger.error("      • Go to: https://myaccount.google.com/lesssecureapps")
        self.logger.error("      • Enable 'Allow less secure apps'")
        self.logger.error("   3️⃣ For GSuite/Work accounts:")
        self.logger.error("      • Contact your administrator to enable IMAP access")
        self.logger.error("      • You may need domain-wide delegation permissions")

    def disconnect(self):
        """Close IMAP connection safely"""
        if self.mail:
            try:
                if self.mail.state not in ["LOGOUT", "NONAUTH"]:
                    self.mail.close()
                self.mail.logout()
                self.logger.info("📤 Disconnected from IMAP server")
            except Exception as e:
                self.logger.warning(f"⚠️ Error during disconnect: {e}")
            finally:
                self.mail = None
                self.connected_at = None

    def get_connection(self) -> Optional[imaplib.IMAP4_SSL]:
        """Get the IMAP connection with health check"""
        if not self.mail:
            return None

        try:
            # Check if connection is still alive
            status, _ = self.mail.noop()
            if status == "OK":
                return self.mail
            else:
                self.logger.warning(
                    "⚠️ IMAP connection appears to be dead. Reconnecting..."
                )
                self.disconnect()
                return None
        except Exception as e:
            self.logger.warning(f"⚠️ Connection health check failed: {e}")
            self.disconnect()
            return None

    def ensure_connection(self) -> bool:
        """Ensure we have a valid connection, reconnecting if necessary"""
        connection = self.get_connection()
        if connection:
            return True

        self.logger.info("🔄 Attempting to reconnect to IMAP server...")
        return self.connect()
