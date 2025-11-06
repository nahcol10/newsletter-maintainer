import email
import imaplib
from typing import Dict, Optional
import logging
import re
from bs4 import BeautifulSoup


class EmailParser:
    """Parse email messages and extract content with enhanced HTML handling"""

    def __init__(self, mail_connection: imaplib.IMAP4_SSL):
        self.mail = mail_connection
        self.logger = logging.getLogger(__name__)

    def parse_email(self, email_id: bytes) -> Optional[Dict]:
        """Parse a single email and return structured data with comprehensive error handling"""
        try:
            if not self.mail:
                self.logger.error("❌ No IMAP connection available for parsing")
                return None

            # Fetch email with BODY.PEEK to avoid marking as read
            status, msg_data = self.mail.fetch(email_id, "(RFC822.HEADER BODY.PEEK[])")
            if status != "OK":
                self.logger.error(f"❌ Failed to fetch email {email_id}: {status}")
                return None

            if not msg_data or len(msg_data) < 2:
                self.logger.error(f"❌ Invalid email data for ID {email_id}")
                return None

            # Parse email headers first
            raw_headers = (
                msg_data[0][1] if isinstance(msg_data[0], tuple) else msg_data[0]
            )
            if isinstance(raw_headers, bytes):
                email_message = email.message_from_bytes(raw_headers)
            else:
                email_message = email.message_from_string(
                    raw_headers.decode("utf-8", errors="ignore")
                )

            # Extract basic info from headers
            subject = self._decode_header(email_message.get("Subject", ""))
            sender = self._decode_header(email_message.get("From", ""))
            date = self._decode_header(email_message.get("Date", ""))
            message_id = self._decode_header(
                email_message.get("Message-ID", "")
            ).strip()

            # Get full email content
            raw_email = (
                msg_data[1][1] if isinstance(msg_data[1], tuple) else msg_data[1]
            )
            if isinstance(raw_email, bytes):
                full_email = email.message_from_bytes(raw_email)
            else:
                full_email = email.message_from_string(
                    raw_email.decode("utf-8", errors="ignore")
                )

            # Extract body content
            body = self._extract_body_comprehensive(full_email)

            # Check for unsubscribe links
            has_unsubscribe = self._has_unsubscribe_options(full_email)

            return {
                "id": email_id.decode(),
                "message_id": message_id,
                "subject": subject,
                "sender": sender,
                "date": date,
                "body": body,
                "has_unsubscribe": has_unsubscribe,
                "content_type": self._get_content_type(full_email),
                "size": len(raw_email),
            }

        except imaplib.IMAP4.abort as e:
            self.logger.error(
                f"❌ IMAP connection aborted while parsing email {email_id}: {e}"
            )
            return None
        except Exception as e:
            self.logger.error(f"❌ Error parsing email {email_id}: {e}")
            return None

    def _decode_header(self, header_value: str) -> str:
        """Decode email headers that might be encoded"""
        if not header_value:
            return ""

        try:
            decoded_parts = email.header.decode_header(header_value)
            decoded_string = ""
            for part, charset in decoded_parts:
                if isinstance(part, bytes):
                    decoded_string += part.decode(charset or "utf-8", errors="replace")
                else:
                    decoded_string += str(part)
            return decoded_string.strip()
        except Exception as e:
            self.logger.warning(f"⚠️ Error decoding header: {e}")
            return header_value.strip()

    def _extract_body_comprehensive(self, email_message) -> str:
        """Extract text body from email message with comprehensive fallbacks"""
        try:
            # Priority 1: Look for text/plain parts
            plain_text = self._extract_plain_text(email_message)
            if plain_text and len(plain_text.strip()) > 50:
                return self._clean_text_content(plain_text)

            # Priority 2: Extract from HTML if no good plain text
            html_content = self._extract_html_content(email_message)
            if html_content and len(html_content.strip()) > 50:
                return self._clean_text_content(html_content)

            # Priority 3: Fallback to payload
            payload = self._extract_payload_fallback(email_message)
            if payload and len(payload.strip()) > 20:
                return self._clean_text_content(payload)

            # Last resort: Return subject and sender info
            self.logger.warning(
                "⚠️ No substantial body content found, using minimal fallback"
            )
            return f"Email from {email_message.get('From', '')} with subject: {email_message.get('Subject', '')}"

        except Exception as e:
            self.logger.error(f"❌ Error extracting body: {e}")
            return ""

    def _extract_plain_text(self, email_message) -> str:
        """Extract plain text content from email"""
        text_parts = []

        if email_message.is_multipart():
            for part in email_message.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))

                # Skip attachments
                if "attachment" in content_disposition.lower():
                    continue

                if content_type == "text/plain":
                    try:
                        charset = part.get_content_charset() or "utf-8"
                        payload = part.get_payload(decode=True)
                        if isinstance(payload, bytes):
                            text = payload.decode(charset, errors="replace")
                            text_parts.append(text)
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error extracting plain text: {e}")
                        continue
        else:
            if email_message.get_content_type() == "text/plain":
                try:
                    charset = email_message.get_content_charset() or "utf-8"
                    payload = email_message.get_payload(decode=True)
                    if isinstance(payload, bytes):
                        return payload.decode(charset, errors="replace")
                except Exception as e:
                    self.logger.warning(
                        f"⚠️ Error extracting single-part plain text: {e}"
                    )

        return "\n\n".join(text_parts) if text_parts else ""

    def _extract_html_content(self, email_message) -> str:
        """Extract and clean HTML content from email"""
        html_parts = []

        if email_message.is_multipart():
            for part in email_message.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))

                if "attachment" in content_disposition.lower():
                    continue

                if content_type == "text/html":
                    try:
                        charset = part.get_content_charset() or "utf-8"
                        payload = part.get_payload(decode=True)
                        if isinstance(payload, bytes):
                            html = payload.decode(charset, errors="replace")
                            html_parts.append(html)
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error extracting HTML: {e}")
                        continue
        else:
            if email_message.get_content_type() == "text/html":
                try:
                    charset = email_message.get_content_charset() or "utf-8"
                    payload = email_message.get_payload(decode=True)
                    if isinstance(payload, bytes):
                        return payload.decode(charset, errors="replace")
                except Exception as e:
                    self.logger.warning(f"⚠️ Error extracting single-part HTML: {e}")

        if html_parts:
            # Use the first HTML part (usually the main content)
            return self._clean_html_content(html_parts[0])

        return ""

    def _clean_html_content(self, html_content: str) -> str:
        """Clean HTML content and extract meaningful text"""
        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # Remove script, style, and other non-content elements
            for element in soup(
                [
                    "script",
                    "style",
                    "head",
                    "title",
                    "meta",
                    "link",
                    "iframe",
                    "nav",
                    "footer",
                ]
            ):
                element.decompose()

            # Remove email-specific elements
            for element in soup.find_all(["table", "tr", "td"]):
                if "role" in element.attrs and element["role"] == "presentation":
                    continue

            # Get text content
            text = soup.get_text(separator="\n", strip=True)

            # Clean up the text
            lines = [line.strip() for line in text.split("\n") if line.strip()]
            cleaned_text = "\n".join(lines)

            return cleaned_text

        except Exception as e:
            self.logger.warning(f"⚠️ Error cleaning HTML: {e}")
            # Fallback to simple text extraction
            return re.sub(r"<[^>]+>", "", html_content)

    def _clean_text_content(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""

        # Remove excessive whitespace
        text = re.sub(r"\s+", " ", text)

        # Remove common email footers and signatures
        footer_patterns = [
            r"--\s*$",  # Signature separator
            r"Best regards,.*",
            r"Sincerely,.*",
            r"Thanks,.*",
            r"Kind regards,.*",
            r"Sent from my.*",
            r"Proudly powered by.*",
            r"Unsubscribe.*",
            r"View this email in your browser.*",
            r"This email was sent to.*",
        ]

        for pattern in footer_patterns:
            text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.MULTILINE)

        # Remove URLs and email addresses (keep the text)
        text = re.sub(r"https?://\S+", "", text)
        text = re.sub(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", "", text)

        return text.strip()

    def _has_unsubscribe_options(self, email_message) -> bool:
        """Check if email contains unsubscribe options"""
        try:
            # Check headers first
            headers = str(email_message).lower()
            unsubscribe_keywords = [
                "unsubscribe",
                "opt-out",
                "opt out",
                "remove me",
                "manage preferences",
            ]

            if any(keyword in headers for keyword in unsubscribe_keywords):
                return True

            # Check body content
            body = self._extract_body_comprehensive(email_message).lower()
            return any(keyword in body for keyword in unsubscribe_keywords)

        except Exception as e:
            self.logger.warning(f"⚠️ Error checking unsubscribe options: {e}")
            return False

    def _get_content_type(self, email_message) -> str:
        """Determine the primary content type of the email"""
        if email_message.is_multipart():
            content_types = []
            for part in email_message.walk():
                content_types.append(part.get_content_type())
            if "text/html" in content_types:
                return "html"
            elif "text/plain" in content_types:
                return "text"
            return "multipart"
        return email_message.get_content_type()
