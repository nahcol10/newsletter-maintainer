from typing import List, Dict, Tuple, Optional, Set
import re
from email.utils import parseaddr
from email.header import decode_header
import html
from html import unescape
from bs4 import BeautifulSoup
from config import UNSUBSCRIBE_KEYWORDS
import json
import logging
from datetime import datetime
import base64


class EmailFilters:
    """Advanced email filtering with multi-dimensional newsletter detection"""

    def __init__(self, debug_mode: bool = False):
        self.debug_mode = debug_mode
        self.logger = self._setup_logger()

        # Comprehensive transactional keywords with categories
        self.transactional_patterns = {
            "security": [
                r"verification\s+code",
                r"confirm\s+your\s+email",
                r"reset\s+your\s+password",
                r"account\s+verification",
                r"please\s+verify",
                r"login\s+attempt",
                r"security\s+alert",
                r"suspicious\s+activity",
                r"account\s+locked",
                r"password\s+expired",
                r"verify\s+your\s+identity",
                r"two[-\s]?factor",
            ],
            "transaction": [
                r"invoice\s*#",
                r"receipt\s*#",
                r"payment\s+confirmation\b",
                r"order\s+confirmation\b",
                r"transaction\s+completed",
                r"payment\s+failed\b",
                r"card\s+declined",
                r"your\s+order\b",
                r"order\s+status\b",
                r"purchase\b",
                r"billing\b",
                r"shipping\b",
                r"delivery\s+notification\b",
            ],
            "system": [
                r"account\s+created",
                r"welcome\s+to",
                r"password\s+changed",
                r"email\s+address\s+updated",
                r"profile\s+updated",
                r"subscription\s+changed",
            ],
        }

        # Positive newsletter indicators with weights
        self.newsletter_indicators = [
            ("weekly", 0.4),
            ("monthly", 0.4),
            ("daily", 0.3),
            ("newsletter", 0.5),
            ("update", 0.3),
            ("digest", 0.5),
            ("curated", 0.4),
            ("roundup", 0.4),
            ("recap", 0.4),
            ("trending", 0.3),
            ("featured", 0.3),
            ("highlight", 0.3),
            ("in this edition", 0.5),
            ("table of contents", 0.6),
            ("issue #", 0.5),
            ("edition #", 0.5),
            ("this week in", 0.4),
            ("top stories", 0.4),
        ]

        # Known newsletter platform domains
        self.newsletter_platforms = [
            "substack.com",
            "beehiiv.com",
            "convertkit.com",
            "mailchimp.com",
            "buttondown.email",
            "revue.co",
            "tinyletter.com",
            "ghost.io",
            "newsletter.com",
            "tinyletter.com",
            "sendinblue.com",
            "constantcontact.com",
            "campaignmonitor.com",
            "activecampaign.com",
            "getbee.io",
            "mailerlite.com",
        ]

        # Common newsletter sender patterns
        self.newsletter_sender_patterns = [
            r"^(newsletter|digest|updates?|news|hello|team|editor|bot)@",
            r"@.*(?:daily|weekly|monthly|newsletter|digest|bulletin|update|roundup)\.",
            r"^(?:[\w\s]+)\s*<[\w._%+-]+@(?:[\w-]+\.)+(?:com|org|io|co|net)>$",
            r"\b(?:team|editor|founder|ceo|community)\b.*<.*>",
        ]

        # Unsubscribe detection patterns
        self.unsubscribe_patterns = [
            r"\b(unsubscribe|opt[-\s]?out|remove\s+me|stop\s+emails|manage\s+preferences|email\s+preferences|subscription\s+preferences)\b",
            r'<a[^>]+?href=["\'][^"\']*?(?:unsubscribe|opt[-_]?out)[^"\']*?["\'][^>]*?>',
            r'class=["\'][^"\']*?(?:unsubscribe|opt[-_]?out)[^"\']*?["\']',
            r'id=["\'][^"\']*?(?:unsubscribe|opt[-_]?out)[^"\']*?["\']',
        ]

        self.unsubscribe_headers = [
            "list-unsubscribe",
            "list-unsubscribe-post",
            "list-manage",
        ]

        # Content structure indicators
        self.content_patterns = [
            (r"<h[1-3][^>]*>.*?section.*?</h[1-3]>", 0.3),  # Section headers
            (r"<h[1-3][^>]*>.*?<\/h[1-3]>", 0.2),  # Any headers
            (r"<table[^>]*?>.*?</table>", 0.1),  # Tables (common in newsletters)
            (r"<img[^>]*?>", 0.1),  # Images
            (
                r'class=["\'][^"\']*?(?:story|article|feature|highlight|section)[^"\']*?["\']',
                0.3,
            ),
            (r"<div[^>]+?(?:article|section|story)[^>]*?>", 0.3),
            (r">\s*read\s+(?:more|full|article)\s*<", 0.4),
            (r">\s*continue\s+reading\s*<", 0.4),
        ]

        # Common false-positive patterns to reduce filtering of legitimate newsletters
        self.false_positive_patterns = [
            r"\b(thanks|thank you)\b\s*for\s+subscribing",  # Welcome emails (often newsletters)
            r"\bconfirm\b.*\bsubscription\b",  # Subscription confirmations
            r"\bwelcome\b.*\bnewsletter\b",  # Welcome newsletters
            r"(?i)you\'?re (?:in|all set|subscribed)",  # Subscription confirmations
        ]

        # Domain reputation scores (higher = more likely to be newsletters)
        self.domain_reputation = {
            # Major tech newsletters
            "techcrunch.com": 0.9,
            "wired.com": 0.9,
            "theverge.com": 0.9,
            "arstechnica.com": 0.9,
            "axios.com": 0.9,
            "bloomberg.com": 0.9,
            "reuters.com": 0.9,
            "ft.com": 0.9,
            # Finance newsletters
            "morningbrew.com": 0.9,
            "finimize.com": 0.9,
            "thestreet.com": 0.8,
            # Industry newsletters
            "sifted.eu": 0.8,
            "mc.ben-evans.com": 0.9,
            "stratechery.com": 0.9,
            # Common ESPs
            "mailchimp.com": 0.7,
            "sendgrid.net": 0.6,
            "mandrillapp.com": 0.6,
            # Potential transactional
            "paypal.com": 0.1,
            "amazon.com": 0.1,
            "ebay.com": 0.1,
            "apple.com": 0.2,
        }

        # Initialize statistics tracking
        self.stats = {
            "total_processed": 0,
            "newsletters_detected": 0,
            "transactional_detected": 0,
            "detection_pattern_counts": {},
        }

    def _setup_logger(self):
        """Setup logger for debug mode"""
        logger = logging.getLogger("EmailFilters")
        if self.debug_mode:
            logger.setLevel(logging.DEBUG)
            if not logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
                handler.setFormatter(formatter)
                logger.addHandler(handler)
        else:
            logger.setLevel(logging.INFO)
        return logger

    def apply_primitive_filtering(self, emails: List[Dict]) -> List[Dict]:
        """Apply enhanced filtering to identify newsletters more accurately"""
        self.logger.info(
            f"🔍 Applying enhanced newsletter detection to {len(emails)} emails..."
        )
        filtered_emails = []
        decisions = []

        for i, email in enumerate(emails):
            decision = self._analyze_email_comprehensively(email, i)
            decisions.append(decision)

            if decision["should_keep"]:
                filtered_emails.append(email)
                self.logger.debug(
                    f"✅ Email {decision['email_id']} kept as newsletter (score: {decision['total_score']:.2f})"
                )
            else:
                self.logger.debug(
                    f"❌ Email {decision['email_id']} filtered out as transactional (score: {decision['total_score']:.2f})"
                )

            # Track statistics
            pattern = decision["primary_pattern"]
            self.stats["detection_pattern_counts"][pattern] = (
                self.stats["detection_pattern_counts"].get(pattern, 0) + 1
            )
            self.stats["total_processed"] += 1
            if decision["should_keep"]:
                self.stats["newsletters_detected"] += 1
            else:
                self.stats["transactional_detected"] += 1

        # Log summary
        self._log_summary(decisions)

        return filtered_emails

    def _analyze_email_comprehensively(self, email: Dict, index: int) -> Dict:
        """Comprehensive analysis of a single email"""
        email_id = email.get("id", f"unknown_{index}")
        subject = self._get_clean_text(email.get("subject", ""))
        sender = self._get_clean_text(email.get("sender", ""))
        body = self._get_clean_text(email.get("body", ""))
        html_body = email.get("html_body", "")
        headers = email.get("headers", {})

        # Extract email domain
        _, email_address = parseaddr(sender)
        domain = email_address.split("@")[-1] if "@" in email_address else ""

        # Extract display name
        display_name = self._extract_display_name(sender)

        # Multi-dimensional analysis
        analysis = {
            "unsubscribe_score": self._analyze_unsubscribe_presence(
                email, headers, body, html_body
            ),
            "sender_score": self._analyze_sender_comprehensive(
                sender, email_address, display_name, domain
            ),
            "content_score": self._analyze_content_comprehensive(
                subject, body, html_body
            ),
            "structural_score": self._analyze_email_structure(html_body),
            "transactional_score": self._analyze_transactional_patterns(subject, body),
            "domain_reputation": self._get_domain_reputation(domain),
            "engagement_signals": self._analyze_engagement_signals(body, html_body),
        }

        # Calculate total score with weighted components
        weights = {
            "unsubscribe_score": 2.5,  # High importance
            "sender_score": 1.5,  # Medium-high importance
            "content_score": 1.8,  # High importance
            "structural_score": 1.2,  # Medium importance
            "transactional_score": -2.0,  # Negative weight (penalty)
            "domain_reputation": 1.0,  # Medium importance
            "engagement_signals": 0.8,  # Low-medium importance
        }

        total_score = sum(analysis[key] * weights[key] for key in weights)

        # Apply adaptive threshold based on email characteristics
        threshold = self._get_adaptive_threshold(analysis)

        # Determine primary detection pattern for statistics
        primary_pattern = self._determine_primary_pattern(
            analysis, total_score, threshold
        )

        # Log detailed decision if in debug mode
        if self.debug_mode:
            self._log_detailed_decision(
                email_id,
                subject,
                sender,
                analysis,
                total_score,
                threshold,
                primary_pattern,
            )

        return {
            "email_id": email_id,
            "subject": subject,
            "sender": sender,
            "total_score": total_score,
            "threshold": threshold,
            "should_keep": total_score >= threshold,
            "analysis": analysis,
            "primary_pattern": primary_pattern,
        }

    def _get_clean_text(self, text: str) -> str:
        """Clean text by handling encoding and HTML entities"""
        if not text:
            return ""

        # Handle bytes if present
        if isinstance(text, bytes):
            try:
                text = text.decode("utf-8")
            except UnicodeDecodeError:
                text = text.decode("latin-1", errors="ignore")

        # Decode email header encoding if needed
        if isinstance(text, str) and ("=?" in text or "?=" in text):
            try:
                decoded_parts = decode_header(text)
                decoded_text = ""
                for part, charset in decoded_parts:
                    if isinstance(part, bytes):
                        decoded_text += part.decode(charset or "utf-8", errors="ignore")
                    else:
                        decoded_text += part
                text = decoded_text
            except Exception as e:
                self.logger.debug(f"Header decoding error: {e}")

        # Handle HTML entities
        try:
            text = unescape(text)
        except Exception as e:
            self.logger.debug(f"HTML unescape error: {e}")

        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text).strip().lower()
        return text

    def _extract_display_name(self, sender_str: str) -> str:
        """Extract display name from sender string"""
        match = re.match(r'"?([^"<]+)"?\s*<[^>]+>', sender_str)
        if match:
            return match.group(1).strip()
        return ""

    def _analyze_unsubscribe_presence(
        self, email: Dict, headers: Dict, body: str, html_body: str
    ) -> float:
        """Comprehensive unsubscribe detection with multiple signals"""
        score = 0.0
        signals_found = []

        # 1. Check standard unsubscribe headers
        for header in self.unsubscribe_headers:
            header_value = headers.get(header, "")
            if header_value:
                signals_found.append(f"header:{header}")
                score += 0.8

        # 2. Check for physical address (CAN-SPAM requirement for newsletters)
        if re.search(
            r"\b\d{1,5}\s+\w+(?:\s+\w+){1,3},\s*(?:[A-Z]{2}|[a-z]{2,})\s+\d{5}", body
        ):
            signals_found.append("physical_address")
            score += 0.3

        # 3. Check for unsubscribe links in HTML
        if html_body:
            # Parse HTML
            soup = None
            try:
                soup = BeautifulSoup(html_body, "html.parser")
            except Exception as e:
                self.logger.debug(f"HTML parsing error: {e}")

            if soup:
                # Look for links with unsubscribe text
                unsubscribe_links = soup.find_all(
                    "a", string=re.compile(r"(?i)unsubscribe|opt[-\s]?out|remove me")
                )
                if unsubscribe_links:
                    signals_found.append(f"html_links:{len(unsubscribe_links)}")
                    score += min(0.3 * len(unsubscribe_links), 0.9)

                # Look for footer with unsubscribe
                footer = soup.find("footer") or soup.find(
                    "div", class_=re.compile("(?:footer|foot)")
                )
                if footer and re.search(r"(?i)unsubscribe|opt[-\s]?out", str(footer)):
                    signals_found.append("footer_unsubscribe")
                    score += 0.5

        # 4. Check for unsubscribe text in body
        for pattern in self.unsubscribe_patterns:
            if re.search(pattern, body, re.IGNORECASE | re.MULTILINE):
                signals_found.append("body_pattern")
                score += 0.7
                break

        # 5. Check for List-Unsubscribe header (RFC 8058)
        list_unsubscribe = headers.get("List-Unsubscribe", "")
        if list_unsubscribe:
            signals_found.append("list_unsubscribe_header")
            # If it contains a mailto link, higher confidence
            if "mailto:" in list_unsubscribe:
                score += 1.0
            else:
                score += 0.8

        # 6. Check for preference center links
        if re.search(
            r"preference[s\-]?center|manage\s+preferences|email\s+preferences",
            body,
            re.IGNORECASE,
        ):
            signals_found.append("preference_center")
            score += 0.6

        self.logger.debug(f"Unsubscribe signals: {signals_found}, Score: {score:.2f}")
        return min(score, 2.0)  # Cap at 2.0

    def _analyze_sender_comprehensive(
        self, sender_str: str, email_address: str, display_name: str, domain: str
    ) -> float:
        """Comprehensive sender analysis with multiple signals"""
        score = 0.0
        signals = []

        # 1. Check against known newsletter platforms
        for platform in self.newsletter_platforms:
            if platform in domain:
                signals.append(f"platform:{platform}")
                score += 1.2
                break

        # 2. Check sender patterns
        for pattern in self.newsletter_sender_patterns:
            if re.search(pattern, sender_str, re.IGNORECASE):
                signals.append(f"pattern:{pattern}")
                score += 0.8
                break

        # 3. Analyze display name for newsletter signals
        newsletter_name_indicators = [
            "newsletter",
            "digest",
            "update",
            "bulletin",
            "brief",
            "report",
        ]
        for indicator in newsletter_name_indicators:
            if indicator in display_name.lower():
                signals.append(f"display_name:{indicator}")
                score += 0.6
                break

        # 4. Check for personal names (less likely to be newsletters)
        personal_name_pattern = r"^[A-Z][a-z]+ (?:[A-Z][a-z]+\.? )?[A-Z][a-z]+$"
        if re.match(personal_name_pattern, display_name):
            signals.append("personal_name")
            score -= 0.5

        # 5. Check for no-reply patterns (more likely transactional)
        if re.search(r"no[-\s]?reply|noreply|donotreply", email_address.lower()):
            signals.append("no_reply")
            score -= 0.7

        # 6. Check domain reputation
        domain_score = self._get_domain_reputation(domain)
        if domain_score > 0.5:
            signals.append(f"domain_reputation:{domain_score:.1f}")
            score += domain_score

        self.logger.debug(f"Sender signals: {signals}, Score: {score:.2f}")
        return min(max(score, 0.0), 2.0)

    def _get_domain_reputation(self, domain: str) -> float:
        """Get reputation score for domain"""
        if not domain:
            return 0.0

        # Exact match
        if domain in self.domain_reputation:
            return self.domain_reputation[domain]

        # Check subdomains
        parts = domain.split(".")
        for i in range(len(parts) - 1):
            subdomain = ".".join(parts[i:])
            if subdomain in self.domain_reputation:
                return self.domain_reputation[subdomain]

        # Default scores based on TLD
        if domain.endswith(("edu", "gov", "org")):
            return 0.4
        elif domain.endswith(("com", "io", "co")):
            return 0.3
        elif domain.endswith(("info", "xyz", "top")):
            return 0.1

        return 0.2

    def _analyze_content_comprehensive(
        self, subject: str, body: str, html_body: str
    ) -> float:
        """Analyze content for newsletter characteristics"""
        score = 0.0
        signals = []

        # 1. Subject line analysis
        for indicator, weight in self.newsletter_indicators:
            if indicator in subject:
                signals.append(f"subject:{indicator}")
                score += weight * 1.2  # Higher weight for subject matches

        # 2. Body content analysis
        for indicator, weight in self.newsletter_indicators:
            if indicator in body:
                signals.append(f"body:{indicator}")
                score += weight

        # 3. Content length analysis (newsletters are typically longer)
        word_count = len(body.split())
        if 300 <= word_count <= 2000:
            signals.append(f"length:{word_count}")
            score += 0.4
        elif word_count > 2000:
            signals.append(f"length:{word_count}")
            score += 0.6

        # 4. False positive detection - reduce score for likely transactional content
        for pattern in self.false_positive_patterns:
            if re.search(pattern, body):
                signals.append(f"false_positive:{pattern}")
                score += 0.5  # This is actually a positive signal for newsletters

        # 5. Check for date patterns (newsletters often reference dates)
        date_patterns = [
            r"\b(?:today|yesterday|tomorrow)\b",
            r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
            r"\b\d{1,2}\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\b",
        ]
        for pattern in date_patterns:
            if re.search(pattern, body, re.IGNORECASE):
                signals.append("date_pattern")
                score += 0.2
                break

        self.logger.debug(f"Content signals: {signals}, Score: {score:.2f}")
        return min(max(score, 0.0), 2.0)

    def _analyze_email_structure(self, html_body: str) -> float:
        """Analyze HTML structure for newsletter patterns"""
        if not html_body:
            return 0.0

        score = 0.0
        signals = []

        try:
            soup = BeautifulSoup(html_body, "html.parser")

            # 1. Check for common newsletter structures
            # Look for table-based layouts (common in newsletters)
            tables = soup.find_all("table")
            if len(tables) >= 3:
                signals.append(f"tables:{len(tables)}")
                score += min(0.1 * len(tables), 0.5)

            # 2. Look for sections
            sections = soup.find_all(
                ["section", "div"],
                class_=re.compile(r"section|article|story|feature|highlight", re.I),
            )
            if len(sections) >= 2:
                signals.append(f"sections:{len(sections)}")
                score += min(0.2 * len(sections), 0.8)

            # 3. Look for header with newsletter name
            header = soup.find(["h1", "h2"])
            if header and re.search(
                r"newsletter|digest|update|edition|issue", header.text, re.I
            ):
                signals.append("header_newsletter")
                score += 0.6

            # 4. Look for "Read More" links
            read_more_links = soup.find_all(
                "a", string=re.compile(r"read\s+(?:more|full|article|story)", re.I)
            )
            if read_more_links:
                signals.append(f"read_more:{len(read_more_links)}")
                score += min(0.2 * len(read_more_links), 0.6)

            # 5. Look for footer with social links (common in newsletters)
            footer = soup.find("footer") or soup.find(
                "div", class_=re.compile("footer|foot", re.I)
            )
            if footer:
                social_links = footer.find_all(
                    "a",
                    href=re.compile(
                        r"facebook|twitter|linkedin|instagram|youtube", re.I
                    ),
                )
                if social_links:
                    signals.append(f"social_links:{len(social_links)}")
                    score += min(0.1 * len(social_links), 0.4)

            # 6. Check for images (newsletters often have images)
            images = soup.find_all("img")
            if 2 <= len(images) <= 10:
                signals.append(f"images:{len(images)}")
                score += 0.3
            elif len(images) > 10:
                signals.append(f"images:{len(images)}")
                score += 0.5

        except Exception as e:
            self.logger.debug(f"HTML structure analysis error: {e}")

        self.logger.debug(f"Structure signals: {signals}, Score: {score:.2f}")
        return min(max(score, 0.0), 2.0)

    def _analyze_transactional_patterns(self, subject: str, body: str) -> float:
        """Analyze for transactional patterns with nuanced scoring"""
        score = 0.0
        signals = []

        # Check each category with appropriate weights
        for category, patterns in self.transactional_patterns.items():
            category_score = 0.0
            category_signals = []

            for pattern in patterns:
                # Check subject first (more important)
                if re.search(pattern, subject, re.IGNORECASE):
                    category_score += 1.5
                    category_signals.append(f"subject:{pattern}")

                # Check body
                if re.search(pattern, body, re.IGNORECASE):
                    category_score += 1.0
                    category_signals.append(f"body:{pattern}")

            # Apply category-specific weights
            if category == "security" and category_score > 0:
                category_score *= 0.7  # Less aggressive for security emails
            elif category == "transaction" and category_score > 0:
                category_score *= 1.2  # More aggressive for transactional content
            elif category == "system" and category_score > 0:
                category_score *= 0.8  # Moderate for system emails

            if category_score > 0:
                score += category_score
                signals.extend(category_signals)

        # Check for one-time codes (strong transactional signal)
        if re.search(r"\b\d{4,6}\b", subject) or re.search(r"\b\d{4,6}\b", body):
            signals.append("one_time_code")
            score += 1.0

        # Check for order numbers
        if re.search(
            r"(?:order|invoice|receipt)[\s#:]*[A-Z0-9]{6,12}", body, re.IGNORECASE
        ):
            signals.append("order_number")
            score += 1.2

        self.logger.debug(f"Transactional signals: {signals}, Score: {score:.2f}")
        return min(score, 3.0)  # Cap at 3.0

    def _analyze_engagement_signals(self, body: str, html_body: str) -> float:
        """Analyze for engagement signals common in newsletters"""
        score = 0.0
        signals = []

        # 1. Call-to-action buttons
        cta_patterns = [
            r">\s*read\s+(?:more|now|full article)\s*<",
            r">\s*learn\s+more\s*<",
            r">\s*subscribe\s*<",
            r">\s*sign\s+up\s*<",
            r">\s*watch\s+now\s*<",
            r">\s*listen\s+now\s*<",
        ]

        for pattern in cta_patterns:
            if re.search(pattern, html_body, re.IGNORECASE):
                signals.append("cta_button")
                score += 0.3
                break

        # 2. Social sharing links
        social_patterns = [
            r"facebook\.com/share",
            r"twitter\.com/intent",
            r"linkedin\.com/share",
            r"pinterest\.com/pin",
            r"whatsapp\.com",
        ]

        for pattern in social_patterns:
            if re.search(pattern, html_body, re.IGNORECASE):
                signals.append("social_sharing")
                score += 0.2
                break

        # 3. Forward-to-a-friend links
        if re.search(
            r"forward\s+to\s+a\s+friend|share\s+this\s+newsletter", body, re.IGNORECASE
        ):
            signals.append("forward_to_friend")
            score += 0.4

        # 4. Newsletter archive links
        if re.search(
            r"view\s+in\s+browser|view\s+online|newsletter\s+archive",
            body,
            re.IGNORECASE,
        ):
            signals.append("view_in_browser")
            score += 0.5

        self.logger.debug(f"Engagement signals: {signals}, Score: {score:.2f}")
        return min(max(score, 0.0), 1.0)

    def _get_adaptive_threshold(self, analysis: Dict) -> float:
        """Get adaptive threshold based on email characteristics"""
        base_threshold = 1.0

        # Adjust threshold based on signals
        if analysis["unsubscribe_score"] >= 1.5:
            # Strong unsubscribe signal - lower threshold
            return max(base_threshold - 0.5, 0.3)

        if analysis["transactional_score"] >= 2.0:
            # Strong transactional signal - raise threshold
            return base_threshold + 0.7

        if analysis["sender_score"] >= 1.0 and analysis["content_score"] >= 1.0:
            # Strong positive signals - lower threshold
            return max(base_threshold - 0.3, 0.5)

        # Default threshold
        return base_threshold

    def _determine_primary_pattern(
        self, analysis: Dict, total_score: float, threshold: float
    ) -> str:
        """Determine the primary pattern that led to the decision"""
        if total_score >= threshold:
            # It's a newsletter - determine strongest signal
            max_signal = max(
                analysis.items(),
                key=lambda x: abs(x[1]) if isinstance(x[1], (int, float)) else 0,
            )
            return f"newsletter_{max_signal[0]}"
        else:
            # It's transactional - determine strongest signal
            if analysis["transactional_score"] > 1.0:
                return "transactional_high"
            elif analysis["unsubscribe_score"] < 0.5:
                return "no_unsubscribe"
            else:
                return "mixed_signals"

    def _log_detailed_decision(
        self,
        email_id: str,
        subject: str,
        sender: str,
        analysis: Dict,
        total_score: float,
        threshold: float,
        primary_pattern: str,
    ):
        """Log detailed decision for debugging"""
        self.logger.debug(f"\n{'=' * 60}")
        self.logger.debug(f"📧 EMAIL DECISION: {email_id}")
        self.logger.debug(
            f"   Subject: '{subject[:60]}{'...' if len(subject) > 60 else ''}'"
        )
        self.logger.debug(
            f"   Sender: '{sender[:60]}{'...' if len(sender) > 60 else ''}'"
        )
        self.logger.debug(f"   Primary Pattern: {primary_pattern}")
        self.logger.debug(f"\n📊 SCORE BREAKDOWN:")
        for key, value in analysis.items():
            if isinstance(value, (int, float)):
                self.logger.debug(f"   • {key.replace('_', ' ').title()}: {value:.2f}")
        self.logger.debug(f"\n🎯 DECISION:")
        self.logger.debug(f"   Total Score: {total_score:.2f}")
        self.logger.debug(f"   Threshold: {threshold:.2f}")
        self.logger.debug(
            f"   {'✅ KEEP AS NEWSLETTER' if total_score >= threshold else '❌ FILTER OUT'}"
        )
        self.logger.debug(f"{'=' * 60}\n")

    def _log_summary(self, decisions: List[Dict]):
        """Log summary of filtering results"""
        total = len(decisions)
        kept = sum(1 for d in decisions if d["should_keep"])
        filtered = total - kept

        self.logger.info(f"\n{'=' * 60}")
        self.logger.info(f"📊 FILTERING SUMMARY")
        self.logger.info(f"{'=' * 60}")
        self.logger.info(f"   Total emails processed: {total}")
        self.logger.info(f"   Newsletters detected: {kept} ({kept / total * 100:.1f}%)")
        self.logger.info(
            f"   Transactional emails filtered: {filtered} ({filtered / total * 100:.1f}%)"
        )

        # Pattern distribution
        pattern_counts = {}
        for decision in decisions:
            pattern = decision["primary_pattern"]
            pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1

        self.logger.info(f"\n🔍 PRIMARY DETECTION PATTERNS:")
        for pattern, count in sorted(
            pattern_counts.items(), key=lambda x: x[1], reverse=True
        ):
            self.logger.info(
                f"   • {pattern}: {count} emails ({count / total * 100:.1f}%)"
            )

        self.logger.info(f"{'=' * 60}")

    def get_stats(self) -> Dict:
        """Get filtering statistics"""
        return self.stats

    def export_decision_log(self, decisions: List[Dict], filename: str = None) -> str:
        """Export decision log to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"decision_log_{timestamp}.json"

        log_data = {
            "generated_at": datetime.now().isoformat(),
            "total_processed": len(decisions),
            "newsletters_detected": sum(1 for d in decisions if d["should_keep"]),
            "decisions": [
                {
                    "email_id": d["email_id"],
                    "subject": d["subject"],
                    "sender": d["sender"],
                    "total_score": d["total_score"],
                    "threshold": d["threshold"],
                    "should_keep": d["should_keep"],
                    "primary_pattern": d["primary_pattern"],
                    "analysis": {
                        k: v
                        for k, v in d["analysis"].items()
                        if isinstance(v, (int, float))
                    },
                }
                for d in decisions
            ],
            "stats": self.stats,
        }

        try:
            with open(filename, "w") as f:
                json.dump(log_data, f, indent=2)
            self.logger.info(f"✅ Decision log exported to: {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"❌ Error exporting decision log: {e}")
            return None

    def update_domain_reputation(self, domain_scores: Dict[str, float]):
        """Update domain reputation scores based on feedback"""
        for domain, score in domain_scores.items():
            self.domain_reputation[domain.lower()] = max(0.0, min(1.0, score))
        self.logger.info(
            f"✅ Updated domain reputation scores for {len(domain_scores)} domains"
        )
