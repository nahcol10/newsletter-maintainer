from datetime import datetime, timedelta
import os
import json
import re
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from processors.daily_newsletter_processor import DailyNewsletterProcessor
from processors.weekly_digest_generator import WeeklyDigestGenerator
from processors.notion_publisher import NotionPublisher


class NewsletterScheduler:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.setup_jobs()

    def setup_jobs(self):
        """Setup scheduled jobs"""
        # Daily processing at 8 PM
        self.scheduler.add_job(
            self.run_daily_job,
            CronTrigger(hour=20, minute=0),  # 8:00 PM
            id="daily_processing",
            name="Daily Newsletter Processing",
            replace_existing=True,
        )

        # Weekly digest on Sunday at 7 AM
        self.scheduler.add_job(
            self.run_weekly_job,
            CronTrigger(day_of_week="sun", hour=7, minute=0),  # Sunday 7:00 AM
            id="weekly_digest",
            name="Weekly Digest Generation",
            replace_existing=True,
        )

        print("✅ Scheduled jobs configured:")
        print("- Daily processing: Every day at 8:00 PM")
        print("- Weekly digest: Every Sunday at 7:00 AM")

    def start(self):
        """Start the scheduler"""
        if not self.scheduler.running:
            self.scheduler.start()
            print("🚀 Scheduler started")

    def shutdown(self):
        """Shutdown the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            print("🛑 Scheduler stopped")

    def get_status(self):
        """Get scheduler status"""
        jobs = self.scheduler.get_jobs()
        return {
            "running": self.scheduler.running,
            "jobs": [
                {"id": job.id, "name": job.name, "next_run": str(job.next_run_time)}
                for job in jobs
            ],
        }

    def run_daily_job(self):
        """Run daily newsletter processing"""
        print("🚀 Starting Daily Newsletter Processing...")

        try:
            processor = DailyNewsletterProcessor()
            results = processor.run_daily_processing()

            if results.get("success"):
                processed_count = results.get("newsletters_processed", 0)
                duration = results.get("processing_time", 0)
                print(
                    f"✅ Daily processing completed: {processed_count} newsletters in {duration:.2f}s"
                )
            else:
                error_msg = results.get("error", "Unknown error")
                print(f"❌ Daily processing failed: {error_msg}")

        except Exception as e:
            print(f"❌ Daily job error: {e}")

    def run_weekly_job(self):
        """Run weekly digest generation"""
        print("🚀 Starting Weekly Digest Generation...")

        try:
            generator = WeeklyDigestGenerator()
            digest_path = generator.generate_weekly_digest()

            if digest_path:
                print("✅ Weekly digest generated")

                # Try to publish to Notion
                try:
                    notion_publisher = NotionPublisher()
                    digest_data = self._create_digest_data_for_notion(digest_path)

                    if digest_data:
                        page_id = notion_publisher.publish_weekly_digest(digest_data)
                        if page_id:
                            print(f"✅ Published to Notion: {page_id}")
                        else:
                            print("⚠️ Notion publishing failed")

                except Exception as e:
                    print(f"❌ Notion error: {e}")
            else:
                print("❌ Weekly digest generation failed")

        except Exception as e:
            print(f"❌ Weekly job error: {e}")

    def _create_digest_data_for_notion(self, digest_path):
        """Create proper digest data for Notion publishing by reading JSON metadata and markdown content"""
        try:
            # Check if JSON metadata file exists
            json_path = digest_path.replace(".md", ".json")
            if os.path.exists(json_path):
                # Load structured metadata from JSON file
                print(f"📄 Loading digest metadata from {json_path}")
                with open(json_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)

                # Read the markdown content
                with open(digest_path, "r", encoding="utf-8") as f:
                    digest_content = f.read()

                # Extract unified summary - look for content between title and first genre section
                unified_summary = ""
                # Split content into sections
                sections = re.split(r"\n##\s+", digest_content, maxsplit=2)
                if len(sections) > 1:
                    intro_section = sections[1]  # Content after the main title
                    # Remove any headers or metadata
                    intro_section = re.sub(
                        r"^#.*$", "", intro_section, flags=re.MULTILINE
                    )
                    intro_section = re.sub(
                        r"^---.*?---\s*", "", intro_section, flags=re.DOTALL
                    )
                    # Get paragraphs
                    paragraphs = [
                        p.strip()
                        for p in intro_section.split("\n\n")
                        if p.strip() and len(p.strip()) > 100
                    ]
                    if paragraphs:
                        unified_summary = paragraphs[0]

                if not unified_summary:
                    # Fallback: take first substantial paragraph from entire content
                    paragraphs = [
                        p.strip()
                        for p in digest_content.split("\n\n")
                        if p.strip() and len(p.strip()) > 150
                    ]
                    unified_summary = (
                        paragraphs[0]
                        if paragraphs
                        else "Weekly newsletter digest with key insights across multiple domains."
                    )

                # Create genre_summaries from metadata and markdown content
                genre_summaries = {}
                for genre, genre_data in metadata.get("genres", {}).items():
                    # More flexible pattern to match genre sections
                    genre_pattern = (
                        rf"###\s+{re.escape(genre)}\b.*?\n+(.*?)(?=\n###\s+|\Z)"
                    )
                    genre_match = re.search(
                        genre_pattern, digest_content, re.DOTALL | re.IGNORECASE
                    )

                    genre_summary = "Key insights from this week's newsletters."
                    if genre_match:
                        # Get the full content of the genre section
                        genre_text = genre_match.group(1).strip()
                        # Remove any sub-headers, images, or markdown formatting
                        genre_text = re.sub(
                            r"#+\s+.*$", "", genre_text, flags=re.MULTILINE
                        )
                        genre_text = re.sub(
                            r"!\[.*?\]\(.*?\)", "", genre_text
                        )  # Remove images
                        genre_text = re.sub(
                            r"\[.*?\]\(.*?\)", "", genre_text
                        )  # Remove links
                        genre_text = re.sub(
                            r"\*\*|\*|_|`", "", genre_text
                        )  # Remove formatting

                        # Get substantial paragraphs
                        paragraphs = [
                            p.strip()
                            for p in genre_text.split("\n\n")
                            if p.strip() and len(p.strip()) > 80
                        ]
                        if paragraphs:
                            # Combine first few substantial paragraphs for a comprehensive summary
                            genre_summary = " ".join(paragraphs[:2])

                    genre_summaries[genre] = {
                        "summary": genre_summary[
                            :2000
                        ],  # Limit to 2000 chars for Notion
                        "newsletters": genre_data.get("newsletters", []),
                    }

                return {
                    "week_start": metadata.get("week_start"),
                    "week_end": metadata.get("week_end"),
                    "total_newsletters": metadata.get("total_newsletters", 0),
                    "genre_summaries": genre_summaries,
                    "unified_summary": unified_summary[
                        :2000
                    ],  # Limit to 2000 chars for Notion
                }
            else:
                # Fallback: Try to parse markdown (legacy support)
                print(f"⚠️ No JSON metadata found, attempting to parse markdown file")
                return self._parse_markdown_digest(digest_path)
        except Exception as e:
            print(f"❌ Error creating digest data for Notion: {e}")
            import traceback

            traceback.print_exc()
            # Fallback to minimal working data
            return self._get_fallback_digest_data()

    def _parse_markdown_digest(self, digest_path):
        """Improved method to parse markdown digest file when JSON metadata is not available"""
        try:
            with open(digest_path, "r", encoding="utf-8") as f:
                digest_content = f.read()

            # Get date range (last 7 days)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)

            # Extract genres from headings
            genre_matches = re.findall(
                r"###\s+(.*?)(?:\n|$)", digest_content, re.IGNORECASE
            )
            if not genre_matches:
                genre_matches = re.findall(
                    r"##\s+(.*?)(?:\n|$)", digest_content, re.IGNORECASE
                )

            genre_summaries = {}
            total_newsletters = 0

            for genre in set(genre_matches):  # Remove duplicates
                genre = genre.strip()
                if (
                    not genre
                    or len(genre) < 2
                    or "date range" in genre.lower()
                    or "conclusion" in genre.lower().lower()
                ):
                    continue

                # More flexible pattern to extract genre content
                genre_pattern = (
                    rf"###\s+{re.escape(genre)}\b.*?\n+(.*?)(?=\n###\s+|\n##\s+|\Z)"
                )
                genre_match = re.search(
                    genre_pattern, digest_content, re.DOTALL | re.IGNORECASE
                )

                if genre_match:
                    genre_text = genre_match.group(1).strip()
                    # Clean the text
                    genre_text = re.sub(r"#+\s+.*$", "", genre_text, flags=re.MULTILINE)
                    genre_text = re.sub(r"\*\*|\*|_|`", "", genre_text)
                    paragraphs = [
                        p.strip()
                        for p in genre_text.split("\n\n")
                        if p.strip() and len(p.strip()) > 80
                    ]

                    if paragraphs:
                        summary = " ".join(
                            paragraphs[:2]
                        )  # Take first two substantial paragraphs
                        genre_summaries[genre] = {
                            "summary": summary[:2000],
                            "newsletters": [],
                        }
                        total_newsletters += 1

            # Extract unified summary - look for introduction paragraph
            unified_summary = ""
            sections = re.split(r"\n##\s+", digest_content, maxsplit=2)
            if len(sections) > 1:
                intro_section = sections[1]
                intro_section = re.sub(r"^#.*$", "", intro_section, flags=re.MULTILINE)
                intro_section = re.sub(
                    r"^---.*?---\s*", "", intro_section, flags=re.DOTALL
                )
                paragraphs = [
                    p.strip()
                    for p in intro_section.split("\n\n")
                    if p.strip() and len(p.strip()) > 150
                ]
                if paragraphs:
                    unified_summary = paragraphs[0]

            if not unified_summary:
                paragraphs = [
                    p.strip()
                    for p in digest_content.split("\n\n")
                    if p.strip() and len(p.strip()) > 200
                ]
                unified_summary = (
                    paragraphs[0]
                    if paragraphs
                    else "Weekly newsletter digest with key insights across multiple domains."
                )

            # If no data extracted, use fallback
            if not genre_summaries:
                return self._get_fallback_digest_data()

            return {
                "week_start": start_date.strftime("%Y-%m-%d"),
                "week_end": end_date.strftime("%Y-%m-%d"),
                "total_newsletters": max(total_newsletters, len(genre_summaries)),
                "genre_summaries": genre_summaries,
                "unified_summary": unified_summary[:2000],
            }
        except Exception as e:
            print(f"❌ Error parsing markdown digest for Notion: {e}")
            import traceback

            traceback.print_exc()
            return self._get_fallback_digest_data()

    def _get_fallback_digest_data(self):
        """Get fallback digest data when parsing fails"""

        return {
            "week_start": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
            "week_end": datetime.now().strftime("%Y-%m-%d"),
            "total_newsletters": 5,
            "genre_summaries": {
                "Technology": {
                    "summary": "Technology insights from this week.",
                    "newsletters": [],
                }
            },
            "unified_summary": "Weekly newsletter digest with key insights across multiple domains.",
        }
