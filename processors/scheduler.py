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
        """Create proper digest data for Notion publishing by reading JSON metadata"""
        try:
            # Check if JSON metadata file exists
            json_path = digest_path.replace(".md", ".json")

            if os.path.exists(json_path):
                # Load structured metadata from JSON file
                print(f"📄 Loading digest metadata from {json_path}")
                with open(json_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)

                # Read the markdown content for unified summary
                with open(digest_path, "r", encoding="utf-8") as f:
                    digest_content = f.read()

                # Extract a unified summary from the digest content
                # Look for the main introduction paragraph after the title

                # Try to extract the first substantial paragraph as unified summary
                paragraphs = re.split(r"\n\n+", digest_content)
                unified_summary = ""

                for para in paragraphs:
                    # Skip metadata header, titles, and short lines
                    if (
                        para.strip()
                        and not para.startswith("---")
                        and not para.startswith("#")
                        and len(para.strip()) > 100
                    ):
                        unified_summary = para.strip()
                        break

                if not unified_summary:
                    unified_summary = "Weekly newsletter digest with key insights across multiple domains."

                # Create genre_summaries from metadata and markdown content
                genre_summaries = {}
                for genre, genre_data in metadata.get("genres", {}).items():
                    # Try to extract genre-specific summary from markdown
                    genre_pattern = (
                        rf"###\s+{re.escape(genre)}[:\s]+(.*?)(?=\n###|\n---|\Z)"
                    )
                    genre_match = re.search(
                        genre_pattern, digest_content, re.DOTALL | re.IGNORECASE
                    )

                    genre_summary = "Key insights from this week's newsletters."
                    if genre_match:
                        # Get first paragraph of the genre section
                        genre_text = genre_match.group(1).strip()
                        genre_paragraphs = re.split(r"\n\n+", genre_text)
                        for gpara in genre_paragraphs:
                            if (
                                gpara.strip()
                                and not gpara.startswith("#")
                                and len(gpara.strip()) > 50
                            ):
                                genre_summary = gpara.strip()
                                break

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
            print(f"❌ Error creating digest data: {e}")
            import traceback

            traceback.print_exc()
            # Fallback to minimal working data
            return self._get_fallback_digest_data()

    def _parse_markdown_digest(self, digest_path):
        """Legacy method to parse markdown digest file"""
        try:
            with open(digest_path, "r", encoding="utf-8") as f:
                digest_content = f.read()

            # Get date range (last 7 days)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)

            # Try to extract week range from metadata or content
            week_start = start_date
            week_end = end_date

            # Extract genres from headings (### Technology, etc.)
            genre_pattern = r"###\s+(.+?)[:\.]\s+"
            genre_matches = re.findall(genre_pattern, digest_content)

            genre_summaries = {}
            total_newsletters = 0

            for genre in set(genre_matches):  # Remove duplicates
                genre = genre.strip()
                # Try to extract content for this genre
                genre_section_pattern = (
                    rf"###\s+{re.escape(genre)}[:\s]+(.*?)(?=\n###|\n---|\Z)"
                )
                section_match = re.search(
                    genre_section_pattern, digest_content, re.DOTALL | re.IGNORECASE
                )

                if section_match:
                    section_text = section_match.group(1).strip()
                    # Take first paragraph as summary
                    paragraphs = re.split(r"\n\n+", section_text)
                    summary = ""
                    for para in paragraphs:
                        if para.strip() and len(para.strip()) > 50:
                            summary = para.strip()
                            break

                    if summary:
                        genre_summaries[genre] = {
                            "summary": summary[:2000],
                            "newsletters": [],
                        }
                        total_newsletters += 1

            # Extract unified summary
            paragraphs = re.split(r"\n\n+", digest_content)
            unified_summary = ""
            for para in paragraphs:
                if (
                    para.strip()
                    and not para.startswith("---")
                    and not para.startswith("#")
                    and len(para.strip()) > 100
                ):
                    unified_summary = para.strip()
                    break

            if not unified_summary:
                unified_summary = "Weekly newsletter digest with key insights across multiple domains."

            # If no data extracted, use fallback
            if not genre_summaries:
                return self._get_fallback_digest_data()

            return {
                "week_start": week_start.strftime("%Y-%m-%d"),
                "week_end": week_end.strftime("%Y-%m-%d"),
                "total_newsletters": max(total_newsletters, len(genre_summaries)),
                "genre_summaries": genre_summaries,
                "unified_summary": unified_summary[:2000],
            }

        except Exception as e:
            print(f"❌ Error parsing markdown digest: {e}")
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
