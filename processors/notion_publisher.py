#!/usr/bin/env python3
import os
import json
import argparse
import logging
from datetime import datetime, timedelta
from notion_client import Client
from notion_client.errors import APIResponseError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("notion_publisher.log"), logging.StreamHandler()],
)
logger = logging.getLogger("NotionPublisher")


class NotionPublisher:
    """Publisher for weekly digests to Notion with enhanced validation"""

    def __init__(self):
        """Initialize Notion client and load environment variables"""
        load_dotenv()
        self.notion_token = os.getenv("NOTION_TOKEN")
        self.database_id = os.getenv("NOTION_DATABASE_ID")

        if not self.notion_token:
            raise ValueError("NOTION_TOKEN environment variable is required")
        if not self.database_id:
            raise ValueError("NOTION_DATABASE_ID environment variable is required")

        self.client = Client(auth=self.notion_token)
        logger.info("✅ Notion client initialized")

    def publish_weekly_digest(self, digest_data: dict) -> str:
        """
        Main method called by scheduler to publish weekly digest.
        This wraps the publish_to_notion method for compatibility.
        """
        return self.publish_to_notion(digest_data)

    def find_latest_digest(self) -> tuple:
        """Find the latest digest files in the data/digests directory"""
        digest_dir = os.path.join("data", "digests")

        if not os.path.exists(digest_dir):
            raise FileNotFoundError(f"Digest directory not found: {digest_dir}")

        # Find all .md files
        md_files = [f for f in os.listdir(digest_dir) if f.endswith(".md")]

        if not md_files:
            raise FileNotFoundError(f"No digest files found in {digest_dir}")

        # Sort by filename (which includes timestamp)
        md_files.sort(reverse=True)
        latest_md = md_files[0]
        latest_json = latest_md.replace(".md", ".json")

        md_path = os.path.join(digest_dir, latest_md)
        json_path = os.path.join(digest_dir, latest_json)

        if not os.path.exists(json_path):
            raise FileNotFoundError(
                f"Corresponding JSON metadata file not found: {json_path}"
            )

        logger.info(f"Found latest digest files:")
        logger.info(f"  MD: {md_path}")
        logger.info(f"  JSON: {json_path}")

        return md_path, json_path

    def load_digest_data(self, md_path: str, json_path: str) -> dict:
        """Load and parse digest data from both MD and JSON files"""
        # Load JSON metadata
        with open(json_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        # Read the MD content
        with open(md_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Extract unified summary from the main content
        # Find the first substantial paragraph after the frontmatter
        paragraphs = [p.strip() for p in content.split("\n\n") if p.strip()]
        unified_summary = ""
        for para in paragraphs:
            if not para.startswith("---") and not para.startswith("#"):
                if len(para) > 100:
                    unified_summary = para
                    break

        if not unified_summary:
            unified_summary = (
                "Weekly newsletter digest with key insights across multiple domains."
            )

        # Create genre_summaries from metadata
        genre_summaries = {}
        for genre, genre_data in metadata.get("genres", {}).items():
            genre_summaries[genre] = {
                "summary": genre_data.get("summary", ""),
                "newsletters": genre_data.get("newsletters", []),
            }

        # Build the digest data structure
        digest_data = {
            "week_start": metadata.get(
                "week_start", (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            ),
            "week_end": metadata.get("week_end", datetime.now().strftime("%Y-%m-%d")),
            "total_newsletters": metadata.get("total_newsletters", 0),
            "genre_summaries": genre_summaries,
            "unified_summary": unified_summary,
        }

        return digest_data

    def create_notion_blocks(self, digest_data: dict) -> list:
        """Create validated Notion blocks from digest data with enhanced error handling"""
        blocks = []

        # 1. Add header
        blocks.append(
            {
                "object": "block",
                "type": "heading_1",
                "heading_1": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": "Weekly Newsletter Digest"},
                        }
                    ]
                },
            }
        )

        # 2. Add week info
        blocks.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": f"📅 Week: {digest_data['week_start']} to {digest_data['week_end']}"
                            },
                        }
                    ]
                },
            }
        )

        # 3. Add newsletter count
        blocks.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": f"📊 Total Newsletters: {digest_data['total_newsletters']}"
                            },
                        }
                    ]
                },
            }
        )

        # 4. Add divider
        blocks.append({"object": "block", "type": "divider", "divider": {}})

        # 5. Add unified summary
        if digest_data.get("unified_summary"):
            blocks.append(
                {
                    "object": "block",
                    "type": "heading_2",
                    "heading_2": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": "🌟 Weekly Highlights"},
                            }
                        ]
                    },
                }
            )

            # Split long summaries into multiple paragraphs
            paragraphs = self._split_long_text(digest_data["unified_summary"])
            for para in paragraphs:
                blocks.append(
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{"type": "text", "text": {"content": para}}]
                        },
                    }
                )

            blocks.append({"object": "block", "type": "divider", "divider": {}})

        # 6. Add genre sections
        if digest_data.get("genre_summaries"):
            blocks.append(
                {
                    "object": "block",
                    "type": "heading_2",
                    "heading_2": {
                        "rich_text": [
                            {"type": "text", "text": {"content": "📑 By Genre"}}
                        ]
                    },
                }
            )

            genre_list = list(digest_data["genre_summaries"].keys())

            for i, genre in enumerate(genre_list):
                genre_data = digest_data["genre_summaries"][genre]

                # Genre header
                blocks.append(
                    {
                        "object": "block",
                        "type": "heading_3",
                        "heading_3": {
                            "rich_text": [
                                {
                                    "type": "text",
                                    "text": {
                                        "content": f"{self._get_genre_emoji(genre)} {genre}"
                                    },
                                }
                            ]
                        },
                    }
                )

                # Newsletter count for this genre
                newsletter_count = len(genre_data.get("newsletters", []))
                blocks.append(
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [
                                {
                                    "type": "text",
                                    "text": {
                                        "content": f"📈 {newsletter_count} newsletters processed"
                                    },
                                }
                            ]
                        },
                    }
                )

                # Genre summary
                genre_summary = genre_data.get("summary", "")
                if genre_summary:
                    paragraphs = self._split_long_text(genre_summary)
                    for para in paragraphs:
                        blocks.append(
                            {
                                "object": "block",
                                "type": "paragraph",
                                "paragraph": {
                                    "rich_text": [
                                        {"type": "text", "text": {"content": para}}
                                    ]
                                },
                            }
                        )

                # Add source newsletters
                newsletters = genre_data.get("newsletters", [])
                if newsletters:
                    blocks.append(
                        {
                            "object": "block",
                            "type": "heading_3",
                            "heading_3": {
                                "rich_text": [
                                    {
                                        "type": "text",
                                        "text": {"content": "📚 Source Newsletters"},
                                    }
                                ]
                            },
                        }
                    )

                    for newsletter in newsletters[:5]:  # Limit to 5 per genre
                        subject = newsletter.get("subject", "Unknown")
                        from_email = newsletter.get("from", "Unknown")
                        content = f"{subject} (from {from_email})"

                        blocks.append(
                            {
                                "object": "block",
                                "type": "bulleted_list_item",
                                "bulleted_list_item": {
                                    "rich_text": [
                                        {"type": "text", "text": {"content": content}}
                                    ]
                                },
                            }
                        )

                # Add divider between genres (except after the last one)
                if i < len(genre_list) - 1:
                    blocks.append({"object": "block", "type": "divider", "divider": {}})

        # 7. Add footer
        blocks.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": f"🤖 Published by Notion Digest Publisher on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            },
                        }
                    ]
                },
            }
        )

        # Validate all blocks before returning
        validated_blocks = []
        for i, block in enumerate(blocks):
            if self._validate_block(block, i):
                validated_blocks.append(block)
            else:
                logger.warning(f"⚠️ Skipping invalid block at index {i}")

        return validated_blocks

    def _split_long_text(self, text: str, max_length: int = 2000) -> list:
        """Split long text into paragraphs that respect Notion's limits"""
        if not text:
            return []

        # First try to split by natural paragraph breaks
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]

        if not paragraphs:
            paragraphs = [text]

        # Further split very long paragraphs
        result = []
        for para in paragraphs:
            if len(para) <= max_length:
                result.append(para)
            else:
                # Split by sentences
                sentences = [s.strip() for s in para.split(".") if s.strip()]
                current_chunk = ""
                for sentence in sentences:
                    if len(current_chunk) + len(sentence) + 1 <= max_length:
                        current_chunk += sentence + ". "
                    else:
                        if current_chunk:
                            result.append(current_chunk.strip())
                        current_chunk = sentence + ". "
                if current_chunk:
                    result.append(current_chunk.strip())

        return result

    def _validate_block(self, block: dict, index: int) -> bool:
        """Validate that a block has proper structure for Notion API"""
        try:
            # Check required properties
            if "object" not in block or block["object"] != "block":
                logger.error(
                    f"❌ Block {index} missing 'object' property or invalid value"
                )
                return False

            if "type" not in block:
                logger.error(f"❌ Block {index} missing 'type' property")
                return False

            block_type = block["type"]
            type_specific_key = block_type

            # Special case mappings for Notion API
            type_mappings = {
                "bulleted_list_item": "bulleted_list_item",
                "numbered_list_item": "numbered_list_item",
                "to_do": "to_do",
                "toggle": "toggle",
                "quote": "quote",
                "callout": "callout",
            }

            if block_type in type_mappings:
                type_specific_key = type_mappings[block_type]

            if type_specific_key not in block:
                logger.error(
                    f"❌ Block {index} (type: {block_type}) missing '{type_specific_key}' property"
                )
                return False

            # Validate rich_text content where applicable
            content_fields = [
                "heading_1",
                "heading_2",
                "heading_3",
                "paragraph",
                "bulleted_list_item",
                "numbered_list_item",
                "quote",
                "to_do",
                "toggle",
                "callout",
            ]

            if block_type in content_fields:
                content_field = block.get(type_specific_key, {})
                if not isinstance(content_field, dict):
                    logger.warning(
                        f"⚠️ Block {index} (type: {block_type}) has invalid content field type"
                    )
                    return False

                if "rich_text" not in content_field:
                    logger.warning(
                        f"⚠️ Block {index} (type: {block_type}) missing 'rich_text' property"
                    )
                    return False

                rich_text = content_field.get("rich_text", [])
                if not isinstance(rich_text, list):
                    logger.warning(
                        f"⚠️ Block {index} (type: {block_type}) has invalid rich_text array"
                    )
                    return False

                if len(rich_text) == 0:
                    logger.warning(
                        f"⚠️ Block {index} (type: {block_type}) has empty rich_text array"
                    )

            return True
        except Exception as e:
            logger.error(f"❌ Error validating block {index}: {e}")
            return False

    def _get_genre_emoji(self, genre: str) -> str:
        """Get appropriate emoji for genre"""
        emoji_map = {
            "Technology": "💻",
            "Business": "💼",
            "Finance": "💰",
            "Health": "🏥",
            "Education": "📚",
            "Entertainment": "🎬",
            "Sports": "⚽",
            "Politics": "🏛️",
            "Science": "🔬",
            "Travel": "✈️",
            "Food": "🍽️",
            "Fashion": "👗",
            "Gaming": "🎮",
            "Art": "🎨",
            "Music": "🎵",
            "Philosophy": "🤔",
            "Culture": "🎭",
            "Productivity": "⚡",
            "Writing & Creativity": "✍️",
            "Personal Growth": "🌱",
            "Spirituality": "✨",
            "Humor & Entertainment": "😂",
            "Lifestyle": "🏠",
        }
        return emoji_map.get(genre, "📰")

    def publish_to_notion(self, digest_data: dict) -> str:
        """Publish digest to Notion with comprehensive validation"""
        try:
            # Validate digest data
            if not self._validate_digest_data(digest_data):
                logger.error("❌ Digest data validation failed")
                return None

            # Create page properties
            properties = {
                "Name": {
                    "title": [
                        {
                            "type": "text",
                            "text": {
                                "content": f"Weekly Newsletter Digest - {digest_data['week_start']} to {digest_data['week_end']}"
                            },
                        }
                    ]
                },
                "Week": {
                    "date": {
                        "start": digest_data["week_start"],
                        "end": digest_data["week_end"],
                    }
                },
                "Newsletter Count": {"number": digest_data["total_newsletters"]},
                "Genres": {
                    "multi_select": [
                        {"name": genre}
                        for genre in digest_data.get("genre_summaries", {}).keys()
                    ]
                },
            }

            # Create page content blocks
            children = self.create_notion_blocks(digest_data)
            if not children:
                logger.error("❌ Failed to create valid blocks for Notion")
                return None

            # Debug: Print block structure before sending
            self._log_block_structure(children)

            # Create the page
            response = self.client.pages.create(
                parent={"database_id": self.database_id},
                properties=properties,
                children=children,
            )

            page_id = response.get("id")
            logger.info(f"✅ Successfully published to Notion. Page ID: {page_id}")
            return page_id

        except APIResponseError as e:
            logger.error(f"❌ Notion API error: {e}")
            if hasattr(e, "body"):
                logger.error(f"❌ API Response Body: {json.dumps(e.body, indent=2)}")
            return None
        except Exception as e:
            logger.error(f"❌ Error publishing to Notion: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return None

    def _validate_digest_data(self, digest_data: dict) -> bool:
        """Validate required digest data fields"""
        required_fields = ["week_start", "week_end", "total_newsletters"]
        for field in required_fields:
            if field not in digest_data:
                logger.error(f"❌ Missing required field: {field}")
                return False
        return True

    def _log_block_structure(self, children: list):
        """Log block structure for debugging"""
        logger.info(f"\n🔍 Notion Block Structure Debug:")
        logger.info(f"Total blocks: {len(children)}")
        for i, block in enumerate(children):
            block_type = block.get("type", "unknown")
            content_preview = ""

            # Try to get content preview for common block types
            try:
                if block_type in ["paragraph", "heading_1", "heading_2", "heading_3"]:
                    rich_text = block.get(block_type, {}).get("rich_text", [])
                    if rich_text and isinstance(rich_text, list) and len(rich_text) > 0:
                        first_rt = rich_text[0]
                        if isinstance(first_rt, dict) and "text" in first_rt:
                            text_content = first_rt["text"].get("content", "")
                            content_preview = text_content[:100]
                elif block_type == "bulleted_list_item":
                    rich_text = block.get("bulleted_list_item", {}).get("rich_text", [])
                    if rich_text and isinstance(rich_text, list) and len(rich_text) > 0:
                        first_rt = rich_text[0]
                        if isinstance(first_rt, dict) and "text" in first_rt:
                            text_content = first_rt["text"].get("content", "")
                            content_preview = text_content[:100]
            except Exception as e:
                content_preview = f"[error extracting content: {str(e)}]"

            logger.info(
                f"Block {i}: type='{block_type}'"
                + (f", content='{content_preview}...'" if content_preview else "")
            )
        logger.info("")

    def test_connection(self) -> bool:
        """Test Notion connection"""
        try:
            # Test by retrieving database info
            response = self.client.databases.retrieve(database_id=self.database_id)
            logger.info(
                f"✅ Notion database retrieved successfully: {response.get('title', [{}])[0].get('plain_text', 'Untitled')}"
            )
            return True
        except APIResponseError as e:
            logger.error(f"❌ Notion connection test failed: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Notion connection test error: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description="Publish weekly digest to Notion")
    parser.add_argument("--file", type=str, help="Path to the digest markdown file")
    parser.add_argument("--json", type=str, help="Path to the JSON metadata file")
    parser.add_argument(
        "--test-connection", action="store_true", help="Test Notion connection only"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    try:
        publisher = NotionPublisher()
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        return 1

    if args.test_connection:
        success = publisher.test_connection()
        return 0 if success else 1

    # Find latest digest if no files specified
    if not args.file or not args.json:
        try:
            md_path, json_path = publisher.find_latest_digest()
        except Exception as e:
            logger.error(f"❌ Error finding latest digest: {e}")
            return 1
    else:
        md_path = args.file
        json_path = args.json

    # Load digest data
    try:
        logger.info(f"📄 Loading digest data from {md_path} and {json_path}")
        digest_data = publisher.load_digest_data(md_path, json_path)

        # Preview the content
        logger.info(f"\n📊 Digest Preview:")
        logger.info(f"  Week: {digest_data['week_start']} to {digest_data['week_end']}")
        logger.info(f"  Total newsletters: {digest_data['total_newsletters']}")
        logger.info(
            f"  Genres: {', '.join(digest_data.get('genre_summaries', {}).keys())}"
        )
        logger.info(
            f"  Summary length: {len(digest_data.get('unified_summary', ''))} characters"
        )

        # Publish to Notion
        logger.info("\n🚀 Publishing to Notion...")
        page_id = publisher.publish_to_notion(digest_data)
        if page_id:
            logger.info(
                f"✅ Digest published successfully to Notion! Page ID: {page_id}"
            )
            return 0
        else:
            logger.error("❌ Failed to publish digest to Notion")
            return 1

    except Exception as e:
        logger.exception(f"❌ Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
