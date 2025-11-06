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


class NotionDigestPublisher:
    """Standalone publisher for weekly digests to Notion"""

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

        # Find the main content body.
        # We'll split the file by the frontmatter (---)
        # and take everything after the second '---'.

        main_content = content
        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) > 2:
                main_content = parts[2].strip()  # Get everything after the frontmatter

        # Now, find the first H2 (##) and take everything *after* its title
        unified_summary = main_content
        if "\n## " in main_content:
            # Get content after the first H2 header line
            body_after_h2 = main_content.split("\n## ", 1)[1]
            # Find the next newline to skip the header text itself
            if "\n\n" in body_after_h2:
                # Get the content *after* the header title line
                unified_summary = body_after_h2.split("\n\n", 1)[1].strip()
            else:
                unified_summary = body_after_h2.strip()

        if not unified_summary:
            unified_summary = "Could not parse digest content."

        # Get genre names from metadata just for the page properties
        genre_keys = metadata.get("genres", {}).keys()

        # Build the digest data structure
        digest_data = {
            "week_start": metadata.get(
                "week_start", (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            ),
            "week_end": metadata.get("week_end", datetime.now().strftime("%Y-%m-%d")),
            "total_newsletters": metadata.get("total_newsletters", 0),
            # Pass genre names for properties, but no content
            "genre_summaries": {genre: {} for genre in genre_keys},
            # Pass the FULL, un-truncated content
            "unified_summary": unified_summary,
        }

        return digest_data

    def create_notion_blocks(self, digest_data: dict) -> list:
        """Create validated Notion blocks from digest data"""
        blocks = []

        # Add header
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

        # Add week info
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

        # Add newsletter count
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

        # Add divider
        blocks.append({"object": "block", "type": "divider", "divider": {}})

        # Add unified summary (which now contains the full article)
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

            # --- NEW: Markdown Parsing Logic ---
            # Split content by paragraphs (double newline)
            all_paragraphs = digest_data["unified_summary"].split("\n\n")

            for para in all_paragraphs:
                para = para.strip()
                if not para:
                    continue

                # Check for H3 (###)
                if para.startswith("### "):
                    blocks.append(
                        {
                            "object": "block",
                            "type": "heading_3",
                            "heading_3": {
                                "rich_text": self._parse_inline_markdown(
                                    para.lstrip("### ").strip()
                                )
                            },
                        }
                    )
                # Check for H2 (##)
                elif para.startswith("## "):
                    blocks.append(
                        {
                            "object": "block",
                            "type": "heading_2",
                            "heading_2": {
                                "rich_text": self._parse_inline_markdown(
                                    para.lstrip("## ").strip()
                                )
                            },
                        }
                    )
                # Check for bullets (* )
                elif para.startswith("* "):
                    blocks.append(
                        {
                            "object": "block",
                            "type": "bulleted_list_item",
                            "bulleted_list_item": {
                                "rich_text": self._parse_inline_markdown(
                                    para.lstrip("* ").strip()
                                )
                            },
                        }
                    )
                # Check for divider (---)
                elif para.strip() == "---":
                    blocks.append({"object": "block", "type": "divider", "divider": {}})
                # Default: Paragraph
                else:
                    blocks.append(
                        {
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": self._parse_inline_markdown(para)
                            },
                        }
                    )
            # --- END: Markdown Parsing Logic ---

            blocks.append({"object": "block", "type": "divider", "divider": {}})

        # Add footer
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

        # Validate blocks
        validated_blocks = []
        for i, block in enumerate(blocks):
            if self._validate_block(block, i):
                validated_blocks.append(block)
            else:
                logger.warning(f"⚠️ Skipping invalid block at index {i}")

        return validated_blocks

    # --- NEW: Helper function to parse simple inline Markdown ---
    def _parse_inline_markdown(self, text: str) -> list:
        """
        Parse simple inline Markdown (bold) into Notion rich_text array.
        Example: "This is **bold** text"
        """
        parts = text.split("**")
        rich_text_array = []
        for i, part in enumerate(parts):
            if not part:  # Skip empty strings (from e.g., **start bold**)
                continue

            # Parts at odd indices are bolded
            is_bold = i % 2 == 1

            rich_text_array.append(
                {
                    "type": "text",
                    "text": {"content": part},
                    "annotations": {"bold": is_bold},
                }
            )

        # If no parts found (e.g., empty string), return empty text
        if not rich_text_array:
            return [{"type": "text", "text": {"content": ""}}]

        return rich_text_array

    def _validate_block(self, block: dict, index: int) -> bool:
        """Validate that a block has proper structure for Notion API"""
        try:
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
                # 'divider' is a valid type that has no type_specific_key block
                if block_type == "divider":
                    return "divider" in block

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
                content_field = block[type_specific_key]
                if "rich_text" not in content_field or not content_field["rich_text"]:
                    # This check is tricky now because _parse_inline_markdown
                    # might return an empty list for an empty string.
                    # We'll allow empty rich_text array for now.
                    pass

            # Handle divider case which passed the 'type_specific_key' check
            if block_type == "divider":
                return "divider" in block

            return True
        except Exception as e:
            logger.error(f"❌ Error validating block {index}: {e}")
            return False

    def _split_long_text(self, text: str, max_length: int = 2000) -> list:
        """
        Split long text into paragraphs that respect Notion's limits.
        NOTE: This is no longer used for the main content parsing but
        kept in case it's needed elsewhere.
        """
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
        """Publish digest to Notion"""
        try:
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
            return None

    def _log_block_structure(self, children: list):
        """Log block structure for debugging"""
        logger.info(f"\n🔍 Notion Block Structure Debug:")
        logger.info(f"Total blocks: {len(children)}")
        for i, block in enumerate(children):
            block_type = block.get("type", "unknown")
            content_preview = ""

            # Try to get content preview for common block types
            try:
                if block_type in [
                    "paragraph",
                    "heading_1",
                    "heading_2",
                    "heading_3",
                ]:
                    rich_text = block.get(block_type, {}).get("rich_text", [])
                    if rich_text and isinstance(rich_text[0], dict):
                        content_preview = (
                            rich_text[0].get("text", {}).get("content", "")[:100]
                        )
                elif block_type == "bulleted_list_item":
                    rich_text = block.get("bulleted_list_item", {}).get("rich_text", [])
                    if rich_text and isinstance(rich_text[0], dict):
                        content_preview = (
                            rich_text[0].get("text", {}).get("content", "")[:100]
                        )
            except Exception:
                content_preview = "[error extracting content]"

            logger.info(
                f"Block {i}: type='{block_type}'"
                + (f", content='{content_preview}...'" if content_preview else "")
            )

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
        publisher = NotionDigestPublisher()
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
