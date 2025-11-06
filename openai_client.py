import os
from dotenv import load_dotenv
import google.generativeai as genai
from config import DEFAULT_MODEL

load_dotenv()


def get_gemini_client():
    """Create and return Google Gemini client"""
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY not found in environment variables")

    genai.configure(api_key=GEMINI_API_KEY)
    return genai


def chat_completion(messages, model=None):
    """Simple wrapper for Gemini chat completion with centralized model config"""
    if model is None:
        model = DEFAULT_MODEL

    # Gemini expects a different format than OpenAI
    # Convert OpenAI-style messages to Gemini format
    gemini_model = genai.GenerativeModel(model)

    # Extract the user message (Gemini primarily uses user messages)
    user_content = ""
    for message in messages:
        if message["role"] == "user":
            user_content = message["content"]
            break

    if not user_content:
        raise ValueError("No user message found in messages")

    # Generate response
    response = gemini_model.generate_content(user_content)

    # Handle potential errors
    if not response.text:
        raise ValueError("Gemini returned empty response")

    return response.text


# Test function
if __name__ == "__main__":
    test_messages = [{"role": "user", "content": "What is 2+2?"}]
    try:
        response = chat_completion(test_messages)
        print(f"Test successful with model {DEFAULT_MODEL}: {response}")
    except Exception as e:
        print(f"Test failed: {e}")
