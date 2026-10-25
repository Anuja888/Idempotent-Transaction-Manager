import os
import sys

# Make the project root importable when this file runs as a Vercel serverless
# function, so `from main import app` (and its `from database import ...`) resolve.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app  # noqa: E402  (FastAPI/ASGI app detected by the Vercel runtime)
