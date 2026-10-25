"""
Vercel serverless entry point.

Vercel's Python runtime detects the `app` variable (an ASGI application)
and routes all traffic to it via the rewrite rule in vercel.json.

The sys.path insert makes the project root importable so that
  `from main import app`  (and its `from database import ...`)
resolve correctly when this file runs as api/index.py inside Vercel.
"""
import os
import sys

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app  # noqa: F401, E402  — Vercel detects this ASGI `app`
