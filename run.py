"""
Entry point for running the Polymarket Whale Copier application.

Usage:
    python run.py
    python run.py --port 8080
    python run.py --reload     (development mode)
"""

import argparse
import sys

import uvicorn


def main():
    parser = argparse.ArgumentParser(description="Polymarket Whale Copier")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload (dev mode)")
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])
    args = parser.parse_args()

    print(f"\n{'='*50}")
    print("  Polymarket Whale Copier")
    print(f"  http://{args.host}:{args.port}")
    print(f"{'='*50}\n")

    uvicorn.run(
        "backend.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
