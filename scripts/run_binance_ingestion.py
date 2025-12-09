#!/usr/bin/env python
"""
Binance Klines Ingestion Runner
Run script that executes the Binance klines ingestion pipeline
This script is designed to be run in Docker containers

Uses dataclass-based return types for type-safe operation
"""
import sys
import argparse
from pathlib import Path

# Add app directory to path for both local and Docker execution
sys.path.insert(0, '/app')
sys.path.insert(0, str(Path(__file__).parent.parent))

from platforms.ingestion.crypto.binance.extract.fetch_binance_klines import run_ingestion


def main():
    """Main entry point for ingestion runner"""
    parser = argparse.ArgumentParser(
        description="Binance Klines Ingestion Runner"
    )
    
    parser.add_argument(
        "--csv-path",
        type=str,
        default="/app/data/transactions.csv",
        help="Path to transactions CSV file (default: /app/data/transactions.csv)"
    )
    
    parser.add_argument(
        "--interval",
        type=str,
        default="1h",
        choices=["1m", "5m", "15m", "1h", "4h", "1d"],
        help="Kline interval (default: 1h)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for parquet files (optional, uses config default if not specified)"
    )
    
    args = parser.parse_args()    
    result = run_ingestion(
        csv_path=args.csv_path,
        interval=args.interval,
        output_dir=args.output_dir
    )
    print(result)

    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
