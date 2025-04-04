#!/usr/bin/env python3
"""
Hansel Attribution Pipeline Runner.

This script runs the attribution pipeline with options for running individual steps
or the entire pipeline with date range filtering.

Examples:
    # Run the entire pipeline
    python run_pipeline.py
    
    # Run with date range
    python run_pipeline.py --start-date 2022-01-01 --end-date 2022-01-31
    
    # Run only the journey building step
    python run_pipeline.py --step build-journeys
    
    # Run only the API step (using existing journeys)
    python run_pipeline.py --step send-to-api
    
    # Run only the reporting step (after API step has been completed)
    python run_pipeline.py --step generate-report
"""

import argparse
import pandas as pd

from pipeline.pipeline import AttributionPipeline


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run the Hansel Attribution Pipeline")
    
    parser.add_argument(
        "--config", 
        default="config.ini",
        help="Path to configuration file (default: config.ini)"
    )
    
    parser.add_argument(
        "--step",
        choices=["build-journeys", "send-to-api", "generate-report", "all"],
        default="all",
        help="Run a specific pipeline step (default: all)"
    )
    
    parser.add_argument(
        "--start-date",
        help="Start date filter (format: YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date",
        help="End date filter (format: YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--journeys-path",
        default="customer_journeys.csv",
        help="Path to save or load customer journeys CSV (default: customer_journeys.csv)"
    )
    
    parser.add_argument(
        "--report-path",
        default="channel_reporting.csv",
        help="Path to save channel reporting CSV (default: channel_reporting.csv)"
    )
    
    return parser.parse_args()


def main():
    """Run the pipeline based on command-line arguments."""
    args = parse_args()
    
    # Initialize pipeline
    pipeline = AttributionPipeline(args.config)
    
    if args.step == "all":
        # Run the full pipeline
        pipeline.run_pipeline(
            journeys_path=args.journeys_path,
            report_path=args.report_path,
            start_date=args.start_date,
            end_date=args.end_date
        )
    elif args.step == "build-journeys":
        # Run only the journey building step
        pipeline.run_step_build_journeys(
            output_path=args.journeys_path,
            start_date=args.start_date,
            end_date=args.end_date
        )
    elif args.step == "send-to-api":
        # Run only the API step
        print("Loading journeys from", args.journeys_path)
        journeys_df = pd.read_csv(args.journeys_path)
        pipeline.run_step_send_to_api(journeys_df)
    elif args.step == "generate-report":
        # Run only the reporting step
        pipeline.run_step_generate_report(
            output_path=args.report_path,
            start_date=args.start_date,
            end_date=args.end_date
        )


if __name__ == "__main__":
    main()