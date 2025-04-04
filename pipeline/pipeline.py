"""Main pipeline module for the Hansel Attribution Pipeline."""

from typing import Optional

import pandas as pd

from pipeline.config import get_config
from pipeline.db_operations import DatabaseManager
from pipeline.cj_builder import CustomerJourneyBuilder
from pipeline.api_client import IHCApiClient
from pipeline.channel_reporter import ChannelReporter


class AttributionPipeline:
    """Main attribution pipeline class that orchestrates the entire workflow."""
    
    def __init__(self, config_path: str = "config.ini"):
        """Initialize AttributionPipeline.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config = get_config(config_path)
        self.db_manager = DatabaseManager(self.config.db_name)
        self.journey_builder = CustomerJourneyBuilder(self.db_manager)
        self.api_client = IHCApiClient(self.config, self.db_manager)
        self.reporter = ChannelReporter(self.db_manager)
        
    def run_step_build_journeys(self, output_path: str = 'customer_journeys.csv',
                              start_date: Optional[str] = None, 
                              end_date: Optional[str] = None) -> pd.DataFrame:
        """Run the journey building step.
        
        Args:
            output_path: Output file path for journeys CSV
            start_date: Optional start date filter (format: YYYY-MM-DD)
            end_date: Optional end date filter (format: YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: Customer journeys dataframe
        """
        print("===== STEP 1: Building Customer Journeys =====")
        return self.journey_builder.build_and_save(output_path, start_date, end_date)
    
    def run_step_send_to_api(self, journeys_df: pd.DataFrame) -> int:
        """Run the API submission step.
        
        Args:
            journeys_df: Customer journeys dataframe
            
        Returns:
            int: Number of records written to database
        """
        print("\n===== STEP 2: Sending to IHC API and Writing Results =====")
        return self.api_client.process_journeys(journeys_df)
        
    def run_step_generate_report(self, output_path: str = 'channel_reporting.csv',
                               start_date: Optional[str] = None, 
                               end_date: Optional[str] = None) -> pd.DataFrame:
        """Run the reporting step.
        
        Args:
            output_path: Output file path for report CSV
            start_date: Optional start date filter (format: YYYY-MM-DD)
            end_date: Optional end date filter (format: YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: Channel reporting dataframe
        """
        print("\n===== STEP 3: Generating Channel Reporting =====")
        return self.reporter.generate_and_save(output_path, start_date, end_date)
    
    def run_pipeline(self, journeys_path: str = 'customer_journeys.csv',
                    report_path: str = 'channel_reporting.csv',
                    start_date: Optional[str] = None, 
                    end_date: Optional[str] = None) -> None:
        """Run the complete pipeline.
        
        Args:
            journeys_path: Output file path for journeys CSV
            report_path: Output file path for report CSV
            start_date: Optional start date filter (format: YYYY-MM-DD)
            end_date: Optional end date filter (format: YYYY-MM-DD)
        """
        print("Starting Attribution Pipeline...")
        print(f"Time range: {start_date or 'All'} to {end_date or 'All'}")
        
        # Step 1: Build customer journeys
        journeys_df = self.run_step_build_journeys(journeys_path, start_date, end_date)
        
        if journeys_df.empty:
            print("No journeys were found. Pipeline cannot continue.")
            return
        
        # Step 2: Send to API and write results
        records = self.run_step_send_to_api(journeys_df)
        
        if records == 0:
            print("No IHC records were written. Pipeline cannot continue.")
            return
        
        # Step 3: Generate channel reporting
        self.run_step_generate_report(report_path, start_date, end_date)
        
        print("\n===== Pipeline Completed Successfully =====")