"""API client module for the Hansel Attribution Pipeline."""

import json
import time
from typing import Dict, List, Optional, Any

import pandas as pd
import requests

from pipeline.config import PipelineConfig
from pipeline.db_operations import DatabaseManager


class IHCApiClient:
    """Client for interacting with the IHC Attribution API."""
    
    def __init__(self, config: PipelineConfig, db_manager: DatabaseManager):
        """Initialize IHCApiClient.
        
        Args:
            config: Pipeline configuration
            db_manager: Database manager instance
        """
        self.config = config
        self.db_manager = db_manager
        self.api_url = f"https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={config.conv_type_id}"
        self.headers = {
            'Content-Type': 'application/json',
            'x-api-key': config.api_key
        }
    
    def send_journeys_to_api(self, journey_data: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
        """Send customer journey data to the IHC API.
        
        Args:
            journey_data: List of journey dictionaries
            
        Returns:
            Optional[List[Dict[str, Any]]]: API response value or None if error
        """
        body = {'customer_journeys': journey_data}
        
        try:
            response = requests.post(
                self.api_url,
                data=json.dumps(body),
                headers=self.headers
            )
            
            if response.status_code != 200:
                print(f"Error: Status code {response.status_code}")
                print(response.text)
                return None
            
            results = response.json()
            
            print(f"Status Code: {results.get('statusCode')}")
            if results.get('partialFailureErrors'):
                print(f"Partial Failure Errors: {results['partialFailureErrors']}")
            
            return results.get('value', [])
        
        except Exception as e:
            print(f"Error sending request: {e}")
            return None
    
    def validate_ihc_results(self, ihc_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate and normalize IHC values to ensure they sum to 1 for each conversion.
        
        Args:
            ihc_results: IHC attribution results from API
            
        Returns:
            List[Dict[str, Any]]: Validated results
        """
        if not ihc_results:
            return []
        
        validated_results = []
        
        # Group results by conversion_id
        conversions = {}
        for result in ihc_results:
            conv_id = result['conversion_id']
            if conv_id not in conversions:
                conversions[conv_id] = []
            conversions[conv_id].append(result)
        
        # Process each conversion group
        for conv_id, sessions in conversions.items():
            # Calculate sum of IHC values for this conversion
            ihc_sum = sum(session['ihc'] for session in sessions)
            
            # If sum is not close to 1, normalize values
            if abs(ihc_sum - 1.0) > 0.0001:
                print(f"Normalizing IHC values for conversion {conv_id} (sum was {ihc_sum})")
                for session in sessions:
                    session['ihc'] = session['ihc'] / ihc_sum
            
            # Verify normalization worked
            new_sum = sum(session['ihc'] for session in sessions)
            print(f"Conversion {conv_id}: IHC sum = {new_sum}")
            
            # Add normalized sessions to validated results
            validated_results.extend(sessions)
        
        return validated_results
    
    def write_ihc_to_db(self, ihc_results: List[Dict[str, Any]]) -> int:
        """Write IHC results to database.
        
        Args:
            ihc_results: Validated IHC attribution results
            
        Returns:
            int: Number of records written
        """
        if not ihc_results:
            return 0
        
        # Extract the relevant fields for the database
        rows = []
        for result in ihc_results:
            rows.append({
                'conv_id': result['conversion_id'],
                'session_id': result['session_id'],
                'ihc': result['ihc']
            })
        
        results_df = pd.DataFrame(rows)
        
        # Double check that IHC sums to 1 for each conversion
        ihc_sums = results_df.groupby('conv_id')['ihc'].sum()
        for conv_id, ihc_sum in ihc_sums.items():
            if abs(ihc_sum - 1.0) > 0.0001:
                print(f"Warning: IHC sum for conversion {conv_id} is {ihc_sum}, not 1.0")
        
        # Clear existing data and insert new records
        self.db_manager.execute_query("DELETE FROM attribution_customer_journey")
        
        # Insert data
        with self.db_manager.connection() as conn:
            cursor = conn.cursor()
            for _, row in results_df.iterrows():
                cursor.execute(
                    "INSERT INTO attribution_customer_journey (conv_id, session_id, ihc) VALUES (?, ?, ?)",
                    (row['conv_id'], row['session_id'], row['ihc'])
                )
            conn.commit()
        
        return len(results_df)
    
    def process_journeys(self, journeys_df: pd.DataFrame) -> int:
        """Process customer journeys through API and write results to database.
        
        This method handles chunking and API limits.
        
        Args:
            journeys_df: Customer journeys dataframe
            
        Returns:
            int: Total number of records written to database
        """
        if journeys_df.empty:
            print("No journeys to process")
            return 0
        
        # Get journey conversion IDs
        unique_conversions = journeys_df['conversion_id'].unique()
        total_conversions = len(unique_conversions)
        
        print(f"Processing {total_conversions} unique conversions")
        print(f"API limits: max {self.config.max_journeys_per_request} journeys and "
              f"{self.config.max_sessions_per_request} sessions per request")
        
        total_processed = 0
        total_records = 0
        
        # Process in chunks based on API limits
        for i in range(0, total_conversions, self.config.max_journeys_per_request):
            # Get chunk of conversion IDs
            conversion_chunk = unique_conversions[i:i+self.config.max_journeys_per_request]
            chunk_num = i // self.config.max_journeys_per_request + 1
            total_chunks = (total_conversions - 1) // self.config.max_journeys_per_request + 1
            
            # Get all journeys for these conversions
            journey_chunk_df = journeys_df[journeys_df['conversion_id'].isin(conversion_chunk)]
            
            # Check if we need to further split due to session limit
            if len(journey_chunk_df) > self.config.max_sessions_per_request:
                print(f"Chunk {chunk_num} exceeds session limit, processing conversions individually")
                
                for conv_id in conversion_chunk:
                    single_conv_df = journeys_df[journeys_df['conversion_id'] == conv_id]
                    num_sessions = len(single_conv_df)
                    
                    if num_sessions > self.config.max_sessions_per_request:
                        print(f"Skipping conversion {conv_id} - too many sessions ({num_sessions})")
                        continue
                        
                    print(f"Processing conversion {conv_id} with {num_sessions} sessions")
                    
                    # Convert to list of dictionaries
                    journey_data = single_conv_df.to_dict('records')
                    
                    # Send to API
                    results = self.send_journeys_to_api(journey_data)
                    if results:
                        # Validate and normalize IHC values
                        validated_results = self.validate_ihc_results(results)
                        
                        # Write to database
                        records = self.write_ihc_to_db(validated_results)
                        total_records += records
                        total_processed += 1
                        print(f"Wrote {records} records to database")
                    
                    # Brief pause to avoid API rate limits
                    time.sleep(1)
            else:
                # Process the chunk normally
                num_conversions = len(conversion_chunk)
                num_sessions = len(journey_chunk_df)
                
                print(f"Sending chunk {chunk_num}/{total_chunks} with {num_sessions} "
                      f"sessions across {num_conversions} conversions")
                
                # Convert to list of dictionaries  
                journey_data = journey_chunk_df.to_dict('records')
                
                # Send to API
                results = self.send_journeys_to_api(journey_data)
                if results:
                    # Validate and normalize IHC values
                    validated_results = self.validate_ihc_results(results)
                    
                    # Write to database
                    records = self.write_ihc_to_db(validated_results)
                    total_records += records
                    total_processed += len(conversion_chunk)
                    print(f"Wrote {records} records to database")
                
                # Brief pause to avoid API rate limits
                time.sleep(1)
        
        print(f"Completed processing {total_processed} conversions")
        print(f"Total IHC records written to database: {total_records}")
        
        # Verify data was written correctly
        self.verify_ihc_data()
        
        return total_records
        
    def verify_ihc_data(self) -> None:
        """Verify that IHC data was written correctly to the database."""
        verification_df = self.db_manager.read_sql(
            "SELECT conv_id, SUM(ihc) as ihc_sum FROM attribution_customer_journey GROUP BY conv_id"
        )
        
        print(f"\nVerification results:")
        print(f"Total conversions in database: {len(verification_df)}")
        print(f"Conversions with IHC sum = 1: {sum(abs(verification_df['ihc_sum'] - 1.0) < 0.0001)}")
        
        # Display any conversions with incorrect IHC sum
        incorrect_sums = verification_df[abs(verification_df['ihc_sum'] - 1.0) > 0.0001]
        if len(incorrect_sums) > 0:
            print(f"Warning: Found {len(incorrect_sums)} conversions with incorrect IHC sum")
            print(incorrect_sums.head())