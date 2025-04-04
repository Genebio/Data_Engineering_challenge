"""Customer journeys builder module for the Hansel Attribution Pipeline."""

from typing import Optional

import pandas as pd

from pipeline.db_operations import DatabaseManager


class CustomerJourneyBuilder:
    """Customer journey builder class."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize CustomerJourneyBuilder.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
    
    def build_journeys(self, start_date: Optional[str] = None, 
                      end_date: Optional[str] = None) -> pd.DataFrame:
        """Build customer journeys from database tables.
        
        This method queries the conversions and session_sources tables
        to create customer journeys, filtered by optional date parameters.
        
        Args:
            start_date: Optional start date filter (format: YYYY-MM-DD)
            end_date: Optional end date filter (format: YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: Customer journeys dataframe
        """
        # Build the conversion query with optional date filtering
        conv_query = """
            SELECT 
                conv_id, 
                user_id, 
                conv_date || ' ' || conv_time as timestamp
            FROM conversions
        """
        
        # Add date filtering if provided
        if start_date or end_date:
            filters = []
            if start_date:
                filters.append(f"conv_date >= '{start_date}'")
            if end_date:
                filters.append(f"conv_date <= '{end_date}'")
            
            if filters:
                conv_query += " WHERE " + " AND ".join(filters)
                
        # Execute queries
        conversions_df = self.db_manager.read_sql(conv_query)
        
        # Format timestamp as datetime object
        conversions_df['timestamp'] = pd.to_datetime(conversions_df['timestamp'])
        
        # Query session sources
        session_sources_df = self.db_manager.read_sql("""
            SELECT 
                session_id, 
                user_id, 
                event_date || ' ' || event_time as timestamp,
                channel_name,
                holder_engagement,
                closer_engagement,
                impression_interaction
            FROM session_sources
        """)
        
        # Format timestamp as datetime object
        session_sources_df['timestamp'] = pd.to_datetime(session_sources_df['timestamp'])
        
        # Prepare journeys collection
        all_journeys = []
        
        # For each conversion, find all sessions for that user that happened before the conversion
        for _, conv_row in conversions_df.iterrows():
            conv_id = conv_row['conv_id']
            user_id = conv_row['user_id']
            conv_timestamp = conv_row['timestamp']
            
            # Get sessions for this user
            user_sessions = session_sources_df[session_sources_df['user_id'] == user_id].copy()
            
            # Filter sessions that happened before or at the conversion time
            user_sessions = user_sessions[user_sessions['timestamp'] <= conv_timestamp]
            
            if not user_sessions.empty:
                # Mark whether this session is the conversion event
                user_sessions['conversion'] = 0
                
                # Add conversion_id to each session
                user_sessions['conversion_id'] = conv_id
                
                # Rename channel_name to channel_label to match expected format
                user_sessions = user_sessions.rename(columns={'channel_name': 'channel_label'})
                
                # Select and reorder columns
                journey_data = user_sessions[[
                    'conversion_id', 'session_id', 'timestamp', 'channel_label',
                    'holder_engagement', 'closer_engagement', 'conversion', 'impression_interaction'
                ]]
                
                # Add the journey to our collection
                all_journeys.append(journey_data)
        
        # Combine all journeys into a single dataframe
        if all_journeys:
            customer_journeys_df = pd.concat(all_journeys)
            
            # Convert timestamp back to string format for CSV
            customer_journeys_df['timestamp'] = customer_journeys_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            return customer_journeys_df
        
        return pd.DataFrame()
    
    def save_to_csv(self, journeys_df: pd.DataFrame, 
                   output_path: str = 'customer_journeys.csv') -> None:
        """Save customer journeys to CSV file.
        
        Args:
            journeys_df: Customer journeys dataframe
            output_path: Output file path
        """
        journeys_df.to_csv(output_path, index=False)
        
        print(f"Created customer journeys for {journeys_df['conversion_id'].nunique()} conversions")
        print(f"Total journey touchpoints: {len(journeys_df)}")
        
    def build_and_save(self, output_path: str = 'customer_journeys.csv',
                      start_date: Optional[str] = None, 
                      end_date: Optional[str] = None) -> pd.DataFrame:
        """Build and save customer journeys in one operation.
        
        Args:
            output_path: Output file path
            start_date: Optional start date filter (format: YYYY-MM-DD)
            end_date: Optional end date filter (format: YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: Customer journeys dataframe
        """
        journeys_df = self.build_journeys(start_date, end_date)
        
        if not journeys_df.empty:
            self.save_to_csv(journeys_df, output_path)
        else:
            print("No customer journeys were found for the given date range")
            
        return journeys_df