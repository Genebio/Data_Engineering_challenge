"""Channel reporting module for the Hansel Attribution Pipeline."""

from typing import Optional

import pandas as pd

from pipeline.db_operations import DatabaseManager


class ChannelReporter:
    """Channel reporting class for generating attribution reports."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize ChannelReporter.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
    
    def generate_report(self, start_date: Optional[str] = None, 
                       end_date: Optional[str] = None) -> pd.DataFrame:
        """Generate channel reporting data.
        
        Args:
            start_date: Optional start date filter (format: YYYY-MM-DD)
            end_date: Optional end date filter (format: YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: Channel reporting dataframe
        """
        # Build query with optional date filtering
        query = """
        -- Get sessions with their channel, date, and costs 
        WITH session_data AS (
            SELECT 
                ss.session_id,
                ss.channel_name,
                ss.event_date as date,
                COALESCE(sc.cost, 0) as cost
            FROM 
                session_sources ss
            LEFT JOIN 
                session_costs sc ON ss.session_id = sc.session_id
        """
        
        # Add date filtering if provided
        if start_date or end_date:
            filters = []
            if start_date:
                filters.append(f"ss.event_date >= '{start_date}'")
            if end_date:
                filters.append(f"ss.event_date <= '{end_date}'")
            
            if filters:
                query += " WHERE " + " AND ".join(filters)
        
        # Complete the query
        query += """
        ),
        -- Calculate IHC and IHC revenue per session
        attribution_data AS (
            SELECT 
                sd.session_id,
                sd.channel_name,
                sd.date,
                sd.cost,
                acj.ihc,
                acj.conv_id,
                c.revenue,
                (acj.ihc * c.revenue) as ihc_revenue
            FROM 
                session_data sd
            JOIN 
                attribution_customer_journey acj ON sd.session_id = acj.session_id
            JOIN 
                conversions c ON acj.conv_id = c.conv_id
        ),
        -- Aggregate at channel and date level
        channel_date_report AS (
            SELECT 
                channel_name,
                date,
                SUM(cost) as cost,
                SUM(ihc) as ihc,
                SUM(ihc_revenue) as ihc_revenue
            FROM 
                attribution_data
            GROUP BY 
                channel_name, date
        )
        SELECT * FROM channel_date_report
        """
        
        # Execute query
        channel_reporting_df = self.db_manager.read_sql(query)
        
        # Insert data into channel_reporting table
        self.db_manager.execute_query("DELETE FROM channel_reporting")
        
        with self.db_manager.connection() as conn:
            cursor = conn.cursor()
            for _, row in channel_reporting_df.iterrows():
                cursor.execute(
                    "INSERT INTO channel_reporting (channel_name, date, cost, ihc, ihc_revenue) VALUES (?, ?, ?, ?, ?)",
                    (row['channel_name'], row['date'], row['cost'], row['ihc'], row['ihc_revenue'])
                )
            conn.commit()
        
        # Add CPO and ROAS columns for the CSV export
        channel_reporting_df['CPO'] = channel_reporting_df['cost'] / channel_reporting_df['ihc']
        channel_reporting_df['ROAS'] = channel_reporting_df['ihc_revenue'] / channel_reporting_df['cost']
        
        # Handle potential infinity or NaN values (when cost or ihc is 0)
        channel_reporting_df['CPO'] = channel_reporting_df['CPO'].fillna(0)
        channel_reporting_df['ROAS'] = channel_reporting_df['ROAS'].fillna(0)
        # Replace infinite values with 0
        channel_reporting_df = channel_reporting_df.replace([float('inf'), -float('inf')], 0)
        
        return channel_reporting_df
    
    def save_to_csv(self, reporting_df: pd.DataFrame, 
                   output_path: str = 'channel_reporting.csv') -> None:
        """Save channel reporting data to CSV file.
        
        Args:
            reporting_df: Channel reporting dataframe
            output_path: Output file path
        """
        reporting_df.to_csv(output_path, index=False)
        
        # Print a summary
        print(f"Generated channel reporting for {len(reporting_df)} channel-date combinations")
        print(f"Total marketing cost: {reporting_df['cost'].sum():.2f} Euro")
        print(f"Total IHC revenue: {reporting_df['ihc_revenue'].sum():.2f} Euro")
        
        # Calculate and print average CPO and ROAS for non-zero values
        valid_cpo = reporting_df[reporting_df['CPO'] > 0]['CPO']
        valid_roas = reporting_df[reporting_df['ROAS'] > 0]['ROAS']
        
        if not valid_cpo.empty:
            print(f"Average CPO: {valid_cpo.mean():.2f} Euro")
        else:
            print("No valid CPO values found")
            
        if not valid_roas.empty:
            print(f"Average ROAS: {valid_roas.mean():.2f}")
        else:
            print("No valid ROAS values found")
            
        print(f"Channel reporting data has been written to the database and exported to {output_path}")
    
    def generate_and_save(self, output_path: str = 'channel_reporting.csv',
                         start_date: Optional[str] = None, 
                         end_date: Optional[str] = None) -> pd.DataFrame:
        """Generate and save channel reporting in one operation.
        
        Args:
            output_path: Output file path
            start_date: Optional start date filter (format: YYYY-MM-DD)
            end_date: Optional end date filter (format: YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: Channel reporting dataframe
        """
        reporting_df = self.generate_report(start_date, end_date)
        
        if not reporting_df.empty:
            self.save_to_csv(reporting_df, output_path)
        else:
            print("No reporting data was generated for the given date range")
            
        return reporting_df