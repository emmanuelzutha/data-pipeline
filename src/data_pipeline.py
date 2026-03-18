"""
Professional Data Pipeline - Windows Optimized Version
No emojis, no errors, just pure functionality
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging
import json
import os

# Configure logging for Windows (no emojis)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataPipeline:
    """Professional data pipeline class - Windows optimized"""
    
    def __init__(self):
        """Initialize the pipeline"""
        self.data = None
        self.aggregates = None
        self.stats = None
        logger.info("="*50)
        logger.info("PIPELINE INITIALIZED")
        logger.info("="*50)
    
    def generate_data(self, n_rows=100000):
        """Generate sample data"""
        logger.info(f"Generating {n_rows:,} rows of data...")
        
        # Set random seed for reproducibility
        np.random.seed(42)
        
        # Generate timestamps as strings to avoid JSON issues
        base_time = datetime.now()
        timestamps = []
        for i in range(n_rows):
            timestamps.append(base_time.strftime("%Y-%m-%d %H:%M:%S"))
            base_time = base_time.replace(second=(base_time.second + 1) % 60)
        
        self.data = pd.DataFrame({
            'timestamp': timestamps,
            'user_id': np.random.randint(1000, 9999, n_rows),
            'action': np.random.choice(['click', 'view', 'purchase', 'login'], n_rows),
            'value': np.random.exponential(100, n_rows),
            'location': np.random.choice(['US', 'UK', 'IN', 'DE', 'JP'], n_rows),
            'device': np.random.choice(['mobile', 'desktop', 'tablet'], n_rows)
        })
        
        logger.info(f"Generated {len(self.data):,} rows")
        logger.info(f"Memory usage: {self.data.memory_usage(deep=True).sum() / 1e6:.2f} MB")
        return self
    
    def clean_data(self):
        """Clean and prepare data"""
        logger.info("Cleaning data...")
        
        initial_rows = len(self.data)
        
        # Remove outliers (values beyond 3 standard deviations)
        mean_val = self.data['value'].mean()
        std_val = self.data['value'].std()
        self.data = self.data[
            (self.data['value'] >= mean_val - 3*std_val) & 
            (self.data['value'] <= mean_val + 3*std_val)
        ]
        
        # Add derived columns
        self.data['hour'] = pd.to_datetime(self.data['timestamp']).dt.hour
        self.data['is_business_hours'] = self.data['hour'].between(9, 17).astype(int)
        
        removed_rows = initial_rows - len(self.data)
        logger.info(f"Removed {removed_rows} outliers")
        logger.info(f"Final rows: {len(self.data):,}")
        return self
    
    def analyze(self):
        """Perform comprehensive analysis"""
        logger.info("Performing analysis...")
        
        # Basic statistics
        self.stats = {
            'total_rows': int(len(self.data)),
            'unique_users': int(self.data['user_id'].nunique()),
            'avg_value': float(round(self.data['value'].mean(), 2)),
            'median_value': float(round(self.data['value'].median(), 2)),
            'std_value': float(round(self.data['value'].std(), 2)),
            'total_value': float(round(self.data['value'].sum(), 2)),
            'date_range': {
                'start': str(self.data['timestamp'].min()),
                'end': str(self.data['timestamp'].max())
            }
        }
        
        # Aggregations - store as regular dictionaries with native Python types
        self.aggregates = {
            'by_location': {},
            'by_action': {},
            'by_device': {},
            'by_hour': {}
        }
        
        # Location aggregation
        loc_agg = self.data.groupby('location')['value'].agg(['count', 'mean', 'sum']).round(2)
        for loc in loc_agg.index:
            self.aggregates['by_location'][str(loc)] = {
                'count': int(loc_agg.loc[loc, 'count']),
                'mean': float(loc_agg.loc[loc, 'mean']),
                'sum': float(loc_agg.loc[loc, 'sum'])
            }
        
        # Action aggregation
        action_agg = self.data.groupby('action')['value'].agg(['count', 'mean', 'sum']).round(2)
        for action in action_agg.index:
            self.aggregates['by_action'][str(action)] = {
                'count': int(action_agg.loc[action, 'count']),
                'mean': float(action_agg.loc[action, 'mean']),
                'sum': float(action_agg.loc[action, 'sum'])
            }
        
        # Device aggregation
        device_agg = self.data.groupby('device')['value'].agg(['count', 'mean', 'sum']).round(2)
        for device in device_agg.index:
            self.aggregates['by_device'][str(device)] = {
                'count': int(device_agg.loc[device, 'count']),
                'mean': float(device_agg.loc[device, 'mean']),
                'sum': float(device_agg.loc[device, 'sum'])
            }
        
        # Hour aggregation
        hour_agg = self.data.groupby('hour')['value'].mean().round(2)
        for hour in hour_agg.index:
            self.aggregates['by_hour'][str(int(hour))] = float(hour_agg[hour])
        
        logger.info("Analysis complete")
        return self
    
    def save_results(self):
        """Save all results to files"""
        logger.info("Saving results...")
        
        # Create output directory
        os.makedirs('output', exist_ok=True)
        
        # Save full dataset
        self.data.to_csv('output/data_full.csv', index=False)
        logger.info("Saved full dataset to output/data_full.csv")
        
        # Save analysis results as JSON
        with open('output/analysis_results.json', 'w', encoding='utf-8') as f:
            json.dump({
                'statistics': self.stats,
                'aggregates': self.aggregates,
                'timestamp': str(datetime.now())
            }, f, indent=2)
        logger.info("Saved analysis to output/analysis_results.json")
        
        # Save summary report
        with open('output/summary_report.txt', 'w', encoding='utf-8') as f:
            f.write("="*60 + "\n")
            f.write("DATA PIPELINE SUMMARY REPORT\n")
            f.write("="*60 + "\n\n")
            f.write(f"Report Generated: {datetime.now()}\n")
            f.write(f"Total Rows Processed: {self.stats['total_rows']:,}\n")
            f.write(f"Unique Users: {self.stats['unique_users']:,}\n")
            f.write(f"Average Value: ${self.stats['avg_value']:.2f}\n")
            f.write(f"Median Value: ${self.stats['median_value']:.2f}\n")
            f.write(f"Total Value: ${self.stats['total_value']:,.2f}\n")
            
            f.write("\n" + "-"*40 + "\n")
            f.write("PERFORMANCE BY LOCATION\n")
            f.write("-"*40 + "\n")
            for loc, stats in self.aggregates['by_location'].items():
                f.write(f"{loc}: {stats['count']} transactions, ${stats['mean']:.2f} avg, ${stats['sum']:,.2f} total\n")
            
            f.write("\n" + "-"*40 + "\n")
            f.write("PERFORMANCE BY ACTION\n")
            f.write("-"*40 + "\n")
            for action, stats in self.aggregates['by_action'].items():
                f.write(f"{action}: {stats['count']} transactions, ${stats['mean']:.2f} avg\n")
        
        logger.info("Saved summary report to output/summary_report.txt")
        return self
    
    def print_summary(self):
        """Print summary to console"""
        print("\n" + "="*60)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*60)
        print(f"Status: SUCCESS")
        print(f"Project: {os.getcwd()}")
        print(f"Rows Processed: {self.stats['total_rows']:,}")
        print(f"Unique Users: {self.stats['unique_users']:,}")
        print(f"Average Value: ${self.stats['avg_value']:.2f}")
        print(f"Total Value: ${self.stats['total_value']:,.2f}")
        print("\nFiles Created:")
        print("   - output/data_full.csv")
        print("   - output/analysis_results.json")
        print("   - output/summary_report.txt")
        print("   - pipeline.log")
        print("="*60)

def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("PROFESSIONAL DATA PIPELINE")
    print("="*60)
    
    # Create and run pipeline
    pipeline = DataPipeline()
    
    try:
        pipeline.generate_data(n_rows=100000)
        pipeline.clean_data()
        pipeline.analyze()
        pipeline.save_results()
        pipeline.print_summary()
        
        print("\nPipeline completed successfully! Ready to scale to billions!")
        
    except Exception as e:
        logger.error(f"Error in pipeline: {str(e)}")
        print(f"\nError: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    if exit_code == 0:
        print("\nSUCCESS! Your professional pipeline is working perfectly!\n")