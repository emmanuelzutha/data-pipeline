"""
Billion-Scale Data Pipeline with Dask
Process 1 Billion+ rows using distributed computing
"""

import dask.dataframe as dd
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import json
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('billion_pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BillionScalePipeline:
    """Process billions of rows using Dask"""
    
    def __init__(self):
        self.data = None
        logger.info("="*60)
        logger.info("BILLION-SCALE PIPELINE INITIALIZED")
        logger.info("="*60)
    
    def generate_billion_rows(self, n_rows=1_000_000_000, partitions=100):
        """Generate 1 billion rows using Dask"""
        logger.info(f"Generating {n_rows:,} rows of data with {partitions} partitions...")
        
        # Calculate rows per partition
        rows_per_partition = n_rows // partitions
        
        # Create list of partitions
        partitions_list = []
        for i in range(partitions):
            # Each partition is a small pandas DataFrame
            partition = pd.DataFrame({
                'timestamp': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * rows_per_partition,
                'user_id': np.random.randint(1000, 9999, rows_per_partition),
                'action': np.random.choice(['click', 'view', 'purchase', 'login'], rows_per_partition),
                'value': np.random.exponential(100, rows_per_partition),
                'location': np.random.choice(['US', 'UK', 'IN', 'DE', 'JP'], rows_per_partition),
                'device': np.random.choice(['mobile', 'desktop', 'tablet'], rows_per_partition)
            })
            partitions_list.append(partition)
            if i % 10 == 0:
                logger.info(f"  Created partition {i+1}/{partitions}")
        
        # Convert to Dask DataFrame
        self.data = dd.from_pandas(pd.concat(partitions_list, ignore_index=True), 
                                   npartitions=partitions)
        
        logger.info(f"✅ Created Dask DataFrame with {self.data.npartitions} partitions")
        logger.info(f"   Total rows: {n_rows:,}")
        logger.info(f"   Memory: Distributed across partitions")
        return self
    
    def analyze_billions(self):
        """Perform analysis on billions of rows"""
        logger.info("Performing billion-scale analysis...")
        
        # Compute basic statistics (lazy evaluation)
        self.stats = {
            'total_rows': len(self.data),
            'unique_users': self.data['user_id'].nunique(),
            'avg_value': self.data['value'].mean(),
            'std_value': self.data['value'].std(),
            'total_value': self.data['value'].sum()
        }
        
        # Group by operations (these are lazy)
        self.aggregates = {
            'by_location': self.data.groupby('location')['value'].agg(['count', 'mean', 'sum']),
            'by_action': self.data.groupby('action')['value'].agg(['count', 'mean', 'sum']),
            'by_device': self.data.groupby('device')['value'].agg(['count', 'mean', 'sum'])
        }
        
        logger.info("Computing results (this may take a few minutes for billions of rows)...")
        
        # Trigger computation
        computed_stats = {}
        for key, value in self.stats.items():
            if key != 'total_rows':  # total_rows is already computed
                computed_stats[key] = float(value.compute())
            else:
                computed_stats[key] = int(value.compute() if hasattr(value, 'compute') else value)
        
        computed_aggs = {}
        for key, agg in self.aggregates.items():
            computed_aggs[key] = agg.compute().round(2).to_dict()
        
        logger.info("✅ Analysis complete!")
        return computed_stats, computed_aggs
    
    def save_summary(self, stats, aggs):
        """Save summary report"""
        logger.info("Saving billion-scale summary...")
        
        os.makedirs('billion_output', exist_ok=True)
        
        # Save as JSON
        with open('billion_output/billion_analysis.json', 'w', encoding='utf-8') as f:
            json.dump({
                'statistics': stats,
                'aggregates': aggs,
                'timestamp': str(datetime.now())
            }, f, indent=2)
        
        # Save readable report
        with open('billion_output/billion_report.txt', 'w', encoding='utf-8') as f:
            f.write("="*60 + "\n")
            f.write("BILLION-SCALE DATA ANALYSIS REPORT\n")
            f.write("="*60 + "\n\n")
            f.write(f"Report Generated: {datetime.now()}\n")
            f.write(f"Total Rows Analyzed: {stats['total_rows']:,}\n")
            f.write(f"Unique Users: {stats['unique_users']:,}\n")
            f.write(f"Average Value: ${stats['avg_value']:.2f}\n")
            f.write(f"Total Value: ${stats['total_value']:,.2f}\n\n")
            
            f.write("PERFORMANCE BY LOCATION\n")
            f.write("-"*40 + "\n")
            for loc, data in aggs['by_location'].items():
                f.write(f"{loc}: {data['count']} transactions, ${data['mean']:.2f} avg, ${data['sum']:,.2f} total\n")
        
        logger.info("✅ Summary saved to billion_output/")
        
    def scale_test(self):
        """Test different scale levels"""
        scales = [1_000_000, 10_000_000, 100_000_000, 1_000_000_000]
        
        print("\n" + "="*60)
        print("SCALE TEST RESULTS")
        print("="*60)
        
        for scale in scales:
            print(f"\n📊 Testing {scale:,} rows...")
            start = datetime.now()
            
            # Generate data at this scale
            data = self.generate_billion_rows(n_rows=scale, partitions=max(10, scale//10_000_000))
            end = datetime.now()
            
            print(f"   ✅ Generation time: {(end-start).total_seconds():.2f} seconds")
            print(f"   📈 Partitions: {data.data.npartitions}")

def main():
    """Main execution"""
    print("\n" + "🚀"*10 + " BILLION-SCALE PIPELINE " + "🚀"*10)
    
    pipeline = BillionScalePipeline()
    
    # Test with 10 million first (you can increase to 1 billion!)
    rows_to_test = 10_000_000  # Start with 10 million
    # rows_to_test = 1_000_000_000  # Uncomment for 1 billion!
    
    try:
        # Generate data
        pipeline.generate_billion_rows(n_rows=rows_to_test, partitions=20)
        
        # Analyze
        stats, aggs = pipeline.analyze_billions()
        
        # Save results
        pipeline.save_summary(stats, aggs)
        
        # Print summary
        print("\n" + "="*60)
        print("BILLION-SCALE PIPELINE SUMMARY")
        print("="*60)
        print(f"Status: SUCCESS")
        print(f"Rows Processed: {stats['total_rows']:,}")
        print(f"Unique Users: {stats['unique_users']:,}")
        print(f"Average Value: ${stats['avg_value']:.2f}")
        print(f"Total Value: ${stats['total_value']:,.2f}")
        print("\nFiles saved in 'billion_output/' folder")
        print("="*60)
        
        # Run scale test
        pipeline.scale_test()
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        print(f"\n❌ Error: {str(e)}")

if __name__ == "__main__":
    main()