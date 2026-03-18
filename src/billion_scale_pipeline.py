"""
Billion-Scale Data Pipeline with Dask
TRUE distributed computing - processes 1 billion rows efficiently
"""
import dask
import dask.dataframe as dd
import dask.array as da
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
    """Process billions of rows using TRUE distributed computing"""
    
    def __init__(self):
        self.data = None
        logger.info("="*60)
        logger.info("BILLION-SCALE PIPELINE INITIALIZED")
        logger.info("="*60)
    
    def generate_billion_rows_distributed(self, n_rows=1_000_000_000, partitions=100):
        """
        Generate billions of rows using Dask's distributed capabilities
        This creates data in chunks without loading everything into memory
        """
        logger.info(f"Generating {n_rows:,} rows with {partitions} partitions...")
        logger.info(f"Each partition: {n_rows//partitions:,} rows")
        
        # Calculate rows per partition
        rows_per_partition = n_rows // partitions
        
        # Create a list of delayed tasks
        import dask
        from dask.delayed import delayed
        
        @delayed
        def create_partition(partition_id, n_rows_part):
            """Create a single partition - runs in parallel"""
            np.random.seed(42 + partition_id)  # Different seed for each partition
            
            return pd.DataFrame({
                'timestamp': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * n_rows_part,
                'user_id': np.random.randint(1000, 9999, n_rows_part),
                'action': np.random.choice(['click', 'view', 'purchase', 'login'], n_rows_part),
                'value': np.random.exponential(100, n_rows_part),
                'location': np.random.choice(['US', 'UK', 'IN', 'DE', 'JP'], n_rows_part),
                'device': np.random.choice(['mobile', 'desktop', 'tablet'], n_rows_part)
            })
        
        # Create all partitions as delayed objects
        logger.info("Creating distributed partitions...")
        partitions_list = [create_partition(i, rows_per_partition) for i in range(partitions)]
        
        # Convert to Dask DataFrame
        self.data = dd.from_delayed(partitions_list)
        
        logger.info(f"✅ Created Dask DataFrame with {self.data.npartitions} partitions")
        logger.info(f"   Total rows: {n_rows:,}")
        logger.info(f"   Memory: Distributed across partitions (not loaded yet)")
        return self
    
    def analyze_billions(self):
        """Perform analysis on billions of rows"""
        logger.info("Performing billion-scale analysis...")
        
        # Compute basic statistics (lazy evaluation - no memory load yet)
        logger.info("  Computing statistics (this will trigger distributed computation)...")
        
        # Trigger computation in a memory-efficient way
        with dask.config.set(scheduler='threads'):  # Use threads for local computation
            # Compute statistics
            total_rows = len(self.data)
            unique_users = self.data['user_id'].nunique().compute()
            avg_value = float(self.data['value'].mean().compute())
            std_value = float(self.data['value'].std().compute())
            total_value = float(self.data['value'].sum().compute())
        
        self.stats = {
            'total_rows': int(total_rows),
            'unique_users': int(unique_users),
            'avg_value': round(avg_value, 2),
            'std_value': round(std_value, 2),
            'total_value': round(total_value, 2)
        }
        
        # Group by operations (done in distributed manner)
        logger.info("  Computing aggregations by location...")
        by_location = self.data.groupby('location')['value'].agg(['count', 'mean', 'sum']).compute()
        
        logger.info("  Computing aggregations by action...")
        by_action = self.data.groupby('action')['value'].agg(['count', 'mean', 'sum']).compute()
        
        logger.info("  Computing aggregations by device...")
        by_device = self.data.groupby('device')['value'].agg(['count', 'mean', 'sum']).compute()
        
        # Convert to dictionaries (now the results are small)
        self.aggregates = {
            'by_location': by_location.round(2).to_dict(),
            'by_action': by_action.round(2).to_dict(),
            'by_device': by_device.round(2).to_dict()
        }
        
        logger.info("✅ Analysis complete!")
        return self.stats, self.aggregates
    
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
                if isinstance(data, dict):
                    f.write(f"{loc}: {data.get('count', 0)} transactions, ${data.get('mean', 0):.2f} avg\n")
        
        logger.info(f"✅ Summary saved to billion_output/")
    
    def progressive_scale_test(self):
        """Test different scale levels progressively"""
        scales = [1_000_000, 10_000_000, 100_000_000, 500_000_000, 1_000_000_000]
        
        print("\n" + "="*60)
        print("PROGRESSIVE SCALE TEST")
        print("="*60)
        
        for scale in scales:
            print(f"\n📊 Testing {scale:,} rows...")
            import time
            start = time.time()
            
            try:
                # Calculate appropriate partitions
                partitions = max(10, scale // 10_000_000)
                
                # Generate at this scale
                self.generate_billion_rows_distributed(n_rows=scale, partitions=partitions)
                
                # Just compute row count to verify
                row_count = len(self.data).compute()
                end = time.time()
                
                print(f"   ✅ Success! Processed {row_count:,} rows")
                print(f"   ⏱️  Time: {(end-start):.2f} seconds")
                print(f"   📈 Partitions: {self.data.npartitions}")
                
            except Exception as e:
                print(f"   ❌ Failed at {scale:,}: {str(e)[:100]}")
                break

def main():
    """Main execution"""
    print("\n" + "="*60)
    print("BILLION-SCALE PIPELINE - DISTRIBUTED MODE")
    print("="*60)
    
    pipeline = BillionScalePipeline()
    
    # Choose your scale:
    # rows_to_test = 10_000_000    # 10 million (safe)
    # rows_to_test = 100_000_000   # 100 million (requires good RAM)
    rows_to_test = 1_000_000_000   # 1 billion (TRUE distributed)
    
    # Number of partitions - more partitions = less memory per partition
    partitions = 200  # Each partition will have 5 million rows
    
    try:
        # Generate data using distributed method
        pipeline.generate_billion_rows_distributed(
            n_rows=rows_to_test, 
            partitions=partitions
        )
        
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
        print(f"Partitions Used: {partitions}")
        print("\nFiles saved in 'billion_output/' folder")
        print("="*60)
        
        # Run progressive scale test
        pipeline.progressive_scale_test()
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
        print("\n💡 Tip: Try with more partitions or fewer rows")
        print("   Example: partitions = 500 for 1B rows")

if __name__ == "__main__":
    main()