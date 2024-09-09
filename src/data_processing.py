from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_data(hdfs_path, processed_path):
    spark = SparkSession.builder.appName("Data Processing").getOrCreate()
    data = spark.read.csv(hdfs_path, header=True, inferSchema=True)
    
    # Example processing: filtering and transforming
    data = data.filter(col('interaction_type') == 'view')
    data = data.select('user_id', 'content_id', 'rating')
    
    data.write.mode('overwrite').parquet(processed_path)
    print(f"Processed data saved to: {processed_path}")

if __name__ == "__main__":
    hdfs_path = 'hdfs://user_interactions'
    processed_path = 'hdfs://processed/user_interactions'
    process_data(hdfs_path, processed_path)
