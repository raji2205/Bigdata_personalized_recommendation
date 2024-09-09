from pyspark.sql import SparkSession

def load_data_to_hdfs(local_path, hdfs_path):
    spark = SparkSession.builder.appName("Data Ingestion").getOrCreate()
    data = spark.read.csv(local_path, header=True, inferSchema=True)
    data.write.mode('overwrite').csv(hdfs_path)
    print(f"Data loaded to HDFS: {hdfs_path}")

if __name__ == "__main__":
    local_path = 'data/raw/user_interactions.csv'
    hdfs_path = 'hdfs://user_interactions'
    load_data_to_hdfs(local_path, hdfs_path)
