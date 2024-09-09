from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

def collaborative_filtering(processed_path):
    spark = SparkSession.builder.appName("Collaborative Filtering").getOrCreate()
    data = spark.read.parquet(processed_path)
    
    # ALS model for collaborative filtering
    als = ALS(userCol='user_id', itemCol='content_id', ratingCol='rating', coldStartStrategy='drop')
    model = als.fit(data)
    
    predictions = model.transform(data)
    
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print(f"Collaborative Filtering RMSE: {rmse}")

    return model

def content_based_filtering(processed_path):
    spark = SparkSession.builder.appName("Content-Based Filtering").getOrCreate()
    data = spark.read.parquet(processed_path)
    
    # Example of a simple content-based filtering algorithm (Placeholder)
    # You can expand this based on specific content features
    content_features = data.select('content_id', 'feature_vector')  # Assume feature_vector exists
    
    user_profiles = data.groupBy('user_id').agg({'rating': 'mean'}).alias('user_profile')
    recommendations = content_features.join(user_profiles, 'content_id', 'inner')

    recommendations.show()
    return recommendations

if __name__ == "__main__":
    processed_path = 'hdfs://processed/user_interactions'
    collaborative_model = collaborative_filtering(processed_path)
    content_recommendations = content_based_filtering(processed_path)
