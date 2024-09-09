from pyspark.ml.evaluation import RegressionEvaluator

def evaluate_model(predictions):
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    print(f"Model RMSE: {rmse}")

if __name__ == "__main__":
    # Placeholder for actual predictions DataFrame
    predictions = None  # Replace with actual predictions DataFrame from the recommendation engine
    evaluate_model(predictions)
