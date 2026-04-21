from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round

def create_spark_session():
    return SparkSession.builder \
        .appName("FinancialDataPipeline") \
        .getOrCreate()

def transform_bronze_to_silver(spark, input_path, output_path):
    """Clean and transform raw financial data."""
    df = spark.read.parquet(input_path)

    df_clean = df \
        .dropDuplicates() \
        .filter(col("amount").isNotNull()) \
        .withColumn("transaction_date", to_date(col("timestamp"))) \
        .withColumn("amount_usd", round(col("amount"), 2)) \
        .select("transaction_id", "transaction_date", 
                "amount_usd", "category", "account_id")

    df_clean.write.mode("overwrite").parquet(output_path)
    print(f"Transformed {df_clean.count()} records to Silver layer.")

if __name__ == "__main__":
    spark = create_spark_session()
    transform_bronze_to_silver(
        spark,
        input_path="s3://my-bucket/bronze/financial/",
        output_path="s3://my-bucket/silver/financial/"
    )
