import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, upper, coalesce, lit
from awsglue.dynamicframe import DynamicFrame

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# --- Define S3 Paths ---
s3_input_path = "s3://handsonfinallanding/"
s3_processed_path = "s3://handsonfinalprocessed/processed-data/"
s3_analytics_path = "s3://handsonfinalprocessed/Athena Results/"

# --- Read the data from S3 ---
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_input_path], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "inferSchema": True},
)

# Convert to DataFrame
df = dynamic_frame.toDF()

# --- Data Cleaning ---
df_transformed = df.withColumn("rating", coalesce(col("rating").cast("integer"), lit(0)))
df_transformed = df_transformed.withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))
df_transformed = df_transformed.withColumn("review_text", coalesce(col("review_text"), lit("No review text")))
df_transformed = df_transformed.withColumn("customer_id", coalesce(col("customer_id"), lit("unknown")))
df_transformed = df_transformed.withColumn("product_id", upper(col("product_id")))

# --- Save cleaned data ---
glue_processed_frame = DynamicFrame.fromDF(df_transformed, glueContext, "transformed_df")
glueContext.write_dynamic_frame.from_options(
    frame=glue_processed_frame,
    connection_type="s3",
    connection_options={"path": s3_processed_path},
    format="csv"
)

# Create temp view for SQL
df_transformed.createOrReplaceTempView("product_reviews")

# --- QUERY 1: Product Performance ---
df_query1 = spark.sql("""
    SELECT 
        product_id, 
        ROUND(AVG(rating), 2) as average_rating,
        COUNT(*) as review_count
    FROM product_reviews
    GROUP BY product_id
    ORDER BY average_rating DESC
""")
query1_frame = DynamicFrame.fromDF(df_query1.repartition(1), glueContext, "query1")
glueContext.write_dynamic_frame.from_options(
    frame=query1_frame,
    connection_type="s3",
    connection_options={"path": f"{s3_analytics_path}product_performance/"},
    format="csv"
)

# --- QUERY 2: Daily Review Count ---
df_query2 = spark.sql("""
    SELECT 
        review_date,
        COUNT(*) as total_reviews
    FROM product_reviews
    WHERE review_date IS NOT NULL
    GROUP BY review_date
    ORDER BY review_date ASC
""")
query2_frame = DynamicFrame.fromDF(df_query2.repartition(1), glueContext, "query2")
glueContext.write_dynamic_frame.from_options(
    frame=query2_frame,
    connection_type="s3",
    connection_options={"path": f"{s3_analytics_path}daily_review_counts/"},
    format="csv"
)

# --- QUERY 3: Top 5 Customers ---
df_query3 = spark.sql("""
    SELECT 
        customer_id,
        COUNT(*) as total_reviews
    FROM product_reviews
    WHERE customer_id != 'unknown'
    GROUP BY customer_id
    ORDER BY total_reviews DESC
    LIMIT 5
""")
query3_frame = DynamicFrame.fromDF(df_query3.repartition(1), glueContext, "query3")
glueContext.write_dynamic_frame.from_options(
    frame=query3_frame,
    connection_type="s3",
    connection_options={"path": f"{s3_analytics_path}top_5_customers/"},
    format="csv"
)

# --- QUERY 4: Rating Distribution ---
df_query4 = spark.sql("""
    SELECT 
        rating,
        COUNT(*) as review_count
    FROM product_reviews
    GROUP BY rating
    ORDER BY rating ASC
""")
query4_frame = DynamicFrame.fromDF(df_query4.repartition(1), glueContext, "query4")
glueContext.write_dynamic_frame.from_options(
    frame=query4_frame,
    connection_type="s3",
    connection_options={"path": f"{s3_analytics_path}rating_distribution/"},
    format="csv"
)

job.commit()
print("All queries completed successfully!")
