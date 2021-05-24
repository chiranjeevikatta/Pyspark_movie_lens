from pyspark.sql.functions import col,expr

# Read data from csv file
def _extract_data(spark,config):
    return(
        spark.read.format("csv")
        .option("header","true")
        .load(f"{config.get('source_data_path')}/movies.csv")
    )

# Transform raw dataframe
def __transform_data(raw_df):
    return raw_df.select(
        col("movieid"),
        expr("substring(title,1,length(title)-6)").alias("title"),
        col("title").substr(-5,4).alias("year")        
    )

# save data to parquet file
def __load_data(config,transformed_df):
    transformed_df.write.mode("overwrite").parquet(
        f"{config.get('output_data_path')}/movies_output"
    )

# run job
def run_job(spark,config):
    __load_data(config, __transform_data(_extract_data(spark,config)))