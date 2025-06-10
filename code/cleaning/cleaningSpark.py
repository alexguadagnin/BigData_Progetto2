import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType, IntegerType
from config import RAW_DATASET, OUTPUT_FOLDER_DATASET, DATASET_10K, DATASET_100K, DATASET_500K, DATASET_1M, DATASET_FULL, DATASET_FULL_X2, DATASET_FULL_X3, DATASET_FULL_X5


# Crea la sessione Spark
spark = SparkSession.builder \
    .appName("Pulizia Dataset Auto Usate") \
    .getOrCreate()

# Leggi il CSV
df = spark.read.option("header", True).option("inferSchema", True).csv(RAW_DATASET)

# Cast dei campi numerici e rimozione righe con valori nulli o incoerenti
df_clean = df \
    .withColumn("price", col("price").cast(FloatType())) \
    .withColumn("horsepower", col("horsepower").cast(FloatType())) \
    .withColumn("engine_displacement", col("engine_displacement").cast(FloatType())) \
    .withColumn("year", col("year").cast(IntegerType())) \
    .filter(
        (col("make_name").isNotNull()) &
        (col("model_name").isNotNull()) &
        (col("price").isNotNull()) & (col("price") > 0) &
        (col("horsepower").isNotNull()) & (col("horsepower") > 0) &
        (col("engine_displacement").isNotNull()) & (col("engine_displacement") > 0) &
        (col("year").isNotNull())
    )

# Salva il dataset pulito completo
df_clean.write.mode("overwrite").csv(OUTPUT_FOLDER_DATASET + "cars_full", header=True)

# Salva versioni ridotte per test
df_clean.limit(10_000).write.mode("overwrite").csv(DATASET_10K, header=True)
df_clean.limit(100_000).write.mode("overwrite").csv(DATASET_100K, header=True)
df_clean.limit(500_000).write.mode("overwrite").csv(DATASET_500K, header=True)
df_clean.limit(1_000_000).write.mode("overwrite").csv(DATASET_1M, header=True)
df_clean.write.mode("overwrite").csv(DATASET_FULL, header=True)

# Genera dataset più grandi concatenando copie
df_clean.union(df_clean).write.mode("overwrite").csv(DATASET_FULL_X2, header=True)
df_clean.union(df_clean).union(df_clean).write.mode("overwrite").csv(DATASET_FULL_X3, header=True)
df_clean.union(df_clean).union(df_clean).union(df_clean).union(df_clean).write.mode("overwrite").csv(DATASET_FULL_X5, header=True)
