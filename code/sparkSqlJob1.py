from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, avg, countDistinct, collect_set, count
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.functions import sum as spark_sum

from config import DATASET_10K, DATASET_100K, DATASET_500K, DATASET_1M, DATASET_FULL, DATASET_FULL_X2, DATASET_FULL_X3, DATASET_FULL_X5 

# Path output
output_path = "..."

# Inizializza Spark
spark = SparkSession.builder.appName("Job1_Marche_Modelli").getOrCreate()

# Carica il dataset pulito
df = spark.read.option("header", True).option("inferSchema", True).csv(DATASET_FULL_X5)

# Filtra solo le colonne che servono
df_filtered = df.select("make_name", "model_name", "price", "year")
print("Totale righe valide Spark Core:", df_filtered.count())

# Rimuove valori nulli residui
df_filtered = df_filtered.dropna(subset=["make_name", "model_name", "price", "year"])
print("Totale righe valide Spark Core:", df_filtered.count())

# Raggruppa per marca e modello
df_grouped = df_filtered.groupBy("make_name", "model_name").agg(
    count("*").alias("count"),
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    avg("price").alias("avg_price"),
    collect_set("year").alias("years_list")
)

# Ordina opzionalmente per marca e modello
df_grouped = df_grouped.orderBy("make_name", "model_name")

# Comverte la lista in stringa
df_grouped = df_grouped.withColumn("years_list", concat_ws(";", col("years_list")))

# Test su auto trovate
df_grouped.agg(spark_sum("count").alias("total_vehicles")).show()

# Salva su disco
df_grouped.write.mode("overwrite").option("header", True).csv(output_path)
