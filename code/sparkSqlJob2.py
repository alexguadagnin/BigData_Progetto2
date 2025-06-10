from pyspark.sql import SparkSession
from pyspark.sql.functions import when, split, explode, lower, regexp_replace, col

from config import DATASET_10K, DATASET_100K, DATASET_500K, DATASET_1M, DATASET_FULL, DATASET_FULL_X2, DATASET_FULL_X3, DATASET_FULL_X5


# 1. Inizializza SparkSession
spark = SparkSession.builder \
    .appName("Auto Report") \
    .master("local[*]") \
    .config("spark.driver.memory", "12g") \
    .getOrCreate()

# 2. Carica dataset
df = spark.read.csv(DATASET_FULL_X5, header=True, inferSchema=True)

# 3a. Statistiche base - fascia e giorni
df_stats = df.select("city", "year", "price", "daysonmarket") \
    .withColumn("fascia_prezzo", when(col("price") < 20000, "bassa")
                               .when((col("price") >= 20000) & (col("price") <= 50000), "media")
                               .otherwise("alta"))

df_stats.createOrReplaceTempView("auto_stats")

# 3b. Tokenizzazione descrizioni per parole
df_terms = df.select("city", "year", "price", "description") \
    .withColumn("fascia_prezzo", when(col("price") < 20000, "bassa")
                               .when((col("price") >= 20000) & (col("price") <= 50000), "media")
                               .otherwise("alta")) \
    .withColumn("term", explode(split(lower(regexp_replace("description", r"[^\w\s]", "")), r"\s+"))) \
    .filter("term != ''")

df_terms.createOrReplaceTempView("term_tokens")

# 4. Statistiche: conteggio auto e media giorni
spark.sql("""
    CREATE OR REPLACE TEMP VIEW fascia_stats AS
    SELECT
        city,
        year,
        fascia_prezzo,
        COUNT(*) AS num_auto,
        AVG(daysonmarket) AS avg_daysonmarket
    FROM auto_stats
    GROUP BY city, year, fascia_prezzo
""")

# 5. Frequenze parole
spark.sql("""
    CREATE OR REPLACE TEMP VIEW term_frequencies AS
    SELECT
        city,
        year,
        fascia_prezzo,
        term,
        COUNT(*) AS freq
    FROM term_tokens
    GROUP BY city, year, fascia_prezzo, term
""")

# 6. Top 3 parole
spark.sql("""
    CREATE OR REPLACE TEMP VIEW top_terms AS
    SELECT city, year, fascia_prezzo, term
    FROM (
        SELECT city, year, fascia_prezzo, term,
               ROW_NUMBER() OVER (PARTITION BY city, year, fascia_prezzo ORDER BY freq DESC) AS rn
        FROM term_frequencies
    ) t
    WHERE rn <= 3
""")

# 7. Aggrega le parole
spark.sql("""
    CREATE OR REPLACE TEMP VIEW top_terms_grouped AS
    SELECT city, year, fascia_prezzo,
           CONCAT_WS(', ', COLLECT_LIST(term)) AS top3_terms
    FROM top_terms
    GROUP BY city, year, fascia_prezzo
""")

# 8. Unione
spark.sql("""
    CREATE OR REPLACE TEMP VIEW full_stats AS
    SELECT s.city, s.year, s.fascia_prezzo,
           s.num_auto, s.avg_daysonmarket, t.top3_terms
    FROM fascia_stats s
    LEFT JOIN top_terms_grouped t
      ON s.city = t.city AND s.year = t.year AND s.fascia_prezzo = t.fascia_prezzo
""")

# 9. Pivot finale
final_df = spark.sql("""
    SELECT city, year,
           MAX(CASE WHEN fascia_prezzo = 'bassa' THEN num_auto END) AS num_auto_fascia_bassa,
           MAX(CASE WHEN fascia_prezzo = 'bassa' THEN avg_daysonmarket END) AS avg_daysonmarket_fascia_bassa,
           MAX(CASE WHEN fascia_prezzo = 'bassa' THEN top3_terms END) AS top3_term_fascia_bassa,

           MAX(CASE WHEN fascia_prezzo = 'media' THEN num_auto END) AS num_auto_fascia_media,
           MAX(CASE WHEN fascia_prezzo = 'media' THEN avg_daysonmarket END) AS avg_daysonmarket_fascia_media,
           MAX(CASE WHEN fascia_prezzo = 'media' THEN top3_terms END) AS top3_term_fascia_media,

           MAX(CASE WHEN fascia_prezzo = 'alta' THEN num_auto END) AS num_auto_fascia_alta,
           MAX(CASE WHEN fascia_prezzo = 'alta' THEN avg_daysonmarket END) AS avg_daysonmarket_fascia_alta,
           MAX(CASE WHEN fascia_prezzo = 'alta' THEN top3_terms END) AS top3_term_fascia_alta
    FROM full_stats
    GROUP BY city, year
""")

# 10. Mostra e salva
final_df.show(truncate=False)

final_df.write.mode("overwrite").csv("...", header=True)
