import csv
from pyspark import SparkContext
from io import StringIO

from config import DATASET_10K, DATASET_100K, DATASET_500K, DATASET_1M, DATASET_FULL, DATASET_FULL_X2, DATASET_FULL_X3, DATASET_FULL_X5


sc = SparkContext(appName="Job1_SparkCore")

# Funzione per il parsing CSV (gestisce anche virgole nei campi)
def parse_csv(line):
    return list(csv.reader(StringIO(line)))[0]

# Leggi file e rimuovi header
raw_rdd = sc.textFile(DATASET_FULL_X5)
header = raw_rdd.first()
print(header.split(","))
data_rdd = raw_rdd.filter(lambda x: x != header).map(parse_csv)

# Colonne interessate: make_name (42), model_name (45), price (48), year (65)
def extract_fields(record):
    try:
        make = record[42]
        model = record[45]
        price = float(record[48])
        year = int(record[65])
        return ((make, model), (1, price, price, price, set([year])))  # count, min, max, sum, anni
    except Exception as e:
        return None

parsed_rdd = data_rdd.map(extract_fields).filter(lambda x: x is not None)
print("Totale righe valide Spark Core:", parsed_rdd.count())

# Aggrega per (make, model)
def reduce_values(v1, v2):
    count = v1[0] + v2[0]
    min_price = min(v1[1], v2[1])
    max_price = max(v1[2], v2[2])
    sum_price = v1[3] + v2[3]
    years = v1[4].union(v2[4])
    return (count, min_price, max_price, sum_price, years)

agg_rdd = parsed_rdd.reduceByKey(reduce_values)
print("Totale righe valide Spark Core:", parsed_rdd.count())

# Calcola la media del prezzo
result_rdd = agg_rdd.map(lambda x: {
    "make": x[0][0],
    "model": x[0][1],
    "count": x[1][0],
    "min_price": x[1][1],
    "max_price": x[1][2],
    "avg_price": round(x[1][3] / x[1][0], 2),
    "years": sorted(list(x[1][4]))
})
    
# Salva su file (JSON o CSV se vuoi)
result_rdd \
    .map(lambda x: f"{x['make']},{x['model']},{x['count']},{x['min_price']},{x['max_price']},{x['avg_price']},{'|'.join(map(str, x['years']))}") \
    .saveAsTextFile("...")
