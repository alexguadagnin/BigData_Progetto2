from pyspark import SparkContext, SparkConf
import re
from collections import Counter
import csv
from io import StringIO
import os
import shutil

from config import DATASET_10K, DATASET_100K, DATASET_500K, DATASET_1M, DATASET_FULL, DATASET_FULL_X2, DATASET_FULL_X3, DATASET_FULL_X5


# 1. Inizializza Spark
conf = SparkConf().setAppName("Auto Report - Spark Core").setMaster("local[*]")
sc = SparkContext(conf=conf)

# 2. Funzione per parsing CSV che gestisce campi con virgole
def parse_csv(line):
    return list(csv.reader(StringIO(line)))[0]

# 3. Carica dataset
raw_rdd = sc.textFile(f"{DATASET_FULL_X5}/*.csv")
data_rdd = raw_rdd.zipWithIndex().filter(lambda x: x[1] > 0).keys().map(parse_csv)

# 4. Estrai campi utili
def parse_line(parts):
    try:
        city = parts[7]
        year = int(parts[65])
        price = float(parts[48])
        days = int(parts[10])
        desc = parts[12].lower()
        return (city, year, price, days, desc)
    except:
        return None

parsed_raw = data_rdd.map(parse_line)
parsed = parsed_raw.filter(lambda x: x is not None)
print("Totale righe valide Spark Core:", parsed.count())
print("Totale righe scartate:", parsed_raw.count() - parsed.count())

# 5. Assegna fascia di prezzo
def get_fascia(price):
    if price < 20000:
        return "bassa"
    elif price <= 50000:
        return "media"
    else:
        return "alta"

rdd = parsed.map(lambda x: ((x[0], x[1], get_fascia(x[2])), (1, x[3], x[4])))

# 6. Statistiche: numero e giorni medi
stats = rdd.mapValues(lambda v: (v[0], v[1])) \
           .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# 7. Conteggio parole
def word_count_per_group(record):
    key, (count, days, desc) = record
    desc_clean = re.sub(r"[^\w\s]", "", desc)
    words = desc_clean.strip().split()
    return [((key, word), 1) for word in words if word]

word_pairs = rdd.flatMap(word_count_per_group)
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

grouped = word_counts.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                     .groupByKey() \
                     .mapValues(lambda vals: [w for w, _ in sorted(vals, key=lambda x: -x[1])[:3]]) \
                     .mapValues(lambda top3: ", ".join(top3))

# 8. Join tra stats e parole
final = stats.join(grouped)  # ((city, year, fascia), ((count, sum_days), top3words))

# 9. Riformattazione per output
def format_record(kv):
    (city, year, fascia) = kv[0]
    (count, sum_days), top3 = kv[1]
    avg_days = round(sum_days / count, 2)
    return ((city, year), (fascia, count, avg_days, top3))

records = final.map(format_record).groupByKey()

def to_output(row):
    (city, year), values = row
    out = {
        "bassa": ("", "", ""),
        "media": ("", "", ""),
        "alta": ("", "", "")
    }
    for fascia, count, avg, words in values:
        out[fascia] = (count, avg, words)
    return (city, year,
            out["bassa"][0], out["bassa"][1], out["bassa"][2],
            out["media"][0], out["media"][1], out["media"][2],
            out["alta"][0], out["alta"][1], out["alta"][2])

def sanitize(x):
    return [str(s).replace(",", " ") if i in [4, 7, 10] else str(s) for i, s in enumerate(x)]

final_result = records.map(to_output)

# 10. Scrittura
output_path = "..."
if os.path.exists(output_path):
    shutil.rmtree(output_path)

header = "city,year,num_auto_fascia_bassa,avg_days_fascia_bassa,top3_term_fascia_bassa," \
         "num_auto_fascia_media,avg_days_fascia_media,top3_term_fascia_media," \
         "num_auto_fascia_alta,avg_days_fascia_alta,top3_term_fascia_alta"

sc.parallelize([header]).union(final_result.map(lambda x: ",".join(sanitize(x)))) \
  .saveAsTextFile(output_path)

sc.stop()
