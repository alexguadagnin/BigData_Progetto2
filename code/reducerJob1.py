#!/usr/bin/env python3
import sys

current_key = None
count = 0
price_sum = 0
min_price = float('inf')
max_price = float('-inf')
years = set()

def emit(key):
    if count == 0:
        return
    avg_price = round(price_sum / count, 2)
    years_str = "|".join(str(y) for y in sorted(years))
    print(f"{key}\t{count}\t{min_price}\t{max_price}\t{avg_price}\t{years_str}")

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t')
    if len(parts) != 2:
        continue

    key_parts = parts[0].split('::')
    if len(key_parts) != 2:
        continue

    make = key_parts[0].strip().upper()
    model = key_parts[1].strip().upper()
    key = f"{make}::{model}"

    try:
        price_str, year_str = parts[1].split(',')
        price = float(price_str)
        year = int(year_str)
    except:
        continue

    if current_key != key:
        if current_key is not None:
            emit(current_key)
        current_key = key
        count = 0
        price_sum = 0
        min_price = float('inf')
        max_price = float('-inf')
        years = set()

    count += 1
    price_sum += price
    min_price = min(min_price, price)
    max_price = max(max_price, price)
    years.add(year)

# Emit last key
emit(current_key)
