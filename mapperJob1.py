#!/usr/bin/env python3
import sys
import csv
from io import StringIO

def parse_csv(line):
    try:
        return next(csv.reader(StringIO(line)))
    except:
        return None

for line in sys.stdin:
    record = parse_csv(line)
    if not record or len(record) < 66:
        continue

    try:
        make = record[42].strip().upper()
        model = record[45].strip().upper()
        price = float(record[48])
        year = int(record[65])

        if not make or not model:
            continue

        key = f"{make}::{model}"  # USIAMO "::" come separatore sicuro
        value = f"{price},{year}"
        print(f"{key}\t{value}")
    except:
        continue
