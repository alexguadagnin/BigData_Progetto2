#!/usr/bin/env python3
import sys
import csv
import re
from io import StringIO

for line in sys.stdin:
    try:
        reader = csv.reader(StringIO(line))
        row = next(reader)
        city = row[7]
        year = row[65]
        price = float(row[48])
        days = int(row[10])
        desc = row[12].lower()
        fascia = "bassa" if price < 20000 else "media" if price <= 50000 else "alta"
        print(f"{city}\t{year}\t{fascia}\tAUTO\t{days}")
        for word in re.findall(r'\w+', desc):
            print(f"{city}\t{year}\t{fascia}\tWORD\t{word}")
    except Exception:
        continue
