#!/usr/bin/env python3
import sys
from collections import defaultdict, Counter

# struttura dati: nested dictionary
data = defaultdict(lambda: {
    'bassa': {'count': 0, 'days': 0, 'words': Counter()},
    'media': {'count': 0, 'days': 0, 'words': Counter()},
    'alta':  {'count': 0, 'days': 0, 'words': Counter()}
})

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 5:
        continue
    city, year, fascia, tipo, valore = parts
    key = (city, year)
    if tipo == 'AUTO':
        data[key][fascia]['count'] += 1
        data[key][fascia]['days'] += int(valore)
    elif tipo == 'WORD':
        data[key][fascia]['words'][valore] += 1

# stampa intestazione
print("city,year,"
      "num_auto_bassa,avg_days_bassa,top3_term_bassa,"
      "num_auto_media,avg_days_media,top3_term_media,"
      "num_auto_alta,avg_days_alta,top3_term_alta")

for (city, year), fasce in sorted(data.items()):
    def fmt(fascia):
        cnt = fasce[fascia]['count']
        avg = round(fasce[fascia]['days'] / cnt, 1) if cnt > 0 else 0
        top3 = [w for w, _ in fasce[fascia]['words'].most_common(3)]
        return f"{cnt},{avg},{' '.join(top3)}"
    
    print(f"{city},{year},{fmt('bassa')},{fmt('media')},{fmt('alta')}")
