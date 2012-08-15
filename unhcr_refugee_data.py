import csv, sys
from itertools import groupby

# Goal is to group data by country and get sums for 2008, 2009, and 2008 + 2009

# Read from argument list
# For this test, use inFile = unhcr_0809.csv
inFile = "data/unhcr_refugees_original.csv"
outFile = "data/unhcr_refugees_processed.csv"

# Open unhcr_0809.csv - header = asylum_ctry, origin_ctry, year, total
unhcr = csv.DictReader(open(inFile, 'rb'), delimiter= ',', quotechar = '"')

# filter out bad data
unhcr_filter = filter(lambda x: x['total'] != '', unhcr)

# the itertools functions need data sorted by key, so first sort by country
unhcr_sort = sorted(unhcr_filter, key=lambda x: x['origin_ctry'])

# Determine years
years = []
for row in unhcr_sort:
    if row['year'] not in years:
        years.append(row['year'])
years.sort()

# then group by country with itertools
header = ['country']
header = header + years
header.append('all')
rows = []
first = True
for country, values in groupby(unhcr_sort, lambda x: x['origin_ctry']):
    # now group by year
    row = [country]
    yearVals = {}
    for y in years:
        yearVals[y] = ''
    for year, v in groupby(sorted(values, key=lambda x: x['year']), lambda x: x['year']):
        yearVals[year] = sum(map(lambda x: int(x['total']), v))
    # get the sum of all elements after the first one (the country name)
    # in this row
    for y in years:
        row.append(yearVals[y])
    total = 0
    for v in yearVals.values():
        if v != '':
            total = total + v
    row.append(total)
    rows.append(row)

output = open(outFile, 'w')
writer = csv.writer(output)
writer.writerow(header)
for r in rows:
    writer.writerow(r)
output.close()
