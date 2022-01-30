import csv
import json

csvfile = open('hive_final.csv', 'r')
jsonfile = open('hive_final.json', 'w')

fieldnames = ("month","borough","people_sum","rank")
reader = csv.DictReader( csvfile, fieldnames)
for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write('\n')