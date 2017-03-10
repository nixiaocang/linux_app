import csv

csvfile = file('./faker500.csv', 'w')
writer = csv.writer(csvfile)
value = "dlfjdoodjfkdlkj"
for row in range(6600000):
    record = []
    for col in xrange(10):
        record.append(value)
    writer.writerow(record)
csvfile.close()    
