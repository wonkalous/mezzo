import csv
import numpy
import os


root_name = "test1"
n_files = 50
rows_mean = 1000  # aim for 100mb
header = [
	('id', lambda: int(numpy.random.random()*1e7)),
	('sex', lambda: numpy.random.choice(['M', 'F'])),
	('age', lambda: int(numpy.random.random()*1e2)),
	('percent', lambda: numpy.random.random()),

]

os.makedirs('../data/'+root_name)

for n in xrange(n_files):
	f = open('../data/%s/%s__%s.csv' % (root_name, root_name, n), 'w+')
	writer = csv.writer(f)
	for i in xrange(int(numpy.random.normal(rows_mean, 100))):
		row = []
		for name, gen in header:
			row.append(gen())
		if numpy.random.rand()>.99:
			print row	
		writer.writerow(row)
	f.close()