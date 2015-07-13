import csv
import numpy
import os


root_name = "test_large"
n_files = 2
rows_mean = 2000000  # aim for 100mb
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
		# if numpy.random.rand()>.9999:
		# 	print row	
		if not i%10000:
			print i
		writer.writerow(row)
	f.close()