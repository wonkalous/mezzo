import os
import hashlib
import multiprocessing
import csv
import math
import numpy




class MapQueue(object):
	def __init__(self, ds, n_procs=8):
		self.ds = ds
		self.q = multiprocessing.Queue()
		self.n_procs = n_procs
		
		valid_path = False
		while not valid_path:
			qid = int(1e10 * numpy.random.random())
			self.dir_name = 'mtemp%s' % qid
			valid_path = not os.path.exists(self.dir_name)
		print self.dir_name
		os.makedirs(self.dir_name)

	def get_runner(self, dir_name):
		def do_job(q):
			input_file, func = q.get()
			reader = csv.reader(open(self.ds.abs_path+input_file))
			for row in reader:
				row = enforce_types(self.ds.types, row)
				args = []
				for arg_name in func.func_code.co_varnames:
					args.append(row[self.ds.header.index(arg_name)])
				result = func(*args)

				dest_key = str(hash(result))[:int(numpy.ceil(math.log10(len(self.ds.files))))]
				dest = '%s/res_%s.csv' % (dir_name, dest_key)
				with open(dest, "a") as f:
					writer = csv.writer(f)
					writer.writerow(row + [result])

		return do_job


	def add(self, job):
		self.q.put(job)

	def start(self):
		for n in xrange(self.n_procs):
			proc = multiprocessing.Process(target=self.get_runner(self.dir_name), args=(self.q, ))
			proc.start()
		proc.join()


def enforce_types(types, row):
	for p, t in enumerate(types):
		row[p] = t(row[p])
	return row


class Dataset(object):
	def __init__(self, path, files, header, types):
		self.files = files
		self.header = header
		self.types = types
		self.abs_path = path

	def do_map(self, map_func, n_procs=1):
		mq = MapQueue(self, n_procs=n_procs)
		for file_name in self.files:
			mq.add((file_name, map_func))
		mq.start()

