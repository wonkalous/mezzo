import os
import hashlib
import multiprocessing
import csv
import math
import numpy




class MapQueue(object):
	def __init__(self, func, n_procs=8):
		self.func = self.make_worker(func)
		self.read_q = multiprocessing.Queue()
		self.write_q = multiprocessing.Queue()
		self.n_procs = n_procs
		
		valid_path = False
		while not valid_path:
			qid = int(1e10 * numpy.random.random())
			self.dir_name = 'mtemp%s' % qid
			valid_path = not os.path.exists(self.dir_name)
		print self.dir_name
		os.makedirs(self.dir_name)

	def make_worker(self, func):
		def worker(read_q, write_q):
			while True:
				args = read_q.get()
				for to_write in func(*args):
					# print to_write
					write_q.put(to_write)
		return worker

	def add(self, job):
		self.read_q.put(job)

	def writer(self, write_q):
		self.fs = {}
		while True:
			row, result = write_q.get(False)
			dest_key = str(hash(result))[:4]
			dest = '%s/res_%s.csv' % (self.dir_name, dest_key)
			# f = self.fs.get(dest, open(dest, "a"))
			with open(dest, "a") as f:
				writer = csv.writer(f)
				writer.writerow(row + [result])
				# write_q.task_done()
				print 'write'
				# print row, result, 'written'
		

	def start(self):
		for n in xrange(self.n_procs-1):
			proc = multiprocessing.Process(target=self.func, args=(self.read_q, self.write_q))
			proc.start()
		print 'start writer'
		proc = multiprocessing.Process(target=self.writer, args=(self.write_q, ))
		proc.start()
		
		# self.write_q.join()
		for f in self.fs.values: 
			f.close()


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

		self._line_indices = {}
		self._file_lens = {}

		self._indexed = False

	@property
	def line_indices(self):
		if not self._indexed:
			self._index_lines()
		return self._line_indices

	@property
	def file_lens(self):
		if not self._indexed:
			self._index_lines()
		return self._file_lens

	def _index_lines(self):
		if self._indexed:
			return
		print 'indexing input files'
		self._indexed = True
		for file_name in self.files:
			print '    ', file_name
			f = open(self.abs_path + file_name)
			self._line_indices[file_name] = []

			position = 0
			for i, line in enumerate(f):
				self._line_indices[file_name].append(position)
				position += len(line) 
			self._file_lens[file_name] = i

	def do_map(self, map_func, n_procs=1, job_size_lines=100000):
		mq = MapQueue(self.map_job, n_procs=n_procs)
		for file_name in self.files:
			n_jobs = self.file_lens[file_name] / job_size_lines + \
			bool(self.file_lens[file_name] % job_size_lines)
			for i in xrange(n_jobs):
				start_line = i*job_size_lines
				start = self.line_indices[file_name][start_line]
				mq.add((file_name, start, job_size_lines, map_func))
		mq.start()

	def map_job(self, input_file, start, n_lines, func):
		f = open(self.abs_path+input_file)
		f.seek(start)
		print 'read file'
		reader = csv.reader(f)
		for i, row in enumerate(reader):
			print 'read', start, i
			if i == n_lines:
				print 'break'
				break
			row = enforce_types(self.types, row)
			args = []
			for arg_name in func.func_code.co_varnames:
				args.append(row[self.header.index(arg_name)])
			result = func(*args)
			yield row, result













