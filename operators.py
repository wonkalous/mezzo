


class Operator(object):
	def __init__(self):
		self.plan = []  # [(map_func, reduce_func)]


	def __rshift__(self): pass

	def __call__(self, dataset): pass



class Transform(Operator):
	def __init__(self, mapfunc, foreach=None):
		# super(self, Transform).__init__()
		self.mapfunc = mapfunc
		self.foreach = foreach

	def __call__(self, dataset, n_procs=1):
		dataset.do_map(self.mapfunc, n_procs=n_procs)


