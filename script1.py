import dataset
import operators
import os

def calc(sex, percent, **row):
	if sex == 'M':
		return percent
	else:
		return percent**2

	
fs = [f for f in os.listdir('../data/test1') if f.endswith('.csv') ]
ds = dataset.Dataset('../data/test1/', fs, ['id', 'sex', 'age', 'percent'], [int, str, int, float])
print ds.files

op = operators.Transform(calc)
op(ds, n_procs=4)