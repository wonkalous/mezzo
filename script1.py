import dataset
import operators
import os

def calc(sex, percent):
	if sex == 'M':
		return percent
	else:
		return percent**2

	
fs = [f for f in os.listdir('../data/test_large') if f.endswith('.csv') ]
ds = dataset.Dataset('../data/test_large/', fs, 
	['id', 'sex', 'age', 'percent'], [int, str, int, float])

# todo path not needed, infer
print ds.files

op = operators.Transform(calc)
op(ds, n_procs=20)