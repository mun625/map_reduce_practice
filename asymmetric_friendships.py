import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
	if record[0] < record[1]:
		key = record[0] + ',' + record[1]
	else:
		key = record[1] + ',' + record[0]
	value = 1
	mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
	if len(list_of_values) == 1:
		name1, name2 = key.split(',')
		mr.emit((name1,name2))
		mr.emit((name2,name1))

def main():
	inputdata = open(sys.argv[1])
	mr.execute(inputdata, mapper, reducer)

if __name__ == '__main__':
	main()