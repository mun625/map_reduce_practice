import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
	key = record[0]
	value = 1
	mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
	total = 0
	for item in list_of_values:
		total += 1
	mr.emit((key,total))

def main():
	inputdata = open(sys.argv[1])
	mr.execute(inputdata, mapper, reducer)

if __name__ == '__main__':
	main()