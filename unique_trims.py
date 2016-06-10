import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
	key = record[1][0:len(record[1])-10]
	value = 1
	mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
	mr.emit(key)

def main():
	inputdata = open(sys.argv[1])
	mr.execute(inputdata, mapper, reducer)

if __name__ == '__main__':
	main()