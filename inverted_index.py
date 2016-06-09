import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
	key = record[0]
	value = record[1]
	words = value.split()
	for word in words:
		mr.emit_intermediate(word, key)

def reducer(key, list_of_value):
	value = []
	for item in list_of_value:
		if item not in value:
			value.append(item)
	mr.emit((key, value))

def main():
	inputdata = open(sys.argv[1])
	mr.execute(inputdata, mapper, reducer)

if __name__ == '__main__':
	main()