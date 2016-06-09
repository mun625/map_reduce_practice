import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
	key = record[1]
	value = record
	mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
	for item in list_of_values:
		if item[0] == 'order':
			for following_item in list_of_values:
				if following_item[0] == 'line_item':
					mr.emit(item + following_item)

def main():
	inputdata = open(sys.argv[1])
	mr.execute(inputdata, mapper, reducer)

if __name__ == '__main__':
	main()