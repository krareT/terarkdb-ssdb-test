CXX=g++
OBJECTS= main.cpp pika_ssdb_client.cpp pika_ssdb_test.cpp libhiredis.a

ssdb-benchmark: ${OBJECTS}
	${CXX} -g -std=c++1y -o ssdb-benchmark ${OBJECTS} 
clean:
	rm -rf *.o
