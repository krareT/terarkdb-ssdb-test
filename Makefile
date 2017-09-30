CXX=g++
#CXXFLAGS=-O2
CXXFLAGS=-g -std=c++11
OBJECTS= main.cpp pika_ssdb_client.cpp pika_ssdb_test.cpp libhiredis.a

ssdb-benchmark: ${OBJECTS}
	${CXX} -g -std=c++1y -o ssdb-benchmark ${OBJECTS} -lpthread 

verify:
	${CXX} ${CXXFLAGS} -o verify verify.cc pika_ssdb_client.cpp libhiredis.a

clean:
	rm -rf *.o
