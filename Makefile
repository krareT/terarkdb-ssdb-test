CXX=g++
OBJECTS=main.o ssdb_test.o libssdb-client.a

ssdb-benchmark: ${OBJECTS}
	${CXX} -std=c++1y -o ssdb-benchmark ${OBJECTS}

main.o: main.cpp ssdb_test.hpp
	${CXX} -std=c++1y -o main.o -c main.cpp

ssdb_test.o: ssdb_test.cpp ssdb_test.hpp SSDB_client.h
	${CXX} -std=c++1y -o ssdb_test.o -c ssdb_test.cpp

clean:
	rm -rf *.o
