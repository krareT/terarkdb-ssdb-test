
#include <assert.h>
#include <fstream>
#include <iostream>
#include <string>
using namespace std;

// to extract info from file
void extract(const std::string& filename, const std::string& pid) {
    std::ifstream infile(filename.c_str());
    assert(infile);
    std::string line;
	std::string pid_tag = "product/productId: " + pid;
		//rid_tag = "review/userId: " + rid;
	int trigger = 0;
    while (std::getline(infile, line)) {
        if (line.find(pid_tag) != std::string::npos) {
			trigger = 1;
			cout << line << std::endl;
		} else if (trigger > 0) {
			if (trigger == 1) {
				cout << line << std::endl;
			} else if (trigger == 7) {
				trigger = 0;
				cout << line << std::endl;
				printf("\n\n");
				continue;
			} 
			trigger ++;
		}
	}
}

int main(int argc, char *argv[]) {
	if (argc != 2) {
		printf("[usage] extract [productid]\n");
		return 1;
	}
	std::string fname = "/data/publicdata/movies/movies.txt";
	std::string pid = argv[1];
	extract(fname, pid);
	return 0;
}
