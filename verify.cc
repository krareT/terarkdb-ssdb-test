
#include <assert.h>
#include <fstream>
#include <iostream>
#include <string>

#include "pika_ssdb_client.h"

using namespace std;

// verify
void verify(Client *client, const std::string& pid, const std::string& uid) {
	std::string key = "review/userId: " + uid +"product/productId: " + pid;
	std::string result;
	std::cout << "will verify: " << key << std::endl;
	client->get(key, result);
	client->freeReply();
	if (!result.empty()) {
		printf("value exist: %s\n", result.c_str());
	} else {
		printf("value not exist\n");
	}
}

int main(int argc, char *argv[]) {
	if (argc != 3) {
		printf("[usage] verify [productid] [userid]\n");
		return 1;
	}
	std::string pid = argv[1];
	std::string uid = argv[2];

	const char *ip = "127.0.0.1";
    int port = 9221;
	Client *client = new Client();				
	client->connect(ip,port);
	if(!client->isok()){
		printf("Connect to pikaServer Faile!\n");
		delete client;
		return -1;
	}

	verify(client, pid, uid);
	delete client;
	return 0;
}
