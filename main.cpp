//
//  main.cpp
//  ssdb-test
//
//  Created by Roy Guo on 27/5/59 BE.
//  Modified by wangshujing on 13/07/17 BE.
//  Copyright © 2559 BE terark. All rights reserved.
//

#include <iostream>
#include <cstring>
#include <stdlib.h>
#include "pika_ssdb_client.h"
#include "pika_ssdb_test.h"
int main(int argc, const char** argv) {
    const char* command = argv[1];
    const char* filepath = argv[2];
    if(command == NULL || filepath == NULL) {
        printf("usage : ./ssdb-benchmark [mset|msettest|mget|delete|expire] [movie_file_path]\n");
        printf("mget env : SSDB_PORT(default 9221) | MGET_AMOUNT(default 70000) | MGET_SIZE(default 100) | DEL_AMOUNT(default 7000000) | EXPIRE_AMOUNT(default 7000000)\n");
        exit(0);
    }

    const char *ip = "127.0.0.1";
    int port = 9221;
    int mget_amount = 70000;
    int mget_size = 100;
    int del_amount = 7000000;
    int expire_amount = 7000000;

	if(const char* env_port = getenv("SSDB_PORT")) {
		port = atoi(env_port);
	}
	if(const char* env_mget_amount = getenv("MGET_AMOUNT")) {
	    mget_amount = atoi(env_mget_amount);
	}
	if(const char* env_mget_size = getenv("MGET_SIZE")) {
	    mget_size = atoi(env_mget_size);
	}
	if(const char* env_del_amount = getenv("DEL_AMOUNT")) {
	    del_amount = atoi(env_del_amount);
	}
	if(const char* env_expire_amount = getenv("EXPIRE_AMOUNT")) {
	    expire_amount = atoi(env_expire_amount);
	}
	Client *client = new Client();				
	client->connect(ip,port);
	if(!client->isok()){
		printf("Connect to pikaServer Faile!\n");
		delete client;
		return -1;
	}
	
	std::cout<<"Connect to pikaServer Success!"<< std::endl;
    if(strcmp(command,"mset")==0){
		//test for mset
		test_multi_set(client, filepath);                	 
	}
    if(strcmp(command,"set")==0){
		//test for set and correct rate 
		test_set(client,filepath);  	
	}
    if(strcmp(command,"get")==0){
		//test for get
		test_get(client,filepath,mget_amount,mget_size);  	
	}
    if(strcmp(command,"mget")==0){
		//test for mget
		test_multi_get(client,filepath,mget_amount,mget_size);   
	}
    if(strcmp(command,"delete")==0){
		//test for delete
		test_delete(client,filepath,del_amount);                 
	}
    if(strcmp(command,"expire")==0){
		//test for expire
		test_multi_expire(client,filepath,expire_amount);          
	}
    if(strcmp(command,"status")==0){
		//test for status
		test_status(client);
	}
	delete client;
	std::cout<<"Disconnect from the pikaServer, goodbye!"<< std::endl;
    return 0;
}
