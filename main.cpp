//
//  main.cpp
//  ssdb-test
//
//  Created by Roy Guo on 27/5/59 BE.
//  Copyright © 2559 BE Roy Guo. All rights reserved.
//

#include <iostream>
#include <cstring>

#include "SSDB_client.h"
#include "ssdb_test.hpp"


int main(int argc, const char** argv) {
    const char* command = argv[1];
    const char* filepath = argv[2];
    if(command == NULL || filepath == NULL) {
        printf("usage : ./ssdb-benchmark [mset|mget|ttl] [movie_file_path]\n");
        printf("mget env : SSDB_PORT(default 8888) | MGET_AMOUNT(default 7000000) | MGET_SIZE(default 100)\n");
        exit(0);
    }

    int port = 8888;
    int mget_amount = 7000000;
    int mget_size = 100;
    
    if(const char* env_port = getenv("SSDB_PORT")) {
        port = atoi(env_port);
    }
    if(const char* env_mget_amount = getenv("MGET_AMOUNT")) {
        mget_amount = atoi(env_mget_amount);
    }
    if(const char* env_mget_size = getenv("MGET_SIZE")) {
        mget_size = atoi(env_mget_size);
    }
    
    ssdb::Client* client = ssdb::Client::connect("127.0.0.1", port);
    
    if( strcmp(command, "mset") == 0) {
        test_multi_set(client, filepath);
    }
    if( strcmp(command, "mget") == 0) {
        test_multi_get(client, filepath, mget_amount, mget_size);
    }
    if( strcmp(command, "delete") == 0) {
        test_delete(client, filepath);
    }
    
    delete client;
    client = nullptr;
    return 0;
}