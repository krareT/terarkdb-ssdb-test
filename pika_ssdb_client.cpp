//
// pika_ssdb_client.cpp
//
// Created by Wangshujing on 12/7/17
//

#include <iostream>
#include <sstream>
#include <cstring>
#include <stdlib.h>
#include "hiredis/hiredis.h"
#include <vector>
#include <string>
#include "pika_ssdb_client.h"

Client::Client(){};

Client::~Client(){
	redisFree(Client::redis);	
}

void Client::freeReply(){
	freeReplyObject(Client::reply);
}

void Client::connect(const char *ip, int &port){
	Client::redis = redisConnect(ip, port);
}

void Client::getstr(){
}

bool Client::isexist(){
	Client::getstr();
	if(Client::reply->type == 4){
		return false;
	}
	return true;
}

bool Client::isok(){
	if(NULL == Client::redis || Client::redis->err){
		return false;
	}
	return true;
}
//set
void Client::set(const std::string &key, const std::string &val){
	reply = (redisReply *)redisCommand(Client::redis,"SET %s %s", key.c_str(), val.c_str());
}
// expire
void Client::expire(const std::string &key, const std::string &expire_time){
	reply = (redisReply *)redisCommand(Client::redis,"expire %s %s", key.c_str(), expire_time.c_str());
}
// del
void Client::del(const std::string &key){
	reply = (redisReply *)redisCommand(Client::redis,"DEL %s", key.c_str());
}
// MSET 
void Client::mset(const std::vector<std::string> &key, const std::vector<std::string> &val){
	if(key.size() != val.size()){
		printf("Redis error");
	}
	std::vector<const char*> argv(key.size() + val.size() + 1);
	std::vector<size_t> argvlen(key.size() + val.size() + 1);
	int j = 0;
	static char msetcmd[] = "MSET";
	argv[j] = msetcmd;
	argvlen[j] = sizeof(msetcmd)-1;
	++j;

	for (int i=0;i<key.size();++i){
		argvlen[j] = key[i].length();
		argv[j] = new char[argvlen[j]];
		memset((void*)argv[j],0,argvlen[j]);
		memcpy((void*)argv[j],key[i].data(),key[i].length());
		++j;
		
		argvlen[j] = val[i].length();
		argv[j] = new char[argvlen[j]];
		memset((void*)argv[j],0,argvlen[j]);
		memcpy((void*)argv[j],val[i].data(),val[i].length());
		++j;
	}
	Client::reply= (redisReply *)redisCommandArgv(Client::redis, argv.size(), &(argv[0]), &(argvlen[0]));
	if (!reply){
		printf("Redis error");
	}
}
// GET
void Client::get( const std::string &key,std::string &val){
	Client::reply = (redisReply *)redisCommand(Client::redis,"GET %s", key.c_str());
	if(Client::isexist()){
		val = Client::reply->str;
	}
};
// MGET
void Client::mget( const std::vector<std::string> &key){
	std::vector<const char*> argv(key.size() + 1);
	std::vector<size_t> argvlen(key.size() + 1);
	int j = 0;
	static char msetcmd[] = "MGET";
	argv[j] = msetcmd;
	argvlen[j] = sizeof(msetcmd)-1;
	++j;

	for (int i=0;i<key.size();++i){
		argvlen[j] = key[i].length();
		argv[j] = new char[argvlen[j]];
		memset((void*)argv[j],0,argvlen[j]);
		memcpy((void*)argv[j],key[i].data(),key[i].length());
		++j;
	}
	//std::cout<<"mget"<<std::endl;
	Client::reply = (redisReply *)redisCommandArgv(Client::redis, argv.size(), &(argv[0]), &(argvlen[0]));
	if (!reply){
		printf("Redis error");
	}
};

