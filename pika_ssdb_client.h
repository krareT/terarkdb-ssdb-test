//
// pika_ssdb_client.h
//
// Created by Wangshujing on 12/7/17
//

#include <iostream>
#include <vector>
#include <string>
#include "hiredis/hiredis.h"
#include <sstream>
class Client {
public:
	Client();
	~Client();
	bool isok();
	bool isexist();
	void getstr();
	void freeReply();
	void connect(const char *ip, int &port);
	// EXPIRE
	void expire(const std::string &key, const std::string &expire_time);
	// SET
	void set(const std::string &key, const std::string &val);
	// DEL
	void del(const std::string &key);
	// GET
	void get( const std::string &key,std::string &val);
	// MGET
	void mget( const std::vector<std::string> &key);
	// MSET
	void mset( const std::vector<std::string> &key, const std::vector<std::string> &val);
	redisContext *redis ;
	redisReply   *reply ;
};
