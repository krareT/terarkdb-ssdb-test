#include <algorithm>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <map>
#include <ctime>
#include <assert.h>
#include <limits.h>
#include <string>
// Mac系统需要自定义clock_gettime
#ifdef __APPLE__
#include <mach/mach_time.h>
#define CLOCK_REALTIME 0
#define CLOCK_MONOTONIC 0
int clock_gettime(int clk_id, struct timespec *t);
#else
#include <time.h>
#endif
#ifndef _PIKA_SSDB_CLIENT_H
#define _PIKA_SSDB_CLIENT_H

void loadRawFile(const char* filename, std::vector<std::string> *keys, std::vector<std::string>* values);
void loadShufFile(const char* filename, std::vector<std::string> *keys);
void test_set(Client *client,const char *filename); 
void test_get(Client *client,const char *filename, int mget_amount, int mget_size); 
void test_multi_set(Client *client,const char *filename); 
void test_multi_get(Client *client,const char *filename, int mget_amount, int mget_size); 
void test_delete(Client *client,const char *filename, int del_amount); 
void test_multi_expire(Client *client,const char *filename, int expire_amount); 
void test_status(Client *client);

#endif

