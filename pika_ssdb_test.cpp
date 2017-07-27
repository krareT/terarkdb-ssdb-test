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
#include <sstream>
#include <string>
// Mac系统需要自定义clock_gettime
#ifdef __APPLE__
#include <mach/mach_time.h>
#define CLOCK_REALTIME 0
#define CLOCK_MONOTONIC 0
int clock_gettime(int clk_id, struct timespec *t){
    mach_timebase_info_data_t timebase;
    mach_timebase_info(&timebase);
    uint64_t time;
    time = mach_absolute_time();
    double nseconds = ((double)time * (double)timebase.numer)/((double)timebase.denom);
    double seconds = ((double)time * (double)timebase.numer)/((double)timebase.denom * 1e9);
    t->tv_sec = seconds;
    t->tv_nsec = nseconds;
    return 0;
}
#else
#include <time.h>
#endif
#include "pika_ssdb_client.h"
#ifndef _HIREDIS_H
#define _HIREDIS_H
#endif
class Review {
public:
	std::string productId;
	std::string userId;
	std::string review;
	bool validOrErase();
};
bool Review::validOrErase() {
    if(!productId.empty() && !userId.empty() && !review.empty()) {
        return true;
    }
    productId.clear();
    userId.clear();
    review.clear();
    return false;
}

// 把原始文件读成key和value的组合
void loadRawFile(const char* filename, std::vector<std::string>* keys, std::vector<std::string>* values) {
    // 逐行遍历原始文件
    Review* review = new Review();
    std::fstream infile(filename);
    assert(infile);
    std::string line;
    while(std::getline(infile, line)) {
        if( (int)line.find("product/productId:", 0) > -1) {
            review->productId = line;
        }else if( (int)line.find("review/userId:", 0) > -1) {
            review->userId = line;
        }else if( (int)line.find("review/text:", 0) > -1) {
            review->review = line;
            // 到最末尾验证一下记录是否填满
            if(review->validOrErase()) {
                keys->push_back(review->userId.append(review->productId));
                values->push_back(review->review);
            }
        }
    }
    assert(keys->size() == values->size());
}
// set
void test_set(Client *client,const char *filename) {
	std::cout<<"II---------> test set, data init..."<<std::endl;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    loadRawFile(filename, &keys, &values);


	auto key_it = keys.begin();
	auto val_it = values.begin();
	int i = 0,j=0;
	const long double time_start = time(0);
	while (key_it != keys.end() && val_it != values.end()){
		client->set(*key_it,*val_it);
		client->freeReply();
		++i;
		++key_it;
		++val_it;
		if (i%10000 == 0){
			std::cout<<"set records = "<<i<<"\r";
			fflush(stdout);
		}
	}
	const long double time_end = time(0);
    printf("%Lf -> %Lf\n", time_start, time_end);
    printf("total records = %ld\n", keys.size());
    printf("read time : %Lf, ops : %Lf\n",(time_end - time_start), keys.size() / (time_end - time_start));
}
// get
void test_get(Client *client,const char *filename, int mget_amount, int mget_size) {
	// 考虑随机性
	std::cout<<"II---------> test get, data init..."<<std::endl;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    loadRawFile(filename, &keys, &values);
	auto key_it = keys.begin();
	auto val_it = values.begin();
        // 执行测试, 随机读取, 允许重复读
        printf("data loaded[size = %ld], start test...\n", keys.size());
        struct timespec t0, t1;
        std::vector<size_t>  indexVec;
	    std::string result;
        size_t incorrect = 0;
        size_t request_cnt = 0;
        size_t response_cnt = 0;
        size_t total_us = 0;
	    for (size_t i =0; i<= mget_amount; ++i){		
		    indexVec.resize(0);
	        for(size_t j=0;j <mget_size; ++j){			
			    int index = lrand48() % keys.size();
			    indexVec.push_back(index);				
		    }  	
            std::sort(indexVec.begin(), indexVec.end());
     	    indexVec.erase(std::unique(indexVec.begin(), indexVec.end()), indexVec.end());
     	    std::random_shuffle(indexVec.begin(), indexVec.end());
      	    // 每100条记录或者结束的时候，进行一组get操作
      	    clock_gettime(CLOCK_MONOTONIC, &t0);
            request_cnt += indexVec.size();			
      	    for (size_t index: indexVec) {
      	        client->get(keys[index], result);
			    client->freeReply();
			    if(result == values[index]){
				    response_cnt += 1;
			    }
			    else{	
				    incorrect +=1;
			    }
				result.clear();
   	        }
     	    clock_gettime(CLOCK_MONOTONIC, &t1);
    	    total_us += (t1.tv_sec - t0.tv_sec) * 1000000LL + (t1.tv_nsec - t0.tv_nsec) / 1000LL;  
      	    //result.resize(0);
    	    //response_cnt += result.size()/2;
		    if (i%1000 == 0){
			    printf("get records = %zd\r", request_cnt);
			    fflush(stdout);
		    }
	    }
	    printf("incorrect records : %zd\n", incorrect);
	    printf("total request key  count  = %zd\n", request_cnt);
	    printf("total response kv  count = %zd\n", response_cnt);
	    printf("read time: %f' seconds,ops: %f\n", total_us/1e6, 1e6*request_cnt/total_us);
}
// mget
void test_multi_get(Client *client, const char* filename, int mget_amount, int mget_size) {
    std::cout<<"II---------> test multi get, data init..."<<std::endl;
    // 数组分别存储key和value
    std::vector<std::pair<std::string, std::string> > kv;
    
    // 逐行遍历原始文件以初始化数据
    Review* review = new Review();
    std::fstream infile(filename);
    assert(infile);
    std::string line;
    size_t limit = size_t(-1);
    if (const char* env = getenv("INPUT_LIMIT")) {
        limit = atoi(env);
    }
    for (size_t i = 0; i < limit && std::getline(infile, line); ) {
        if( (int)line.find("product/productId:", 0) > -1) {
            review->productId = line;
        }else if( (int)line.find("review/userId:", 0) > -1) {
            review->userId = line;
        }else if( (int)line.find("review/text:", 0) > -1) {
            review->review = line;
            // 到最末尾验证一下记录是否填满了对象
            if(review->validOrErase()) {
                kv.push_back(std::make_pair(review->userId.append(review->productId), review->review));
            }
            i++;
        }
    }
    std::sort(kv.begin(), kv.end(), [&](const auto& x, const auto& y) {
        return x.first < y.first;
    });
    
    // 执行测试, 随机读取, 允许重复读
    printf("data loaded[size = %ld], start test...\n", kv.size());
    struct timespec t0, t1;
    std::vector<std::string> key;
    std::vector<size_t>  indexVec;
    size_t incorrect = 0;
    size_t request_cnt = 0;
    size_t response_cnt = 0;
    size_t total_us = 0;
    for (size_t i = 0; i <= mget_amount; ++i) {
        indexVec.resize(0);
        for (size_t j = 0; j < mget_size; ++j) {
            int index = lrand48() % kv.size();
            indexVec.push_back(index);
        }
        std::sort(indexVec.begin(), indexVec.end());
        indexVec.erase(std::unique(indexVec.begin(), indexVec.end()), indexVec.end());
        std::random_shuffle(indexVec.begin(), indexVec.end()); 	
	    key.clear();            
        for (size_t index: indexVec) {
            key.push_back(kv[index].first);
        }
        request_cnt += indexVec.size();                          
        
        // 每100条记录或者结束的时候，进行一次mget操作
        //result.resize(0);                  
        clock_gettime(CLOCK_MONOTONIC, &t0);
        client->mget(key);                       
        clock_gettime(CLOCK_MONOTONIC, &t1);
        total_us += (t1.tv_sec - t0.tv_sec) * 1000000LL + (t1.tv_nsec - t0.tv_nsec) / 1000LL;
        //  test output 
	    for (int i =0; i < client->reply->elements; ++i){
		    if(client->reply->element[i]->str == kv[indexVec[i]].second){
			    response_cnt += 1;
		    } 
		    else{
			    incorrect += 1;
		    } 
	    }
        client->freeReply();
        //response_cnt += result.size()/2;
        if(i%1000 == 0) {
            printf("get records = %zd\r", request_cnt);
            fflush(stdout);
        }
    }
    printf("incorrect records : %zd\n", incorrect);
    printf("total request  keys count = %zd\n", request_cnt);
    printf("total response kv   count = %zd\n", response_cnt);
    printf("read time : %f'seconds, ops : %f\n",total_us/1e6, 1e6*request_cnt / total_us);
}

// MSET
void test_multi_set(Client *client,const char* filename) {
    std::cout<<"II---------> test multi set, data init..."<<std::endl;
    // 数组分别存储key和value
    std::vector<std::string> keys;
    std::vector<std::string> values;
    loadRawFile(filename, &keys, &values);
    // 执行测试
    const long double time_start = time(0);
    
    auto key_it = keys.begin();
    auto val_it = values.begin();
    
    int i = 0;
    while (key_it != keys.end() && val_it != values.end()) {
		std::vector<std::string> key;
		std::vector<std::string> value;
		key.push_back(*key_it);
		value.push_back(*val_it);
        ++key_it;
        ++val_it;
        ++i;
        
        // 每1000条记录或者结束的时候，进行一次set操作
        if(i%1000 == 0 || (key_it == keys.end() && val_it == values.end()) ){
			client->mset(key,value);
			client->freeReply();
			client->mget(key);
			std::cout<<value[0]<<"\n"<<client->reply->element[0]->str<<std::endl;
			client->freeReply();
			key.clear();
			value.clear();
        }
        if(i%100000 == 0) {
            printf("set records = %d \r", i);
            fflush(stdout);
        }
    }
    const long double time_end = time(0);
    printf("%Lf -> %Lf\n", time_start, time_end);
    printf("total records = %ld\n", keys.size());
    printf("read time : %Lf, ops : %Lf\n",(time_end - time_start), keys.size() / (time_end - time_start));
}

// DEL  
void test_delete(Client *client ,const char* filename, int del_amount) {
    printf("II---------> test delete...\n");
    // 数组分别存储key和value
    std::vector<std::string> keys;
    std::vector<std::string> values;
    loadRawFile(filename, &keys, &values);
    printf("data loaded\n");
    
    printf("key size : %d\n", int(keys.size()));
    int size = keys.size();
    
    while(del_amount-- > 0) {
        // 随机删除一个key，可以重复删除
        int index = lrand48() % size;
	std::string val;
	//std::cout<<7000000-del_amount<<std::endl;
        client->get(keys[index], val);
        // 只有存在的key，才尝试删除
        if(client->isexist()&& !val.empty()) {
	    client->freeReply();
            client->del(keys[index]);
	    client->freeReply();
        }
	else{
	    client->freeReply();
	}
        if( del_amount % 10000 == 0) {
            printf("delete amount reminds : %d\r", del_amount);
            fflush(stdout);
        }
    }
    std::cout<<"\ndelete done!"<<std::endl;
}

// 测试expire
void test_multi_expire(Client *client, const char* filename, int expire_amount) {
    // 数组分别存储key和value
    printf("II---------> test expire...\n");
    std::vector<std::string> keys;
    std::vector<std::string> values;
    loadRawFile(filename, &keys, &values);
    printf("data loaded\n");
    
    printf("key size : %d\n", int(keys.size()));
    int size = keys.size();
    
    while(expire_amount-- > 0) {
        int index = lrand48() % size;
        client->expire(keys[index], "10");
	client->freeReply();
        if( expire_amount % 10000 == 0) {
            printf("expire amount reminds : %d\r", expire_amount);
            fflush(stdout);
        }
    }
    std::cout<<"\ntest expire...done!"<<std::endl;
}
// 尝试API返回的状态
void test_status(Client *client) {
    client->set("test_key", "test_val");  		
    printf("set status code : %s\n", client->reply->str);	
    std::cout<<"type:"<<client->reply->type<<std::endl;	
    client->freeReply();
    
    std::string val;
    client->get("test_key", val);              	
    printf("get success status value and code : %s , %s\n", val.c_str(), client->reply->str);
    std::cout<<"type:"<<client->reply->type<<std::endl;
    client->freeReply();
    
    client->get("test_key_not_found", val);    	
    printf("get failure status code : %s\n",client->reply->str);
    std::cout<<"type:"<<client->reply->type<<std::endl;
    client->freeReply();
    
    client->del("test_key_not_found");			
    printf("del failure status code : %lld\n", client->reply->integer);
    std::cout<<"type:"<<client->reply->type<<std::endl;
    client->freeReply();
    
    client->expire("test_key", "10");   		
    printf("expire success status code : %lld\n", client->reply->integer);
    std::cout<<"type:"<<client->reply->type<<std::endl;
    client->freeReply();
    
    client->expire("test_key_not_found", "10");  	
    printf("expire failure status code : %lld\n", client->reply->integer);
    std::cout<<"type:"<<client->reply->type<<std::endl;
    client->freeReply();

	std::vector<std::string> msetk;
	std::vector<std::string> msetv;
	msetk.push_back("wang shujing");
	msetk.push_back("wang liguang");
	msetv.push_back("beijing hang kong ");
	msetv.push_back("beijing bei sihuan");
	client->mset(msetk,msetv);
	client->mget(msetk);
	std::cout<<client->reply->element[0]->str<<std::endl;
	std::cout<<client->reply->element[1]->str<<std::endl;

}
