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

#include "ssdb_test.hpp"
#include "SSDB_client.h"

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


void test_multi_set(ssdb::Client* const client, const char* filename) {
    std::cout<<"test multi set"<<std::endl;
    // 数组分别存储key和value
    std::vector<std::string> keys;
    std::vector<std::string> values;
    loadRawFile(filename, &keys, &values);
    
    
    // 执行测试
    const long double time_start = time(0);
    
    auto key_it = keys.begin();
    auto val_it = values.begin();
    
    std::map<std::string, std::string> data;
    int i = 0;
    while (key_it != keys.end() && val_it != values.end()) {
        std::map<std::string,std::string>::value_type item(*key_it, *val_it);
        data.insert(item);
        ++key_it;
        ++val_it;
        ++i;
        
        // 每1000条记录或者结束的时候，进行一次set操作
        if(i%1000 == 0 || (key_it == keys.end() && val_it == values.end()) ){
            client->multi_set(data);
            data.clear();
        }
        if(i%100000 == 0) {
            printf("set records = %d\r", i);
            fflush(stdout);
        }
    }
    const long double time_end = time(0);
    printf("%Lf -> %Lf\n", time_start, time_end);
    printf("total records = %ld\n", keys.size());
    printf("read time : %Lf, ops : %Lf\n",
           (time_end - time_start), keys.size() / (time_end - time_start));
    
    // 完成set后，进行一下正确性测试
    printf("test multi-set correction:\n");
    key_it = keys.begin();
    val_it = values.begin();
    std::string val;
    i = 0;
    while (key_it != keys.end() && val_it != values.end()) {
        client->get(*key_it, &val);
        assert((*val_it).compare(val) == 0);
        ++key_it;
        ++val_it;
        if(++i % 100000 == 0) {
            printf("tested records = %d\r", i);
            fflush(stdout);
        }
    }
}


void test_multi_get(ssdb::Client* const client, const char* filename, int mget_amount, int mget_size) {
    std::cout<<"test multi get, data init..."<<std::endl;
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
    
    // 执行测试
    printf("data loaded[size = %ld], start test...\n", kv.size());
    struct timespec t0, t1;
    std::vector<std::string> data;
    std::vector<std::string> result;
    std::vector<size_t>  indexVec;
    std::vector<size_t>  sorted;
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
        data.resize(0);
        for (size_t index: indexVec) {
            data.push_back(kv[index].first);
        }
        request_cnt += indexVec.size();
        
        // 每100条记录或者结束的时候，进行一次get操作
        result.resize(0);
        clock_gettime(CLOCK_MONOTONIC, &t0);
        ssdb::Status status = client->multi_get(data, &result);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        total_us += (t1.tv_sec - t0.tv_sec) * 1000000LL + (t1.tv_nsec - t0.tv_nsec) / 1000LL;
        response_cnt += result.size()/2;
#if 0
        for (size_t j = 0; j < result.size()/2; ++j) {
            const std::string& key = result[2*j + 0];
            const std::string& val = result[2*j + 1];
            size_t lo = 0, hi = kv.size();
            while (lo < hi) {
                size_t mid = (lo + hi) / 2;
                if (kv[mid].first < key)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            if (lo < kv.size() && kv[lo].first == key) { // found
                if (kv[lo].second != val) {
                    printf("---------------------------------------------\n");
                    printf("value of key  = %s \n", kv[lo].second.c_str());
                    printf("value of ssdb = %s \n", val.c_str());
                    ++incorrect;
                }
            } else {
                printf("not found key in input = %s\n", key.c_str());
            }
        }
#endif
        if(i%1000 == 0) {
            printf("get records = %zd\r", request_cnt);
            fflush(stdout);
        }
    }
    printf("incorrect records : %zd\n", incorrect);
    printf("total request  keys count = %zd\n", request_cnt);
    printf("total response kv   count = %zd\n", response_cnt);
    printf("read time : %f'seconds, ops : %f\n",
           total_us/1e6, 1e6*request_cnt / total_us);
}


// 测试delete
void test_delete(ssdb::Client* const client, const char* filename) {
    // 数组分别存储key和value
    std::vector<std::string> keys;
    std::vector<std::string> values;
    loadRawFile(filename, &keys, &values);
    printf("data loaded\n");
    
    printf("key size : %d\n", int(keys.size()));
    int i,j = 0;
    ssdb::Status status;
    std::string val;
    for(std::string key: keys) {
        // 只有存在的key，才尝试删除
        status = client->get(key, &val);
        if(status.ok() && !val.empty()) {
            client->del(key);
            if(j++ % 10000 == 0){
                printf("deleted : %d\n", j);
            }
        }
        if(i++ % 10000 == 0) {
            printf("\t tried : %d\n", i);
        }
    }
}

// 测试expire
void test_expire(ssdb::Client* const client, const char* filename) {
    // 数组分别存储key和value
    std::vector<std::string> keys;
    std::vector<std::string> values;
    loadRawFile(filename, &keys, &values);
    printf("data loaded\n");
    
    printf("key size : %d\n", int(keys.size()));
    int i = 0;
    const std::vector<std::string>* ret;
    for(std::string key: keys) {
        // 设置key5秒后过期
        ret = client->request("expire", key, "5");
        // 无法触发，因为SSDB的C++ API的delete找不到key也返回ok
        if(ret->front().compare("not_found") == 0) {
            printf("key not found, break.\n");
            break;
        }
        if(i++ % 10000 == 0) {
            printf("expire set : %d\r", i);
            fflush(stdout);
        }
    }
}

// 尝试API返回的状态
void test_status(ssdb::Client* const client, const char* filename) {
    ssdb::Status status = client->set("test_key", "test_val");
    printf("set status code : %s\n", status.code().c_str());
    
    std::string val;
    status = client->get("test_key", &val);
    printf("get success status value and code : %s , %s\n", val.c_str(), status.code().c_str());
    
    status = client->get("test_key_not_found", &val);
    printf("get failure status code : %s\n", status.code().c_str());
    
    status = client->del("test_key_not_found");
    printf("del failure status code : %s\n", status.code().c_str());
    
    status = client->request("expire", "test_key", "10");
    printf("expire success status code : %s\n", status.code().c_str());
    
    status = client->request("expire", "test_key_not_found", "10");
    printf("expire failure status code : %s\n", status.code().c_str());
}