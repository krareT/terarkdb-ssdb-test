#include <algorithm>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <ctime>
#include <assert.h>
#include <limits.h>

// Mac系统不支持clock_gettime
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

void test_multi_set(ssdb::Client* const client, const char* filename) {
    std::cout<<"test multi set"<<std::endl;
    // 数组分别存储key和value
    std::vector<std::string> keys;
    std::vector<std::string> values;
    
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
                keys.push_back(review->userId.append(review->productId));
                values.push_back(review->review);
            }
        }
    }
    assert(keys.size() == values.size());
    
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


// 测试TTL
void test_ttl(ssdb::Client* const client, const char* filename) {
    // 先进性multi-set操作，把所有数据灌进去
    test_multi_set(client, filename);
    // 通过ssdb的接口取出所有的key，进行expire操作
    std::vector<std::string> keys;
    client->keys("", "", LONG_MAX, &keys);
    // 先试一下delete操作
    int i = 0;
    for(auto key: keys) {
        client->del(key);
        if(i++ % 1000 == 0) {
            printf("deleted : %d\n", i);
        }
    }
}

