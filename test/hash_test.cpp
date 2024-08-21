#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <iostream>
#include <fstream>
#include <random>
#include <cassert>


#define TEST_NUM 1000
#define KEY_MAX 60000000

int table_size;
int ampl_size;


uint32_t murmurhash(const void* key, int len, uint32_t seed) {
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    const uint32_t r1 = 15;
    const uint32_t r2 = 13;
    const uint32_t m = 5;
    const uint32_t n = 0xe6546b64;
 
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 4;
 
    uint32_t hash = seed;
 
    for (int i = 0; i < nblocks; i++) {
        uint32_t k = *(uint32_t*)(data + i * 4);
 
        k *= c1;
        k = (k << r1) | (k >> (32 - r1));
        k *= c2;
 
        hash ^= k;
        hash = (hash << r2) | (hash >> (32 - r2));
        hash = hash * m + n;
    }
 
    const uint8_t* tail = (const uint8_t*)(data + nblocks * 4);
    uint32_t k = 0;
 
    switch (len & 3) {
        case 3:
            k ^= tail[2] << 16;
        case 2:
            k ^= tail[1] << 8;
        case 1:
            k ^= tail[0];
            k *= c1;
            k = (k << r1) | (k >> (32 - r1));
            k *= c2;
            hash ^= k;
    }
 
    hash ^= len;
    hash ^= hash >> 16;
    hash *= 0x85ebca6b;
    hash ^= hash >> 13;
    hash *= 0xc2b2ae35;
    hash ^= hash >> 16;
    return hash;
}


double get_load(std::vector<std::vector<int>>& hash_table, const int hash_val) {
  int bucket_idx = hash_val;
  auto& main_bucket = hash_table[bucket_idx];

  int cnt = 0, total = 0;
  for (auto v : main_bucket) {
    if (v) ++ cnt;
    ++ total;
  }
  return (double) cnt / total;
}


bool try_insert(std::vector<std::vector<int>>& hash_table, const int hash_val) {
  int bucket_idx = hash_val;
  auto& main_bucket = hash_table[bucket_idx];
  int bucket_size = ampl_size;
  int i = 0;
  for (; i < bucket_size; ++ i) if (main_bucket[i] == 0) {
    main_bucket[i] = 1;
    break;
  }
  if (i == bucket_size) { // main bucket is full
    if (i == bucket_size) { // overflow bucket is full
      return false;
    }
  }
  return true;
}


int main(int argc, char *argv[]) {
  if (argc != 3) {
    printf("Usage: ./hash_test table_size ampl_size\n");
    exit(-1);
  }
  table_size = atoi(argv[1]);
  ampl_size = atoi(argv[2]);

  static std::random_device rd;
  static std::mt19937 e(rd());
  std::uniform_int_distribution<int> u(1, KEY_MAX);

  std::vector<double> load_factors;
  for (int i = 0; i < TEST_NUM; ++ i) {
    std::vector<std::vector<int>> hash_table(table_size, std::vector<int>(ampl_size, 0));  // [bucket, bucket, ... bucket]
    // insert
    bool is_ok = true;
    while (is_ok) {
      int key = u(e);
      int hash_val_1 = CityHash64((char *)&key, sizeof(key)) % table_size;
      is_ok = try_insert(hash_table, hash_val_1);
    }
    // test load factor
    int cnt = 0, total = 0;
    for (const auto& bucket : hash_table) {
      for (auto v : bucket) {
        if (v) ++ cnt;
        ++ total;
      }
    }
    load_factors.push_back((double)cnt / total);
  }
  double sum = 0;
  for (auto a : load_factors) sum += a;
  printf("Avg. load factor: %.3lf\n", sum / (int)load_factors.size());
}