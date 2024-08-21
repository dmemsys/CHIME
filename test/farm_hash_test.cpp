#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <iostream>
#include <fstream>
#include <random>
#include <algorithm>
#include <cassert>

// #define OVERFLOW_CHAIN
#define WRAP_AROUND

#define TEST_NUM 1000
#define KEY_MAX 60000000

int table_size;
int ampl_size;
int main_bucket_size;
int chain_size;

using FaRMBucket = std::tuple<std::vector<int>, std::vector<int>, int>;  //  bucket: [keys], [chain keys], hop_bitmap

void set_hop_bit(int& hop_bitmap, int idx) {
  assert(idx >= 0 && idx < 2);
  hop_bitmap |= 1U << (2 - idx - 1);
}

void unset_hop_bit(int& hop_bitmap, int idx) {
  assert(idx >= 0 && idx < 2);
  hop_bitmap &= ~(1U << (2 - idx - 1));
}


bool try_insert(std::vector<FaRMBucket>& hash_table, const int key, const int hash_idx) {
  auto get_entry = [=, &hash_table](int logical_idx) -> FaRMBucket& {
    return hash_table[(logical_idx + table_size) % table_size];  // [key, hop_bitmap]
  };

  auto insert_into_bucket = [](FaRMBucket& bucket, int key, int bucket_idx, int hash_idx){
    auto& [main_bucket, chain_bucket, bitmap] = bucket;
    for (auto& k : main_bucket) {
      if (!k) {
        k = key;
        set_hop_bit(bitmap, bucket_idx - hash_idx);
        return true;
      }
    }
    for (auto& k : chain_bucket) {
      if (!k) {
        k = key;
        set_hop_bit(bitmap, bucket_idx - hash_idx);
        return true;
      }
    }
    return false;
  };

  auto remove_from_bucket = [](FaRMBucket& bucket, int key, int bucket_idx, int hash_idx){
    auto& [main_bucket, chain_bucket, bitmap] = bucket;
    for (auto& k : main_bucket) if (k == key) { k = 0; break; }
    for (auto& k : chain_bucket) if (k == key) { k = 0; break; }
    bool is_empty = true;
    for (auto& k : main_bucket) if (k) { is_empty = false; break; }
    for (auto& k : chain_bucket) if (k) { is_empty = false; break; }
    if (is_empty) {
      unset_hop_bit(bitmap, bucket_idx - hash_idx);
    }
  };

  // find an empty slot
  int j = -1;
#ifdef WRAP_AROUND
  int detect_end = hash_idx + table_size;
#else
  int detect_end = table_size;
#endif
  for (int i = hash_idx; i < detect_end; ++ i) {
    const auto& main_bucket = std::get<0>(get_entry(i));
    const auto& chain_bucket = std::get<1>(get_entry(i));
    for (const auto k : main_bucket) if (!k) { j = i; break; }
    if (j >= 0) break;
    for (const auto k : chain_bucket) if (!k) { j = i; break; }
    if (j >= 0) break;
  }
  if (j < 0) return false;
  // hop
next:
  if (j < hash_idx + 2) {
    return insert_into_bucket(get_entry(j), key, j, hash_idx);
  }
  int h = j - 1;
  std::vector<int> h_keys;
  auto& h_main_bucket = std::get<0>(get_entry(h));
  auto& h_chain_bucket = std::get<1>(get_entry(h));

  std::merge(h_main_bucket.begin(), h_main_bucket.end(), h_chain_bucket.begin(), h_chain_bucket.end(), std::back_inserter(h_keys));
  for (const auto& h_key : h_keys) if (h_key) {
    int h_hash_idx = CityHash64((char *)&h_key, sizeof(h_key)) % table_size;
    // corner case
    if (h - h_hash_idx < 0) h_hash_idx -= table_size;
    else if (h - h_hash_idx >= 2) h_hash_idx += table_size;
    assert(h_hash_idx <= h);
    // hop h => j is ok
    if (h_hash_idx + 2 > j) {
      bool is_ok = insert_into_bucket(get_entry(j), h_key, j, h_hash_idx);
      if (!is_ok) continue;
      remove_from_bucket(get_entry(h), h_key, h, h_hash_idx);
      j = h;
      goto next;
    }
  }

  return false;
}


int main(int argc, char *argv[]) {
  if (argc != 3) {
    printf("Usage: ./farm_hash_test table_size ampl_size\n");
    exit(-1);
  }
  table_size = atoi(argv[1]);
  ampl_size = atoi(argv[2]);
  assert(ampl_size > 2);

#ifdef OVERFLOW_CHAIN
  main_bucket_size = (ampl_size - 2) / 2;
  chain_size = ampl_size - main_bucket_size * 2;
#else
  main_bucket_size = ampl_size / 2;
  chain_size = 0;
#endif
  table_size = table_size / (main_bucket_size + chain_size);

  static std::random_device rd;
  static std::mt19937 e(rd());
  std::uniform_int_distribution<int> u(1, KEY_MAX);

  std::vector<double> load_factors;
  for (int i = 0; i < TEST_NUM; ++ i) {
    std::vector<FaRMBucket> hash_table(table_size,
                                       std::make_tuple(std::vector<int>(main_bucket_size, 0), std::vector<int>(chain_size, 0), 0));
    // insert
    bool is_ok = true;
    while (is_ok) {
      int key = u(e);
      int hash_val = CityHash64((char *)&key, sizeof(key)) % table_size;
      is_ok = try_insert(hash_table, key, hash_val);
    }
    // test load factor
    int cnt = 0, total = 0;
    for (const auto& p : hash_table) {
      const auto& main_bucket = std::get<0>(p);
      const auto& chain_bucket = std::get<1>(p);
      for (const auto& k : main_bucket) if (k) ++ cnt;
      for (const auto& k : chain_bucket) if (k) ++ cnt;
      total += main_bucket_size + chain_size;
    }
    load_factors.push_back((double)cnt / total);
  }
  double sum = 0;
  for (auto a : load_factors) sum += a;
  printf("Avg. load factor: %.3lf\n", sum / (int)load_factors.size());
}