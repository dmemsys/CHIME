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
#include <algorithm>

#define WRAP_AROUND

#define TEST_NUM 1000
#define KEY_MAX 60000000

int table_size;
int ampl_size;
int scan_entry_cnt = -1;


void set_hop_bit(int& hop_bitmap, int idx) {
  assert(idx >= 0 && idx < ampl_size && !(hop_bitmap & (1U << (ampl_size - idx - 1))));
  hop_bitmap |= 1U << (ampl_size - idx - 1);
}

void unset_hop_bit(int& hop_bitmap, int idx) {
  assert(idx >= 0 && idx < ampl_size && (hop_bitmap & (1U << (ampl_size - idx - 1))));
  hop_bitmap &= ~(1U << (ampl_size - idx - 1));
}


bool try_insert(std::vector<std::pair<int, int>>& hash_table, const int key, const int hash_idx) {
  auto get_entry = [=, &hash_table](int logical_idx) -> std::pair<int, int>& {
    return hash_table[(logical_idx + table_size) % table_size];  // [key, hop_bitmap]
  };

  // find an empty slot
  int j = -1;
#ifdef WRAP_AROUND
  int detect_end = hash_idx + table_size;
#else
  int detect_end = table_size;
#endif
  for (int i = hash_idx; i < detect_end; ++ i) {
    if (!get_entry(i).first) {
      j = i;
      break;
    }
  }
  if (j < 0) return false;
  // hop
next:
  if (j < hash_idx + ampl_size) {
    get_entry(j).first = key;
    set_hop_bit(get_entry(hash_idx).second, j - hash_idx);
    return true;
  }
  for (int offset = ampl_size - 1; offset > 0 ; -- offset) {
    int h = j - offset;
    int h_key = get_entry(h).first;
    int h_hash_idx = CityHash64((char *)&h_key, sizeof(h_key)) % table_size;
    // corner case
    if (h - h_hash_idx < 0) h_hash_idx -= table_size;
    else if (h - h_hash_idx >= ampl_size) h_hash_idx += table_size;
    assert(h_hash_idx <= h);
    // hop h => j is ok
    if (h_hash_idx + ampl_size > j) {
      get_entry(j).first = h_key;
      unset_hop_bit(get_entry(h_hash_idx).second, h - h_hash_idx);
      set_hop_bit(get_entry(h_hash_idx).second, j - h_hash_idx);
      j = h;
      goto next;
    }
  }
  return false;
}


int main(int argc, char *argv[]) {
  if (argc != 3 && argc != 4) {
    printf("Usage: ./hopscotch_hash_test table_size ampl_size [scan_entry_cnt]\n");
    exit(-1);
  }
  table_size = atoi(argv[1]);
  ampl_size = atoi(argv[2]);
  if (argc == 4) scan_entry_cnt = atoi(argv[3]);

  static std::random_device rd;
  static std::mt19937 e(rd());
  std::uniform_int_distribution<int> u(1, KEY_MAX);

  std::vector<double> load_factors;
  std::vector<int> segment_cnts;
  std::vector<int> read_entry_cnts;
  for (int i = 0; i < TEST_NUM; ++ i) {
    std::vector<int> loaded_keys;
    std::vector<std::pair<int, int>> hash_table(table_size, std::make_pair(0, 0));  // [key, hop_bitmap]
    // insert
    bool is_ok = true;
    while (is_ok) {
      int key = u(e);
      int hash_val = CityHash64((char *)&key, sizeof(key)) % table_size;
      is_ok = try_insert(hash_table, key, hash_val);
      if (is_ok) loaded_keys.push_back(key);
    }
    // test scan segmentation
    if (scan_entry_cnt > 0) {
      std::sort(loaded_keys.begin(), loaded_keys.end());

      std::vector<int> flags(table_size, 0);
      for (int idx = 0; idx < (int)hash_table.size(); ++ idx) {
        auto key = hash_table[idx].first;
        if (key != 0) {
          int hash_idx = CityHash64((char *)&key, sizeof(key)) % table_size;
          if (scan_entry_cnt > (int)loaded_keys.size() || key <= loaded_keys[scan_entry_cnt - 1]) {
            for (int i = 0; i < ampl_size; ++ i) flags[(hash_idx + i) % table_size] = 1;
          }
        }
      }
      int s_cnt = 0;
      for (int idx = 1; idx < table_size; ++ idx) {
        if (flags[idx - 1] != flags[idx]) ++ s_cnt;
      }
      s_cnt = (s_cnt + 1) / 2;
      if (flags.front() == 1 && flags.back() == 1) ++ s_cnt;
      segment_cnts.push_back(s_cnt);

      int re_cnt = 0;
      for (auto f : flags) if (f == 1) ++ re_cnt;
      read_entry_cnts.push_back(re_cnt);
    }
    // test load factor
    int cnt = 0, total = 0;
    for (const auto& p : hash_table) {
        if (p.first) ++ cnt;
        ++ total;
    }
    load_factors.push_back((double)cnt / total);
  }
  double sum = 0, s_sum = 0, re_sum = 0;
  for (auto a : load_factors) sum += a;
  for (auto a : segment_cnts) s_sum += a;
   for (auto a : read_entry_cnts) re_sum += a;
  printf("Avg. load factor: %.3lf\n", sum / (int)load_factors.size());
  if (scan_entry_cnt > 0) {
    printf("Avg. segment count: %.3lf\n", s_sum / (int)segment_cnts.size());
    printf("Avg. read entry count: %.3lf\n", re_sum / (int)read_entry_cnts.size());
  }
}