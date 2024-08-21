#include "DSM.h"
#include "Tree.h"

#include <iostream>

#define TEST_NUM 102400  // 102400

int main() {

  DSMConfig config;
  config.machineNR = 2;
  assert(MEMORY_NODE_NUM == 1);
  DSM *dsm = DSM::getInstance(config);
 
  dsm->registerThread();

  auto tree = new Tree(dsm);

  Value v;

  if (dsm->getMyNodeID() != 0) {
    dsm->barrier("fin");
    return 0;
  }

  // test insert
  for (uint64_t i = 1; i <= TEST_NUM; ++i) {
    printf("inserting %lu...\n", i);
    tree->insert(int2key(i), i * 2);
  }
  printf("insert passed.\n");

  // test update
  for (uint64_t i = TEST_NUM; i >= 1; --i) {
    // printf("updating %lu...\n", i);
    tree->update(int2key(i), i * 3);
  }
  printf("update passed.\n");

  // test search
  for (uint64_t i = 1; i <= TEST_NUM; ++i) {
    assert(!tree->search(int2key(TEST_NUM + i), v));
  }
  for (uint64_t i = 1; i <= TEST_NUM; ++i) {
    auto res = tree->search(int2key(i), v);
    // std::cout << "search result:  " << (bool)res << " v: " << v << " ans: " << i * 3 << std::endl;
    assert(res && v == i * 3);
    // assert(res && v == i * 2);
  }
  printf("search passed.\n");

  // test scan
  std::map<Key, Value> ret;
  uint64_t from = 1, to = 1024;
  tree->range_query(int2key(from), int2key(to), ret);
  for (uint64_t j = from; j < to; ++ j) assert(ret[int2key(j)] == j * 3);
  printf("range query passed.\n");

  printf("Hello!\n");
  dsm->barrier("fin");
  return 0;
}