#include "Tree.h"
#include "DSM.h"
#include "Timer.h"
#include "zipf.h"
#include <city.h>

#include <stdlib.h>
#include <thread>
#include <time.h>
#include <vector>
#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <random>
#include <boost/crc.hpp>

#define TEST_EPOCH 10
#define TIME_INTERVAL 1

// #define USE_CORO

int kThreadCount;
int kNodeCount;
int kCoroCnt = 8;
int leaf_node_len = 1024;
int segment_len = 128;
int metadata_len = 8;
double zipfan = 0.99;
uint64_t addressCnt = 1000000;
int kReadRatio = 100;
enum TestType {
  READ_WHOLE_NODE,
  READ_METADATA_BEFORE_SEGMENT,
  READ_SEGMENT,
  READ_METADATA_AND_SEGMENT_1_IO,
  READ_METADATA_AND_SEGMENT_2_IO,
  OPTIMISTIC_LOCK,
  PESSIMISTIC_LOCK
} test_type;
#ifdef USE_CORO
bool kUseCoro = true;
#else
bool kUseCoro = false;
#endif


std::thread th[MAX_APP_THREAD];
uint64_t tp[MAX_APP_THREAD][MAX_CORO_NUM];

extern volatile bool need_stop;
extern uint64_t latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];
uint64_t latency_th_all[LATENCY_WINDOWS];

Tree *tree;
DSM *dsm;

using CRCProcessor = boost::crc_optimal<64, 0x42F0E1EBA9EA3693, 0xffffffffffffffff, 0xffffffffffffffff, false, false>;


class RequsetGenBench : public RequstGen {
public:
  RequsetGenBench(DSM* dsm, Request* req, int req_num, int coro_id, int coro_cnt) :
                  dsm(dsm), req(req), req_num(req_num), coro_id(coro_id), coro_cnt(coro_cnt) {
    local_thread_id = dsm->getMyThreadID();
    seed = rdtsc();
    mehcached_zipf_init(&state, addressCnt, zipfan,
                        (rdtsc() & (0x0000ffffffffffffull)) ^ local_thread_id);
  }

  Request next() override {
    Request r;
    r.req_type = (rand_r(&seed) % 100 < kReadRatio) ? SEARCH : UPDATE;
    uint64_t dis = mehcached_zipf_next(&state);
    r.v = dis;  // use v to store zipfian value

    tp[local_thread_id][coro_id]++;
    return r;
  }

private:
  DSM *dsm;
  Request* req;
  int req_num;
  int coro_id;
  int coro_cnt;
  int local_thread_id;
  unsigned int seed;
  struct zipf_gen_state state;
};


RequstGen *gen_func(DSM* dsm, Request* req, int req_num, int coro_id, int coro_cnt) {
  return new RequsetGenBench(dsm, req, req_num, coro_id, coro_cnt);
}

void lock_node(const GlobalAddress& lock_addr, uint64_t* cas_buffer, CoroPull *sink) {
  bool res;
re_acquire:
  res = dsm->cas_sync(lock_addr, 0, 1, cas_buffer, sink);
  if (!res) {
    if (sink != nullptr) {
      tree->busy_waiting_queue.push(sink->get());
      (*sink)();
    }
    goto re_acquire;
  }
  return;
}


void test_optimistic_lock(Tree *tree, const Request& r, const GlobalAddress& lock_addr, const GlobalAddress& data_addr, char* buffer, uint64_t* cas_buffer, CoroPull *sink) {
  static std::random_device rd;
  static std::mt19937 e(rd());
  static std::uniform_int_distribution<uint64_t> u(0, 10000);  // random value
  thread_local CRCProcessor crc_processor;
  auto& local_lock_table = tree->local_lock_table;
  auto& busy_waiting_queue = tree->busy_waiting_queue;

  if (r.req_type == UPDATE) {
    // handover
    bool write_handover = false;
    std::pair<bool, bool> lock_res = std::make_pair(false, false);
    Value rand_val = u(e);
#ifdef TREE_ENABLE_WRITE_COMBINING
    lock_res = local_lock_table->acquire_local_write_lock(int2key(r.v), rand_val, &busy_waiting_queue, sink);
    write_handover = (lock_res.first && !lock_res.second);
#else
    UNUSED(lock_res);
#endif
    if (write_handover) {
      goto update_finish;
    }

    {
    // lock
    lock_node(lock_addr, cas_buffer, sink);
    // write [checksum + value]
#ifdef TREE_ENABLE_WRITE_COMBINING
    local_lock_table->get_combining_value(int2key(r.v), rand_val);
#endif
    *((uint64_t *)(buffer + 8)) = rand_val;
    crc_processor.reset();
    crc_processor.process_bytes((char *)(buffer + 8), leaf_node_len);
    *((uint64_t *)buffer) = rand_val ? crc_processor.checksum() : 0;
    dsm->write_sync(buffer, data_addr, leaf_node_len + 8, sink);
    // unlock
    *cas_buffer = 0;
    dsm->write_sync((char *)cas_buffer, lock_addr, sizeof(uint64_t), sink);
    }

update_finish:
#ifdef TREE_ENABLE_WRITE_COMBINING
    local_lock_table->release_local_write_lock(int2key(r.v), lock_res);
#endif
    return;
  }
  else {
    Value val = 0;
    assert(r.req_type == SEARCH);
    // handover
    bool search_res = false;
    std::pair<bool, bool> lock_res = std::make_pair(false, false);
    bool read_handover = false;
#ifdef TREE_ENABLE_READ_DELEGATION
    lock_res = local_lock_table->acquire_local_read_lock(int2key(r.v), &busy_waiting_queue, sink);
    read_handover = (lock_res.first && !lock_res.second);
#else
    UNUSED(lock_res);
#endif
    if (read_handover) {
      goto search_finish;
    }

    {
    // read [checksum + value]
re_read:
    dsm->read_sync(buffer, data_addr, leaf_node_len + 8, sink);
    uint64_t checksum = *((uint64_t *)buffer);
    crc_processor.reset();
    crc_processor.process_bytes((char *)(buffer + 8), leaf_node_len);
    val = *((uint64_t *)(buffer + 8));
    auto calculated_checksum = val ? crc_processor.checksum() : 0;
    if (calculated_checksum != checksum) {
      goto re_read;
    }
    }

search_finish:
#ifdef TREE_ENABLE_READ_DELEGATION
    local_lock_table->release_local_read_lock(int2key(r.v), lock_res, search_res, val);
#endif
    return;
  }
}


void test_pessimistic_lock(Tree *tree, const Request& r, const GlobalAddress& lock_addr, const GlobalAddress& data_addr, char* buffer, uint64_t* cas_buffer, CoroPull *sink) {
  static std::random_device rd;
  static std::mt19937 e(rd());
  static std::uniform_int_distribution<uint64_t> u(0, 10000);  // random value
  auto& local_lock_table = tree->local_lock_table;
  auto& busy_waiting_queue = tree->busy_waiting_queue;

  if (r.req_type == UPDATE) {
    // handover
    bool write_handover = false;
    std::pair<bool, bool> lock_res = std::make_pair(false, false);
    Value rand_val = u(e);
#ifdef TREE_ENABLE_WRITE_COMBINING
    lock_res = local_lock_table->acquire_local_write_lock(int2key(r.v), rand_val, &busy_waiting_queue, sink);
    write_handover = (lock_res.first && !lock_res.second);
#else
    UNUSED(lock_res);
#endif
    if (write_handover) {
      goto update_finish;
    }

    {
    // lock
    lock_node(lock_addr, cas_buffer, sink);
    // write [value]
#ifdef TREE_ENABLE_WRITE_COMBINING
    local_lock_table->get_combining_value(int2key(r.v), rand_val);
#endif
    *((uint64_t *)buffer) = rand_val;
    dsm->write_sync(buffer, data_addr, leaf_node_len, sink);
    // unlock
    *cas_buffer = 0;
    dsm->write_sync((char *)cas_buffer, lock_addr, sizeof(uint64_t), sink);
    }

update_finish:
#ifdef TREE_ENABLE_WRITE_COMBINING
    local_lock_table->release_local_write_lock(int2key(r.v), lock_res);
#endif
    return;
  }
  else {
    Value val = 0;
    assert(r.req_type == SEARCH);
    // handover
    bool search_res = false;
    std::pair<bool, bool> lock_res = std::make_pair(false, false);
    bool read_handover = false;
#ifdef TREE_ENABLE_READ_DELEGATION
    lock_res = local_lock_table->acquire_local_read_lock(int2key(r.v), &busy_waiting_queue, sink);
    read_handover = (lock_res.first && !lock_res.second);
#else
    UNUSED(lock_res);
#endif
    if (read_handover) {
      goto search_finish;
    }

    {
    // lock
    lock_node(lock_addr, cas_buffer, sink);
    // read [value]
    dsm->read_sync(buffer, data_addr, leaf_node_len, sink);
    val = *((uint64_t *)buffer);
    // unlock
    *cas_buffer = 0;
    dsm->write_sync((char *)cas_buffer, lock_addr, sizeof(uint64_t), sink);
    }

search_finish:
#ifdef TREE_ENABLE_READ_DELEGATION
    local_lock_table->release_local_read_lock(int2key(r.v), lock_res, search_res, val);
#endif
    return;
  }
}


void read_only_test(Tree *tree, TestType test_type,
                    char* metadata_buffer, char* node_buffer, char* segment_buffer, uint64_t* cas_buffer,
                    const GlobalAddress& metadata_addr, const GlobalAddress& node_addr, const GlobalAddress& segment_addr, CoroPull *sink) {
  auto& local_lock_table = tree->local_lock_table;
  auto& busy_waiting_queue = tree->busy_waiting_queue;
  // handover
  Value val = 0;  // fake value
  bool search_res = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);
  bool read_handover = false;
#ifdef TREE_ENABLE_READ_DELEGATION
  lock_res = local_lock_table->acquire_local_read_lock(int2key(metadata_addr.to_uint64()), &busy_waiting_queue, sink);
  read_handover = (lock_res.first && !lock_res.second);
#else
  UNUSED(lock_res);
#endif
  if (read_handover) {
    goto search_finish;
  }

  {
  if (test_type == READ_WHOLE_NODE) {
    dsm->read_sync(node_buffer, metadata_addr, metadata_len + leaf_node_len, sink);
  }
  else if (test_type == READ_METADATA_BEFORE_SEGMENT) {
    dsm->read_sync(metadata_buffer, metadata_addr, metadata_len, sink);
    dsm->read_sync(segment_buffer, segment_addr, segment_len, sink);
  }
  else if (test_type == READ_SEGMENT) {
    dsm->read_sync(segment_buffer, segment_addr, segment_len, sink);
  }
  else if (test_type == READ_METADATA_AND_SEGMENT_1_IO) {
    dsm->read_sync(segment_buffer, segment_addr, segment_len + metadata_len, sink);
  }
  else if (test_type == READ_METADATA_AND_SEGMENT_2_IO) {
    RdmaOpRegion rs[2];
    rs[0].source = (uint64_t)metadata_buffer;
    rs[0].dest = metadata_addr.to_uint64();
    rs[0].size = metadata_len;
    rs[0].is_on_chip = false;

    rs[1].source = (uint64_t)segment_buffer;
    rs[1].dest = segment_addr.to_uint64();
    rs[1].size = segment_len;
    rs[1].is_on_chip = false;
    dsm->read_batch_sync(rs, 2, sink);
  }
  else assert(false);
  }

search_finish:
#ifdef TREE_ENABLE_READ_DELEGATION
  local_lock_table->release_local_read_lock(int2key(metadata_addr.to_uint64()), lock_res, search_res, val);
#endif
  return;
}


void work_func(Tree *tree, const Request& r, CoroPull *sink) {
  auto metadata_buffer = (dsm->get_rbuf(sink)).get_metadata_buffer();
  auto node_buffer = (dsm->get_rbuf(sink)).get_leaf_buffer();
  auto segment_buffer = (dsm->get_rbuf(sink)).get_segment_buffer();
  auto cas_buffer = (dsm->get_rbuf(sink)).get_cas_buffer();

  auto zipf_randval = (CityHash64((char *)&r.v, sizeof(r.v))) % addressCnt;
  auto metadata_addr = GlobalAddress{0, zipf_randval * (metadata_len + leaf_node_len + 8)};  // zipfian distribution

  auto node_addr = metadata_addr + metadata_len;
  auto segment_addr = node_addr + leaf_node_len / 2;  // without loss of generality

  if (test_type == OPTIMISTIC_LOCK) {
    test_optimistic_lock(tree, r, metadata_addr, node_addr, node_buffer, cas_buffer, sink);
  }
  else if (test_type == PESSIMISTIC_LOCK) {
    test_pessimistic_lock(tree, r, metadata_addr, node_addr, node_buffer, cas_buffer, sink);
  }
  else {
    read_only_test(tree, test_type, metadata_buffer, node_buffer, segment_buffer, cas_buffer, metadata_addr, node_addr, segment_addr, sink);
  }
}

std::atomic<int64_t> warmup_cnt{0};
std::atomic_bool ready{false};


void thread_run(int id) {
  bindCore(id * 2 + 1);  // bind to CPUs in NUMA that close to mlx5_2

  dsm->registerThread();
  uint64_t my_id = kThreadCount * dsm->getMyNodeID() + id;

  printf("I am %lu\n", my_id);

  warmup_cnt.fetch_add(1);

  if (id == 0) {
    while (warmup_cnt.load() != kThreadCount)
      ;
    printf("node %d ready\n", dsm->getMyNodeID());
    dsm->barrier("warm_finish");
    ready = true;
    warmup_cnt.store(-1);
  }
  while (warmup_cnt.load() != -1)
    ;

  // start test
  if (kUseCoro) {
    tree->run_coroutine(gen_func, work_func, kCoroCnt);
  }
  else {
    /// without coro
    Timer timer;
    auto thread_id = dsm->getMyThreadID();
    RequstGen *gen = gen_func(dsm, nullptr, 0, 0, 0);

    while (!need_stop) {
      timer.begin();
      work_func(tree, gen->next(), nullptr);
      auto us_10 = timer.end() / 100;

      if (us_10 >= LATENCY_WINDOWS) {
        us_10 = LATENCY_WINDOWS - 1;
      }
      latency[thread_id][0][us_10]++;
    }
  }
}

void parse_args(int argc, char *argv[]) {
  if (argc != 5 && argc != 7 && argc != 10) {
    printf("Usage 1: ./zipf_rdma_test kNodeCount kThreadCount kCoroCnt key_size value_size span_size segment_size metadata_len test_type[0/1/2/3/4]\n");
    printf("Usage 2: ./zipf_rdma_test kNodeCount kThreadCount kCoroCnt message_size\n");
    printf("Usage 3: ./zipf_rdma_test kNodeCount kThreadCount kCoroCnt kReadRatio message_size test_type[5/6]\n");
    exit(-1);
  }

  kNodeCount = atoi(argv[1]);
  kThreadCount = atoi(argv[2]);
  kCoroCnt = atoi(argv[3]);

  if (argc == 5) {  // Usage 2
    segment_len = atoi(argv[4]);
    test_type = READ_SEGMENT;
    printf("kNodeCount %d, kThreadCount %d, kCoroCnt %d, segment_len %d\n", kNodeCount, kThreadCount, kCoroCnt, segment_len);
    return;
  }

  if (argc == 7) {  // Usage 3
    kReadRatio = atoi(argv[4]);
    leaf_node_len = atoi(argv[5]);
    int test_type_num = atoi(argv[6]);
    assert(test_type_num >= 5 && test_type_num <= 6);
    test_type = static_cast<TestType>(test_type_num);
    addressCnt = 100000000;  // use large addressCnt to alleviate the lock conflicts under zipfian-0.99
    printf("kNodeCount %d, kThreadCount %d, kCoroCnt %d, kReadRatio %d, leaf_node_len %d\n", kNodeCount, kThreadCount, kCoroCnt, kReadRatio, leaf_node_len);
    return;
  }

  int key_size = atoi(argv[4]);
  int value_size = atoi(argv[5]);
  int span_size = atoi(argv[6]);
  int segment_size = atoi(argv[7]);
  metadata_len = atoi(argv[8]);
  int test_type_num = atoi(argv[9]);

  leaf_node_len = (key_size + value_size + 2 + 2) * span_size;  // 2 bytes for hop-bitmap, and ~2 bytes for versions
  segment_len = (key_size + value_size + 2 + 2) * segment_size;
  assert(test_type_num >= 0 && test_type_num <= 4);
  test_type = static_cast<TestType>(test_type_num);

  printf("kNodeCount %d, kThreadCount %d, kCoroCnt %d, leaf_node_len %d, segment_len %d, test_type %d\n", kNodeCount, kThreadCount, kCoroCnt, leaf_node_len, segment_len, test_type_num);
}

void save_latency(int epoch_id) {
  // sum up local latency cnt
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    latency_th_all[i] = 0;
    for (int k = 0; k < MAX_APP_THREAD; ++k)
      for (int j = 0; j < MAX_CORO_NUM; ++j) {
        latency_th_all[i] += latency[k][j][i];
    }
  }
  // store in file
  std::ofstream f_out("../us_lat/epoch_" + std::to_string(epoch_id) + ".lat");
  f_out << std::setiosflags(std::ios::fixed) << std::setprecision(1);
  if (f_out.is_open()) {
    for (int i = 0; i < LATENCY_WINDOWS; ++i) {
      f_out << i / 10.0 << "\t" << latency_th_all[i] << std::endl;
    }
    f_out.close();
  }
  else {
    printf("Fail to write file!\n");
    assert(false);
  }
  memset(latency, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_CORO_NUM * LATENCY_WINDOWS);
}

int main(int argc, char *argv[]) {

  parse_args(argc, argv);

  DSMConfig config;
  assert(kNodeCount >= MEMORY_NODE_NUM);
  config.machineNR = kNodeCount;
  config.threadNR = kThreadCount;
  dsm = DSM::getInstance(config);
  bindCore(kThreadCount * 2 + 1);
  dsm->registerThread();
  tree = new Tree(dsm, 0, false);
  dsm->barrier("benchmark");

  memset(tp, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_CORO_NUM);
  memset(latency, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_CORO_NUM * LATENCY_WINDOWS);

  for (int i = 0; i < kThreadCount; i ++) {
    th[i] = std::thread(thread_run, i);
  }

  while (!ready.load())
    ;
  timespec s, e;
  uint64_t pre_tp = 0;
  int count = 0;

  clock_gettime(CLOCK_REALTIME, &s);
  while(!need_stop) {

    sleep(TIME_INTERVAL);
    clock_gettime(CLOCK_REALTIME, &e);
    int microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                       (double)(e.tv_nsec - s.tv_nsec) / 1000;

    uint64_t all_tp = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      for (int j = 0; j < kCoroCnt; ++j)
        all_tp += tp[i][j];
    }
    clock_gettime(CLOCK_REALTIME, &s);

    uint64_t cap = all_tp - pre_tp;
    pre_tp = all_tp;

    save_latency(++ count);

    double per_node_tp = cap * 1.0 / microseconds;
    uint64_t cluster_tp = dsm->sum((uint64_t)(per_node_tp * 1000));  // only node 0 return the sum

    printf("%d, throughput %.4f\n", dsm->getMyNodeID(), per_node_tp);

    if (dsm->getMyNodeID() == 0) {
      printf("epoch %d passed!\n", count);
      printf("cluster throughput %.3f Mops\n", cluster_tp / 1000.0);
      printf("\n");
    }
    if (count >= TEST_EPOCH) {
      need_stop = true;
    }
  }
  printf("[END]\n");
  for (int i = 0; i < kThreadCount; i++) {
    th[i].join();
    printf("Thread %d joined.\n", i);
  }
  dsm->barrier("fin");

  return 0;
}
