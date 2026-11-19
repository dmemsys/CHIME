#if !defined(_LOCAL_LOCK_TABLE_H_)
#define _LOCAL_LOCK_TABLE_H_

#include "Common.h"
#include "Key.h"
#include "LeafNode.h"
#include "InternalNode.h"
#include "GlobalAddress.h"
#include "Hash.h"

#include <queue>
#include <set>
#include <atomic>
#include <mutex>


#define MAX_HANDOVER_TYPE_NUM 2
#define MAX_HOCL_HANDOVER 8
enum HandoverType {
  READ_HANDOVER,
  WRITE_HANDOVER,
};


struct LocalSpinLock {
  std::atomic<uint8_t> flag{0};

  inline void lock() noexcept {
    while (flag.exchange(1, std::memory_order_acquire) != 0) {
      while (flag.load(std::memory_order_relaxed) != 0) {
        __builtin_ia32_pause();
      }
    }
  }

  inline void unlock() noexcept {
    flag.store(0, std::memory_order_release);
  }
};


struct alignas(64) LocalLockNode {
  // Naturally 8-byte aligned hot fields (offsets 0..31)
  std::atomic<uint64_t> unique_read_key;
  std::atomic<uint64_t> unique_write_key;
  std::atomic<Value> wc_buffer;
  GlobalAddress unique_addr;

  // Read delegation / cas-handover payload (offset 32..48, 17 bytes)
  union __attribute__((packed)) {
    Value ret_value;
    InternalEntry ret_p;
  };

  // Compact 1-byte control fields (offsets 49..61)
  std::atomic<uint8_t> read_current;
  std::atomic<uint8_t> read_ticket;
  volatile bool read_handover;
  std::atomic<uint8_t> write_current;
  std::atomic<uint8_t> write_ticket;
  volatile bool write_handover;
  std::atomic<bool> window_start;
  std::atomic<uint8_t> read_window;
  std::atomic<uint8_t> write_window;
  LocalSpinLock r_lock;
  LocalSpinLock w_lock;
  bool res;
  uint8_t handover_cnt;

  LocalLockNode() : unique_read_key(0), unique_write_key(0), wc_buffer(0), unique_addr(0),
                    ret_value(0),
                    read_current(0), read_ticket(0), read_handover(false),
                    write_current(0), write_ticket(0), write_handover(false),
                    window_start(false), read_window(0), write_window(0),
                    r_lock(), w_lock(), res(false), handover_cnt(0) {}
};

static_assert(sizeof(LocalLockNode) == 64, "LocalLockNode must fit in a single 64B cache line");


class LocalLockTable {
public:
  LocalLockTable() {}

  // read-delegation
  std::pair<bool, bool> acquire_local_read_lock(const Key& k, CoroQueue *waiting_queue = nullptr, CoroPull* sink = nullptr);
  void release_local_read_lock(const Key& k, std::pair<bool, bool> acquire_ret, bool& res, Value& ret_value);

  // write-combining
  std::pair<bool, bool> acquire_local_write_lock(const Key& k, const Value& v, CoroQueue *waiting_queue = nullptr, CoroPull* sink = nullptr);
  bool get_combining_value(const Key& k, Value& v);
  void release_local_write_lock(const Key& k, std::pair<bool, bool> acquire_ret);

  /* ---- baseline ---- */
  // lock-handover
  bool acquire_local_lock(const GlobalAddress& addr, CoroQueue *waiting_queue = nullptr, CoroPull* sink = nullptr);
  using RemoteFunc = std::function<void (const GlobalAddress &)>;
  void release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func);
  void release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func, RemoteFunc write_without_unlock, RemoteFunc write_and_unlock);

  // cas-handover
  bool acquire_local_lock(const Key& k, CoroQueue *waiting_queue = nullptr, CoroPull* sink = nullptr);
  void release_local_lock(const Key& k, bool& res, InternalEntry& ret_p);

  // write_testing
  bool acquire_local_write_lock(const GlobalAddress& addr, const Value& v, CoroQueue *waiting_queue = nullptr, CoroPull* sink = nullptr);
  using RemoteWriteBackFunc = std::function<void (const Value&)>;
  void release_local_write_lock(const GlobalAddress& addr, RemoteFunc unlock_func, const Value& v, RemoteWriteBackFunc write_func);

  // read-testing
  bool acquire_local_read_lock(const GlobalAddress& addr, CoroQueue *waiting_queue = nullptr, CoroPull* sink = nullptr);
  void release_local_read_lock(const GlobalAddress& addr, bool& res, Value& ret_value);

private:
  LocalLockNode local_locks[define::kLocalLockNum];
};


// read-delegation
inline std::pair<bool, bool> LocalLockTable::acquire_local_read_lock(const Key& k, CoroQueue *waiting_queue, CoroPull* sink) {
  const uint64_t raw_k = key_to_u64_raw(k);
  if (__builtin_expect(raw_k == 0ULL, 0)) {
    return std::make_pair(false, true);
  }
  auto &node = local_locks[get_hashed_local_lock_index(k)];

  uint64_t unique_key = node.unique_read_key.load(std::memory_order_acquire);
  if (unique_key != raw_k) {
    if (unique_key != 0ULL) {
      return std::make_pair(false, true);
    }
    if (!node.unique_read_key.compare_exchange_strong(unique_key, raw_k,
                                                      std::memory_order_acq_rel,
                                                      std::memory_order_acquire)) {
      if (unique_key != raw_k) {  // conflict keys
        return std::make_pair(false, true);
      }
    }
  }

  uint8_t ticket = node.read_ticket.fetch_add(1);  // acquire local lock
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (sink != nullptr) {
      waiting_queue->push(sink->get());
      (*sink)();
    }
    current = node.read_current.load(std::memory_order_relaxed);
  }
  unique_key = node.unique_read_key.load(std::memory_order_acquire);
  if (unique_key != raw_k) {  // conflict keys
    if (node.read_window) {
      -- node.read_window;
      if (!node.read_window && !node.write_window) {
        node.window_start = false;
      }
    }
    // node.read_handover = false;
    node.read_current.fetch_add(1);
    return std::make_pair(false, true);
  }
  if (!node.read_window) {
    node.read_handover = false;
  }
  return std::make_pair(node.read_handover, false);
}

// read-delegation
inline void LocalLockTable::release_local_read_lock(const Key& k, std::pair<bool, bool> acquire_ret, bool& res, Value& ret_value) {
  if (acquire_ret.second) return;

  auto &node = local_locks[get_hashed_local_lock_index(k)];

  if (!node.read_handover) {  // winner
    node.res = res;
    node.ret_value = ret_value;
  }
  else {  // losers accept the ret val from winner
    res = node.res;
    ret_value = node.ret_value;
  }

  uint8_t ticket = node.read_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  bool start_window = false;
  if (!node.read_handover && node.window_start.compare_exchange_strong(start_window, true)) {
    // read time window start
    node.read_window = ((1UL << 8) + ticket - current) % (1UL << 8);

    node.w_lock.lock();
    auto w_current = node.write_current.load(std::memory_order_relaxed);
    node.write_window = ((1UL << 8) + node.write_ticket.load(std::memory_order_relaxed) - w_current) % (1UL << 8);
    node.w_lock.unlock();
  }

  node.read_handover = ticket != (uint8_t)(current + 1);

  if (!node.read_handover) {  // next epoch
    node.unique_read_key.store(0ULL, std::memory_order_release);
  }

  node.r_lock.lock();
  if (node.read_window) {
    -- node.read_window;
    if (!node.read_window && !node.write_window) {
      node.window_start = false;
    }
  }
  node.read_current.fetch_add(1);
  node.r_lock.unlock();

  return;
}

// write-combining
inline std::pair<bool, bool> LocalLockTable::acquire_local_write_lock(const Key& k, const Value& v, CoroQueue *waiting_queue, CoroPull* sink) {
  const uint64_t raw_k = key_to_u64_raw(k);
  if (__builtin_expect(raw_k == 0ULL, 0)) {
    return std::make_pair(false, true);
  }
  auto &node = local_locks[get_hashed_local_lock_index(k)];

  uint64_t unique_key = node.unique_write_key.load(std::memory_order_acquire);
  if (unique_key != raw_k) {
    if (unique_key != 0ULL) {
      return std::make_pair(false, true);
    }
    if (!node.unique_write_key.compare_exchange_strong(unique_key, raw_k,
                                                       std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
      if (unique_key != raw_k) {  // conflict keys
        return std::make_pair(false, true);
      }
    }
  }

  node.wc_buffer.store(v, std::memory_order_release);     // local overwrite (combining)

  uint8_t ticket = node.write_ticket.fetch_add(1);  // acquire local lock
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (sink != nullptr) {
      waiting_queue->push(sink->get());
      (*sink)();
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }
  unique_key = node.unique_write_key.load(std::memory_order_acquire);
  if (unique_key != raw_k) {  // conflict keys
    if (node.write_window) {
      -- node.write_window;
      if (!node.read_window && !node.write_window) {
        node.window_start = false;
      }
    }
    // node.write_handover = false;
    node.write_current.fetch_add(1);
    return std::make_pair(false, true);
  }
  if (!node.write_window) {
    node.write_handover = false;
  }
  return std::make_pair(node.write_handover, false);
}

// write-combining
inline bool LocalLockTable::get_combining_value(const Key& k, Value& v) {
  const uint64_t raw_k = key_to_u64_raw(k);
  if (__builtin_expect(raw_k == 0ULL, 0)) return false;
  auto &node = local_locks[get_hashed_local_lock_index(k)];
  bool res = false;
  uint64_t unique_key = node.unique_write_key.load(std::memory_order_acquire);
  if (unique_key == raw_k) {  // wc
    Value cur = node.wc_buffer.load(std::memory_order_acquire);
    res = (cur != v);
    v = cur;
  }
  return res;
}

// write-combining
inline void LocalLockTable::release_local_write_lock(const Key& k, std::pair<bool, bool> acquire_ret) {
  if (acquire_ret.second) return;

  auto &node = local_locks[get_hashed_local_lock_index(k)];

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  bool start_window = false;
  if (!node.write_handover && node.window_start.compare_exchange_strong(start_window, true)) {
    // write time window start
    node.r_lock.lock();
    auto r_current = node.read_current.load(std::memory_order_relaxed);
    node.read_window = ((1UL << 8) + node.read_ticket.load(std::memory_order_relaxed) - r_current) % (1UL << 8);
    node.r_lock.unlock();

    node.write_window = ((1UL << 8) + ticket - current) % (1UL << 8);
  }

  node.write_handover = ticket != (uint8_t)(current + 1);

  if (!node.write_handover) {  // next epoch
    node.unique_write_key.store(0ULL, std::memory_order_release);
  }

  node.w_lock.lock();
  if (node.write_window) {
    -- node.write_window;
    if (!node.read_window && !node.write_window) {
      node.window_start = false;
    }
  }
  node.write_current.fetch_add(1);
  node.w_lock.unlock();

  return;
}

// lock-handover
inline bool LocalLockTable::acquire_local_lock(const GlobalAddress& addr, CoroQueue *waiting_queue, CoroPull* sink) {
  auto &node = local_locks[get_hashed_local_lock_index(addr)];

  uint8_t ticket = node.write_ticket.fetch_add(1);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (sink != nullptr) {
      waiting_queue->push(sink->get());
      (*sink)();
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }

  if (!node.write_handover) {  // winner
    node.unique_addr = addr;
  }
  // if (node.unique_addr == addr) {
  //   node.handover_cnt ++;
  // }
  return node.write_handover && node.unique_addr == addr;  // only if updating at the same k can this update handover
}

// lock-handover
inline void LocalLockTable::release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func) {
  auto &node = local_locks[get_hashed_local_lock_index(addr)];

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);
  if (node.handover_cnt ++ > MAX_HOCL_HANDOVER) {
    node.write_handover = false;
  }
  if (!node.write_handover) {
    node.handover_cnt = 0;
  }

  if (node.unique_addr != addr) {
    unlock_func(addr);
  }
  if (!node.write_handover) {
    unlock_func(node.unique_addr);
  }

  node.write_current.fetch_add(1);
  return;
}

// lock-handover + embedding lock
inline void LocalLockTable::release_local_lock(const GlobalAddress& addr, RemoteFunc unlock_func, RemoteFunc write_without_unlock, RemoteFunc write_and_unlock) {
  auto &node = local_locks[get_hashed_local_lock_index(addr)];

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);
  if (node.handover_cnt ++ > MAX_HOCL_HANDOVER) {
    node.write_handover = false;
  }
  if (!node.write_handover) {
    node.handover_cnt = 0;
  }

  if (!node.write_handover) {
    if (node.unique_addr != addr) {
      unlock_func(node.unique_addr);
      write_and_unlock(addr);
    }
    else {
      write_and_unlock(addr);
    }
  }
  else {
    if (node.unique_addr != addr) {
      write_and_unlock(addr);
    }
    else {
      write_without_unlock(addr);
    }
  }

  node.write_current.fetch_add(1);
  return;
}

// cas-handover
inline bool LocalLockTable::acquire_local_lock(const Key& k, CoroQueue *waiting_queue, CoroPull* sink) {
  auto &node = local_locks[get_hashed_local_lock_index(k)];

  uint8_t ticket = node.write_ticket.fetch_add(1);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (sink != nullptr) {
      waiting_queue->push(sink->get());
      (*sink)();
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }

  const uint64_t raw_k = key_to_u64_raw(k);
  if (!node.write_handover) {  // winner
    node.unique_write_key.store(raw_k, std::memory_order_relaxed);
  }
  auto unique_key = node.unique_write_key.load(std::memory_order_relaxed);
  return node.write_handover && (raw_k != 0ULL && unique_key == raw_k);  // only if updating at the same k can this update handover
}

// cas-handover
inline void LocalLockTable::release_local_lock(const Key& k, bool& res, InternalEntry& ret_p) {
  auto &node = local_locks[get_hashed_local_lock_index(k)];

  const uint64_t raw_k = key_to_u64_raw(k);
  auto unique_key = node.unique_write_key.load(std::memory_order_relaxed);
  if (raw_k != 0ULL && unique_key == raw_k) {
    if (!node.write_handover) {  // winner
      node.res = res;
      node.ret_p = ret_p;
    }
    else {
      res = node.res;
      ret_p = node.ret_p;
    }
  }

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);
  if (node.handover_cnt ++ > MAX_HOCL_HANDOVER) {
    node.write_handover = false;
  }
  if (!node.write_handover) {
    node.handover_cnt = 0;
  }

  node.write_current.fetch_add(1);
  return;
}

// write-testing
inline bool LocalLockTable::acquire_local_write_lock(const GlobalAddress& addr, const Value& v, CoroQueue *waiting_queue, CoroPull* sink) {
  auto &node = local_locks[get_hashed_local_lock_index(addr)];

  node.wc_buffer.store(v, std::memory_order_release);     // local overwrite (combining)

  uint8_t ticket = node.write_ticket.fetch_add(1);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (sink != nullptr) {
      waiting_queue->push(sink->get());
      (*sink)();
    }
    current = node.write_current.load(std::memory_order_relaxed);
  }

  if (!node.write_handover) {  // winner
    node.unique_addr = addr;
  }
  return node.write_handover && node.unique_addr == addr;  // only if updating at the same k can this update handover
}

// write-testing
inline void LocalLockTable::release_local_write_lock(const GlobalAddress& addr, RemoteFunc unlock_func, const Value& v, RemoteWriteBackFunc write_func) {
  auto &node = local_locks[get_hashed_local_lock_index(addr)];

  if (!node.write_handover) {
    // unlock lock_node.unique_key
    Value wc_v = node.wc_buffer.load(std::memory_order_acquire);
    write_func(wc_v);
    unlock_func(node.unique_addr);
  }
  if (node.unique_addr != addr) {
    write_func(v);
    unlock_func(addr);
  }

  uint8_t ticket = node.write_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.write_current.load(std::memory_order_relaxed);

  node.write_handover = ticket != (uint8_t)(current + 1);

  node.write_current.fetch_add(1);
  return;
}

// read-testing
inline bool LocalLockTable::acquire_local_read_lock(const GlobalAddress& addr, CoroQueue *waiting_queue, CoroPull* sink) {
  auto &node = local_locks[get_hashed_local_lock_index(addr)];

  uint8_t ticket = node.read_ticket.fetch_add(1);
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  while (ticket != current) { // lock failed
    if (sink != nullptr) {
      waiting_queue->push(sink->get());
      (*sink)();
    }
    current = node.read_current.load(std::memory_order_relaxed);
  }

  if (!node.read_handover) {  // winner
    node.unique_addr = addr;
  }
  return node.read_handover && node.unique_addr == addr;  // only if updating at the same k can this update handover
}

// read-testing
inline void LocalLockTable::release_local_read_lock(const GlobalAddress& addr, bool& res, Value& ret_value) {
  auto &node = local_locks[get_hashed_local_lock_index(addr)];

  uint8_t ticket = node.read_ticket.load(std::memory_order_relaxed);
  uint8_t current = node.read_current.load(std::memory_order_relaxed);

  if (node.unique_addr == addr) {  // hash conflict clients is not involved in ret value handover
    if (!node.read_handover) {  // winner
      node.res = res;
      node.ret_value = ret_value;
    }
    else {  // losers accept the ret val from winner
      res = node.res;
      ret_value = node.ret_value;
    }
  }
  node.read_handover = ticket != (uint8_t)(current + 1);
  node.read_current.fetch_add(1);
  return;
}

#endif // _LOCAL_LOCK_TABLE_H_
