#ifndef CHIME_CPU_AFFINITY_H
#define CHIME_CPU_AFFINITY_H

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <map>
#include <pthread.h>
#include <sched.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/syscall.h>
#include <unistd.h>
#include <utility>
#include <vector>

class CpuAffinity {
public:
  std::vector<int> allowed, workers;
  int main_cpu, directory_cpu;

  CpuAffinity(std::vector<int> cpus, const std::map<int, std::vector<int>>& siblings,
              unsigned worker_count, bool has_directory)
      : allowed(std::move(cpus)), main_cpu(-1), directory_cpu(-1) {
    std::sort(allowed.begin(), allowed.end());
    allowed.erase(std::unique(allowed.begin(), allowed.end()), allowed.end());
    if (allowed.size() < worker_count + 1 + unsigned(has_directory))
      throw std::runtime_error("CHIME_CPU_AFFINITY_FAIL insufficient allowed CPUs");
    main_cpu = allowed[0];
    if (has_directory) {
      directory_cpu = allowed[1];
      bool paired = false;
      for (int cpu : allowed) {
        for (int sibling : siblings.at(cpu)) {
          if (sibling > cpu && std::binary_search(allowed.begin(), allowed.end(), sibling)) {
            main_cpu = cpu;
            directory_cpu = sibling;
            paired = true;
            break;
          }
        }
        if (paired) break;
      }
    }
    for (int cpu : allowed)
      if (cpu != main_cpu && cpu != directory_cpu) workers.push_back(cpu);
    workers.resize(worker_count);
  }

  static CpuAffinity capture(unsigned workers, bool has_directory) {
    auto mask = cpuMask();
    const size_t bytes = mask.size() * sizeof(cpu_set_t);
    if (sched_getaffinity(0, bytes, mask.data()))
      throw std::runtime_error(std::string("CHIME_CPU_AFFINITY_FAIL sched_getaffinity: ") + std::strerror(errno));
    std::vector<int> allowed;
    std::map<int, std::vector<int>> siblings;
    for (int cpu : cpuList("/sys/devices/system/cpu/online")) {
      if (CPU_ISSET_S(cpu, bytes, mask.data())) {
        allowed.push_back(cpu);
        siblings[cpu] = cpuList("/sys/devices/system/cpu/cpu" + std::to_string(cpu) +
                               "/topology/thread_siblings_list");
      }
    }
    return CpuAffinity(std::move(allowed), siblings, workers, has_directory);
  }

  void bindMain() const { bind(main_cpu, "main", 0); }
  void bindWorker(unsigned index) const { bind(workers.at(index), "worker", index); }
  void bindDirectory() const { bind(directory_cpu, "directory", 0); }

  void log(unsigned node) const {
    std::printf("CHIME_CPU_LAYOUT node=%u allowed=%s main=%d directory=%d workers=%s\n",
                node, list(allowed).c_str(), main_cpu, directory_cpu, list(workers).c_str());
    std::fflush(stdout);
  }

private:
  static std::vector<int> cpuList(const std::string& path) {
    std::ifstream input(path);
    std::string text, token;
    if (!(input >> text))
      throw std::runtime_error("CHIME_CPU_AFFINITY_FAIL cannot read " + path);
    std::vector<int> result;
    std::istringstream parts(text);
    while (std::getline(parts, token, ',')) {
      auto number = [&](size_t begin, size_t end) {
        int value = -1;
        auto parsed = std::from_chars(token.data() + begin, token.data() + end, value);
        if (parsed.ec != std::errc{} || parsed.ptr != token.data() + end || value < 0)
          throw std::runtime_error("CHIME_CPU_AFFINITY_FAIL invalid CPU list in " + path);
        return value;
      };
      size_t dash = token.find('-');
      int first = number(0, dash == std::string::npos ? token.size() : dash);
      int last = dash == std::string::npos ? first : number(dash + 1, token.size());
      if (last < first)
        throw std::runtime_error("CHIME_CPU_AFFINITY_FAIL reversed CPU range in " + path);
      for (int cpu = first; cpu <= last; ++cpu) result.push_back(cpu);
    }
    if (result.empty())
      throw std::runtime_error("CHIME_CPU_AFFINITY_FAIL empty CPU list in " + path);
    return result;
  }

  static std::vector<cpu_set_t> cpuMask() {
    static const int largest = cpuList("/sys/devices/system/cpu/possible").back();
    return std::vector<cpu_set_t>(largest / CPU_SETSIZE + 1);
  }

  static std::string list(const std::vector<int>& cpus) {
    std::string result;
    for (int cpu : cpus) {
      if (!result.empty()) result += ',';
      result += std::to_string(cpu);
    }
    return result;
  }

  void bind(int cpu, const char* role, unsigned index) const {
    if (!std::binary_search(allowed.begin(), allowed.end(), cpu))
      throw std::runtime_error("CHIME_CPU_AFFINITY_FAIL CPU is outside the startup mask");
    auto mask = cpuMask();
    const size_t bytes = mask.size() * sizeof(cpu_set_t);
    CPU_SET_S(cpu, bytes, mask.data());
    int error = pthread_setaffinity_np(pthread_self(), bytes, mask.data());
    if (error)
      throw std::runtime_error(std::string("CHIME_CPU_AFFINITY_FAIL setaffinity: ") + std::strerror(error));
    CPU_ZERO_S(bytes, mask.data());
    error = pthread_getaffinity_np(pthread_self(), bytes, mask.data());
    if (error || CPU_COUNT_S(bytes, mask.data()) != 1 || !CPU_ISSET_S(cpu, bytes, mask.data()))
      throw std::runtime_error("CHIME_CPU_AFFINITY_FAIL affinity readback differs");
    std::printf("CHIME_CPU_BIND role=%s index=%u cpu=%d tid=%ld\n",
                role, index, cpu, syscall(SYS_gettid));
    std::fflush(stdout);
  }
};

#endif
