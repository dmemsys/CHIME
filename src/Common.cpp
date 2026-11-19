#include "Common.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <memory>

namespace {
std::unique_ptr<CpuAffinity> affinity;
}

void initCpuAffinity(unsigned workers, bool has_directory, unsigned node) {
    if (affinity)
        throw std::runtime_error("CHIME_CPU_AFFINITY_FAIL layout already initialized");
    // Capture once, before main or Directory narrows the mask inherited by workers.
    affinity = std::make_unique<CpuAffinity>(CpuAffinity::capture(workers, has_directory));
    affinity->log(node);
    affinity->bindMain();
}

const CpuAffinity& cpuAffinity() {
    if (!affinity)
        throw std::runtime_error("CHIME_CPU_AFFINITY_FAIL layout not initialized");
    return *affinity;
}

char *getIP() {
    struct ifreq ifr;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    ifr.ifr_addr.sa_family = AF_INET;
    strncpy(ifr.ifr_name, NET_DEV_NAME, IFNAMSIZ - 1);

    ioctl(fd, SIOCGIFADDR, &ifr);
    close(fd);

    return inet_ntoa(((struct sockaddr_in*)&ifr.ifr_addr)->sin_addr);
}

char *getMac() {
    static struct ifreq ifr;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    ifr.ifr_addr.sa_family = AF_INET;
    strncpy(ifr.ifr_name, "ens2", IFNAMSIZ - 1);

    ioctl(fd, SIOCGIFHWADDR, &ifr);
    close(fd);

    return (char *)ifr.ifr_hwaddr.sa_data;
}
