#include "Rdma.h"

int kMaxDeviceMemorySize = 0;

void rdmaQueryQueuePair(ibv_qp *qp) {
  struct ibv_qp_attr attr;
  struct ibv_qp_init_attr init_attr;
  ibv_query_qp(qp, &attr, IBV_QP_STATE, &init_attr);
  switch (attr.qp_state) {
  case IBV_QPS_RESET:
    printf("QP state: IBV_QPS_RESET\n");
    break;
  case IBV_QPS_INIT:
    printf("QP state: IBV_QPS_INIT\n");
    break;
  case IBV_QPS_RTR:
    printf("QP state: IBV_QPS_RTR\n");
    break;
  case IBV_QPS_RTS:
    printf("QP state: IBV_QPS_RTS\n");
    break;
  case IBV_QPS_SQD:
    printf("QP state: IBV_QPS_SQD\n");
    break;
  case IBV_QPS_SQE:
    printf("QP state: IBV_QPS_SQE\n");
    break;
  case IBV_QPS_ERR:
    printf("QP state: IBV_QPS_ERR\n");
    break;
  case IBV_QPS_UNKNOWN:
    printf("QP state: IBV_QPS_UNKNOWN\n");
    break;
  }
}

void checkDMSupported(struct ibv_context *ctx) {
  struct ibv_device_attr_ex attrs{};
  if (ibv_query_device_ex(ctx, nullptr, &attrs) || !attrs.max_dm_size) {
    fprintf(stderr, "Cannot query required device memory support\n");
    exit(-1);
  } else {
    kMaxDeviceMemorySize = attrs.max_dm_size;
    printf("NIC Device Memory is %dKB\n", kMaxDeviceMemorySize / 1024);
  }
}
