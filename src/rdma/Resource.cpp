#include "Rdma.h"
#include <infiniband/mlx5dv.h>
#include <vector>

bool createContext(RdmaContext *context, uint8_t port, int gidIndex,
                   uint8_t devIndex) {

  ibv_device *dev = NULL;
  ibv_context *ctx = NULL;
  ibv_pd *pd = NULL;
  ibv_port_attr portAttr;

  // get device names in the system
  int devicesNum;
  struct ibv_device **deviceList = ibv_get_device_list(&devicesNum);
  if (!deviceList) {
    Debug::notifyError("failed to get IB devices list");
    goto CreateResourcesExit;
  }

  // if there isn't any IB device in host
  if (!devicesNum) {
    Debug::notifyInfo("found %d device(s)", devicesNum);
    goto CreateResourcesExit;
  }
  // Debug::notifyInfo("Open IB Device");

  for (int i = 0; i < devicesNum; ++i) {
    // printf("Device %d: %s\n", i, ibv_get_device_name(deviceList[i]));
    if (ibv_get_device_name(deviceList[i])[5] == IB_DEV_NAME_IDX) {
      devIndex = i;
      break;
    }
  }

  if (devIndex >= devicesNum) {
    Debug::notifyError("ib device wasn't found");
    goto CreateResourcesExit;
  }

  dev = deviceList[devIndex];
  // printf("I open %s :)\n", ibv_get_device_name(dev));

  // get device handle
  ctx = ibv_open_device(dev);
  if (!ctx) {
    Debug::notifyError("failed to open device");
    goto CreateResourcesExit;
  }
  /* We are now done with device list, free it */
  ibv_free_device_list(deviceList);
  deviceList = NULL;

  // query port properties
  if (ibv_query_port(ctx, port, &portAttr)) {
    Debug::notifyError("ibv_query_port failed");
    goto CreateResourcesExit;
  }

  // allocate Protection Domain
  // Debug::notifyInfo("Allocate Protection Domain");
  pd = ibv_alloc_pd(ctx);
  if (!pd) {
    Debug::notifyError("ibv_alloc_pd failed");
    goto CreateResourcesExit;
  }

  if (ibv_query_gid(ctx, port, gidIndex, &context->gid)) {
    Debug::notifyError("could not get gid for port: %d, gidIndex: %d", port,
                       gidIndex);
    goto CreateResourcesExit;
  }

  // Success :)
  context->devIndex = devIndex;
  context->gidIndex = gidIndex;
  context->port = port;
  context->ctx = ctx;
  context->pd = pd;
  context->lid = portAttr.lid;

  // check device memory support
#ifdef TREE_TEST_HOCL_HANDOVER
  if (kMaxDeviceMemorySize == 0) {
    checkDMSupported(ctx);
  }
#endif

  return true;

/* Error encountered, cleanup */
CreateResourcesExit:
  Debug::notifyError("Error Encountered, Cleanup ...");

  if (pd) {
    ibv_dealloc_pd(pd);
    pd = NULL;
  }
  if (ctx) {
    ibv_close_device(ctx);
    ctx = NULL;
  }
  if (deviceList) {
    ibv_free_device_list(deviceList);
    deviceList = NULL;
  }

  return false;
}

bool destoryContext(RdmaContext *context) {
  bool rc = true;
  if (context->pd) {
    if (ibv_dealloc_pd(context->pd)) {
      Debug::notifyError("Failed to deallocate PD");
      rc = false;
    }
  }
  if (context->ctx) {
    if (ibv_close_device(context->ctx)) {
      Debug::notifyError("failed to close device context");
      rc = false;
    }
  }

  return rc;
}

ibv_mr *createMemoryRegion(uint64_t mm, uint64_t mmSize, RdmaContext *ctx) {

  ibv_mr *mr = NULL;
  mr = ibv_reg_mr(ctx->pd, (void *)mm, mmSize,
                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);

  if (!mr) {
    Debug::notifyError("Memory registration failed");
  }

  return mr;
}

ibv_mr *createMemoryRegionOnChip(uint64_t mm, uint64_t mmSize,
                                 RdmaContext *ctx) {

  struct ibv_alloc_dm_attr dm_attr{};
  dm_attr.length = mmSize;
  struct ibv_dm *dm = ibv_alloc_dm(ctx->ctx, &dm_attr);
  if (!dm) {
    Debug::notifyError("Allocate on-chip memory failed");
    return nullptr;
  }

  struct ibv_mr *mr = ibv_reg_dm_mr(ctx->pd, dm, mm, mmSize,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
      IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_ZERO_BASED);
  if (!mr) {
    Debug::notifyError("Memory registration failed");
    ibv_free_dm(dm);
    return nullptr;
  }

  std::vector<char> zeros(mmSize, 0);
  if (ibv_memcpy_to_dm(dm, 0, zeros.data(), mmSize)) {
    Debug::notifyError("Device memory initialization failed");
    ibv_dereg_mr(mr);
    ibv_free_dm(dm);
    return nullptr;
  }

  return mr;
}

bool createQueuePair(ibv_qp **qp, ibv_qp_type mode, ibv_cq *send_cq,
                     ibv_cq *recv_cq, RdmaContext *context,
                     uint32_t qpsMaxDepth, uint32_t maxInlineData) {

  struct ibv_qp_init_attr_ex attr;
  memset(&attr, 0, sizeof(attr));

  attr.qp_type = mode;
  attr.sq_sig_all = 0;
  attr.send_cq = send_cq;
  attr.recv_cq = recv_cq;
  attr.pd = context->pd;

  attr.comp_mask = IBV_QP_INIT_ATTR_PD;

  attr.cap.max_send_wr = qpsMaxDepth;
  attr.cap.max_recv_wr = qpsMaxDepth;
  attr.cap.max_send_sge = 1;
  attr.cap.max_recv_sge = 1;
  attr.cap.max_inline_data = maxInlineData;

  if (mode == IBV_QPT_RC) {
    attr.comp_mask |= IBV_QP_INIT_ATTR_SEND_OPS_FLAGS;
    attr.send_ops_flags = IBV_QP_EX_WITH_SEND | IBV_QP_EX_WITH_RDMA_READ |
                         IBV_QP_EX_WITH_RDMA_WRITE |
                         IBV_QP_EX_WITH_ATOMIC_CMP_AND_SWP |
                         IBV_QP_EX_WITH_ATOMIC_FETCH_AND_ADD;
    mlx5dv_qp_init_attr direct{};
    direct.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_SEND_OPS_FLAGS |
                       MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS;
    direct.send_ops_flags = MLX5DV_QP_EX_WITH_RAW_WQE;
    // Modern CQ scatter decoding does not understand masked atomic opcodes.
    direct.create_flags = MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
    *qp = mlx5dv_create_qp(context->ctx, &attr, &direct);
  } else {
    *qp = ibv_create_qp_ex(context->ctx, &attr);
  }
  if (!(*qp)) {
    Debug::notifyError("Failed to create QP");
    return false;
  }

  // Debug::notifyInfo("Create Queue Pair with Num = %d", (*qp)->qp_num);

  return true;
}

bool createQueuePair(ibv_qp **qp, ibv_qp_type mode, ibv_cq *cq,
                     RdmaContext *context, uint32_t qpsMaxDepth,
                     uint32_t maxInlineData) {
  return createQueuePair(qp, mode, cq, cq, context, qpsMaxDepth, maxInlineData);
}

void fillAhAttr(ibv_ah_attr *attr, uint32_t remoteLid, uint8_t *remoteGid,
                RdmaContext *context) {

  (void)remoteGid;

  memset(attr, 0, sizeof(ibv_ah_attr));
  attr->dlid = remoteLid;
  attr->sl = 0;
  attr->src_path_bits = 0;
  attr->port_num = context->port;

  // attr->is_global = 0;

  // fill ah_attr with GRH
  attr->is_global = 1;
  memcpy(&attr->grh.dgid, remoteGid, 16);
  attr->grh.flow_label = 0;
  attr->grh.hop_limit = 1;
  attr->grh.sgid_index = context->gidIndex;
  attr->grh.traffic_class = 0;
}
