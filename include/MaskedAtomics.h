#ifndef CHIME_MASKED_ATOMICS_H
#define CHIME_MASKED_ATOMICS_H

#include "Debug.h"
#include <cerrno>
#include <cstdint>
#include <endian.h>
#include <infiniband/mlx5dv.h>

namespace chime_raw {

struct alignas(16) MaskedCas {
  mlx5_wqe_ctrl_seg control;
  mlx5_wqe_raddr_seg remote;
  __be64 swap;
  __be64 compare;
  __be64 swap_mask;
  __be64 compare_mask;
  mlx5_wqe_data_seg result;
};

struct alignas(16) BoundaryFaa {
  mlx5_wqe_ctrl_seg control;
  mlx5_wqe_raddr_seg remote;
  __be64 add;
  __be64 boundaries;
  mlx5_wqe_data_seg result;
};

static_assert(sizeof(MaskedCas) == 80, "64-bit masked CAS occupies five DS");
static_assert(sizeof(BoundaryFaa) == 64, "64-bit boundary FAA occupies four DS");

inline void control(mlx5_wqe_ctrl_seg &segment, uint32_t qpn, uint8_t opcode,
                    uint8_t ds, bool signal, bool fence) {
  // OFED's extended-atomic opmod is 0x08 | (log2(8) - 2).
  segment.opmod_idx_opcode = htobe32((0x09u << 24) | opcode);
  segment.qpn_ds = htobe32((qpn << 8) | ds);
  segment.fm_ce_se = (signal ? MLX5_WQE_CTRL_CQ_UPDATE : 0) |
                    (fence ? MLX5_WQE_CTRL_FENCE : 0);
}

inline MaskedCas cas(uint32_t qpn, uint64_t source, uint64_t destination,
                     uint32_t lkey, uint32_t rkey, uint64_t compare,
                     uint64_t swap, uint64_t compare_mask, uint64_t swap_mask,
                     bool signal, bool fence = false) {
  MaskedCas wqe{};
  control(wqe.control, qpn, MLX5_OPCODE_ATOMIC_MASKED_CS, sizeof(wqe) / 16,
          signal, fence);
  wqe.remote.raddr = htobe64(destination);
  wqe.remote.rkey = htobe32(rkey);
  wqe.swap = htobe64(swap);
  wqe.compare = htobe64(compare);
  wqe.swap_mask = htobe64(swap_mask);
  wqe.compare_mask = htobe64(compare_mask);
  mlx5dv_set_data_seg(&wqe.result, sizeof(uint64_t), lkey, source);
  return wqe;
}

inline BoundaryFaa faa(uint32_t qpn, uint64_t source, uint64_t destination,
                       uint32_t lkey, uint32_t rkey, uint64_t add,
                       uint64_t boundaries, bool signal, bool fence = false) {
  BoundaryFaa wqe{};
  control(wqe.control, qpn, MLX5_OPCODE_ATOMIC_MASKED_FA, sizeof(wqe) / 16,
          signal, fence);
  wqe.remote.raddr = htobe64(destination);
  wqe.remote.rkey = htobe32(rkey);
  wqe.add = htobe64(add);
  wqe.boundaries = htobe64(boundaries);
  mlx5dv_set_data_seg(&wqe.result, sizeof(uint64_t), lkey, source);
  return wqe;
}

inline bool post(ibv_qp *qp, const void *wqe, uint64_t wr_id) {
  ibv_qp_ex *extended = ibv_qp_to_qp_ex(qp);
  mlx5dv_qp_ex *direct = extended ? mlx5dv_qp_ex_from_ibv_qp_ex(extended) : nullptr;
  if (!direct) {
    Debug::notifyError("CHIME_RAW_WQE_FAIL missing extended mlx5 QP");
    errno = EOPNOTSUPP;
    return false;
  }
  ibv_wr_start(extended);
  extended->wr_id = wr_id;
  // raw_wqe ignores wr_flags; CQ update and fence are encoded in control.
  extended->wr_flags = 0;
  mlx5dv_wr_raw_wqe(direct, wqe);
  const int result = ibv_wr_complete(extended);
  if (result) {
    errno = result;
    Debug::notifyError("CHIME_RAW_WQE_FAIL ibv_wr_complete: %d", result);
    return false;
  }
  return true;
}
}  // namespace chime_raw

#endif
