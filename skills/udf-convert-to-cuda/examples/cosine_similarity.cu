/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "cosine_similarity.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/lists/lists_column_view.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/utilities/bit.hpp>
#include <cudf/utilities/type_checks.hpp>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_uvector.hpp>
#include <rmm/exec_policy.hpp>

#include <cuda/std/cmath>

#include <thrust/iterator/counting_iterator.h>
#include <thrust/logical.h>
#include <thrust/transform.h>

namespace {

struct cosine_similarity_functor {
  float const* const v1;
  float const* const v2;
  int32_t const* const v1_offsets;
  int32_t const* const v2_offsets;

  __device__ float operator()(cudf::size_type row_idx)
  {
    auto const v1_start_idx = v1_offsets[row_idx];
    auto const v1_num_elems = v1_offsets[row_idx + 1] - v1_start_idx;
    auto const v2_start_idx = v2_offsets[row_idx];
    auto const v2_num_elems = v2_offsets[row_idx + 1] - v2_start_idx;

    double magnitude1 = 0;
    double magnitude2 = 0;
    double dot_product = 0;
    for (auto i = 0; i < v1_num_elems; i++) {
      float const f1 = v1[v1_start_idx + i];
      float const f2 = v2[v2_start_idx + i];
      magnitude1 += f1 * f1;
      magnitude2 += f2 * f2;
      dot_product += f1 * f2;
    }
    return static_cast<float>(dot_product / (cuda::std::sqrt(magnitude1) * cuda::std::sqrt(magnitude2)));
  }
};

}  // namespace

std::unique_ptr<cudf::column> cosine_similarity(cudf::lists_column_view const& lv1,
                                                cudf::lists_column_view const& lv2,
                                                rmm::cuda_stream_view stream,
                                                rmm::device_async_resource_ref mr)
{
  if (!cudf::have_same_types(lv1.child(), lv2.child()) ||
      lv1.child().type().id() != cudf::type_id::FLOAT32) {
    throw std::invalid_argument("inputs are not lists of floats");
  }

  auto const row_count = lv1.size();
  if (row_count != lv2.size()) {
    throw std::invalid_argument("input row counts do not match");
  }
  if (row_count == 0) {
    return cudf::make_empty_column(cudf::data_type{cudf::type_id::FLOAT32});
  }
  if (lv1.child().null_count() != 0 || lv2.child().null_count() != 0) {
    throw std::invalid_argument("null floats are not supported");
  }

  auto const lv1_offsets_ptr = lv1.offsets().data<int32_t>();
  auto const lv2_offsets_ptr = lv2.offsets().data<int32_t>();
  auto const lv1_null_mask = lv1.parent().null_mask();
  auto const lv2_null_mask = lv2.parent().null_mask();

  bool const are_offsets_equal =
    thrust::all_of(rmm::exec_policy_nosync(stream),
                   thrust::make_counting_iterator<cudf::size_type>(0),
                   thrust::make_counting_iterator<cudf::size_type>(row_count),
                   [lv1_offsets_ptr, lv2_offsets_ptr, lv1_null_mask, lv2_null_mask]
                   __device__(cudf::size_type idx) -> bool {
                     bool const lv1_is_null =
                       lv1_null_mask != nullptr && !cudf::bit_is_set(lv1_null_mask, idx);
                     bool const lv2_is_null =
                       lv2_null_mask != nullptr && !cudf::bit_is_set(lv2_null_mask, idx);
                     if (lv1_is_null || lv2_is_null) {
                       return true;
                     }
                     return (lv1_offsets_ptr[idx + 1] - lv1_offsets_ptr[idx]) ==
                            (lv2_offsets_ptr[idx + 1] - lv2_offsets_ptr[idx]);
                   });
  if (!are_offsets_equal) {
    throw std::invalid_argument("input list lengths do not match for every row");
  }

  rmm::device_uvector<float> float_results(row_count, stream, mr);
  thrust::transform(rmm::exec_policy_nosync(stream),
                    thrust::make_counting_iterator<cudf::size_type>(0),
                    thrust::make_counting_iterator<cudf::size_type>(row_count),
                    float_results.data(),
                    cosine_similarity_functor({lv1.child().data<float>(),
                                               lv2.child().data<float>(),
                                               lv1.offsets().data<int32_t>(),
                                               lv2.offsets().data<int32_t>()}));

  auto [null_mask, null_count] =
    cudf::bitmask_and(cudf::table_view({lv1.parent(), lv2.parent()}), stream, mr);
  return std::make_unique<cudf::column>(cudf::data_type{cudf::type_id::FLOAT32},
                                        row_count,
                                        float_results.release(),
                                        std::move(null_mask),
                                        null_count);
}
