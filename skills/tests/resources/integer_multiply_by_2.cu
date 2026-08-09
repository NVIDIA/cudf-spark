/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "integer_multiply_by_2.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/default_stream.hpp>
#include <cudf/utilities/error.hpp>
#include <cudf/utilities/memory_resource.hpp>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/resource_ref.hpp>

#include <cstdint>
#include <stdexcept>

namespace {

__global__ void multiply_by_2_kernel(int32_t const* input, int32_t* output, cudf::size_type size)
{
  auto const idx = static_cast<cudf::size_type>(blockIdx.x * blockDim.x + threadIdx.x);
  if (idx < size) {
    output[idx] = input[idx] * 2;
  }
}

}  // namespace

std::unique_ptr<cudf::column> integer_multiply_by_2(
  cudf::column_view const& input,
  rmm::cuda_stream_view stream,
  rmm::device_async_resource_ref mr)
{
  if (input.type().id() != cudf::type_id::INT32) {
    throw std::invalid_argument("input must be INT32");
  }

  auto const row_count = input.size();
  auto null_mask       = cudf::copy_bitmask(input, stream, mr);
  auto result          = cudf::make_numeric_column(
    input.type(), row_count, std::move(null_mask), input.null_count(), stream, mr);

  if (row_count > 0) {
    constexpr int threads_per_block = 256;
    int const blocks = (row_count + threads_per_block - 1) / threads_per_block;
    multiply_by_2_kernel<<<blocks, threads_per_block, 0, stream.value()>>>(
      input.data<int32_t>(), result->mutable_view().data<int32_t>(), row_count);
    CUDF_CHECK_CUDA(stream.value());
  }

  return result;
}
