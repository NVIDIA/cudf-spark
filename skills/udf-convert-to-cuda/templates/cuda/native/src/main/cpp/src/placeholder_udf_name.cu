/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "placeholder_udf_name.hpp"

#include <cudf/column/column_factories.hpp>
#include <cudf/null_mask.hpp>
#include <cudf/types.hpp>

std::unique_ptr<cudf::column> placeholder_udf_name(cudf::column_view const& input)
{
  // TODO: Replace this placeholder with the actual CUDA/libcudf implementation.
  auto null_mask = cudf::create_null_mask(input.size(), cudf::mask_state::ALL_NULL);
  return cudf::make_numeric_column(
    cudf::data_type{cudf::type_id::INT32}, input.size(), std::move(null_mask), input.size());
}
