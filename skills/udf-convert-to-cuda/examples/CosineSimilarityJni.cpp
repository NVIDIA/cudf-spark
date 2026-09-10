/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "cosine_similarity.hpp"

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/lists/lists_column_view.hpp>

#include <jni.h>

#include <memory>
#include <string>

namespace {

constexpr char const* RUNTIME_ERROR_CLASS = "java/lang/RuntimeException";
constexpr char const* ILLEGAL_ARG_CLASS = "java/lang/IllegalArgumentException";

void throw_java_exception(JNIEnv* env, char const* class_name, char const* message)
{
  jclass ex_class = env->FindClass(class_name);
  if (ex_class != nullptr) {
    env->ThrowNew(ex_class, message);
  }
}

}  // namespace

extern "C" {

JNIEXPORT jlong JNICALL
Java_com_udf_CosineSimilarityNativeRapidsUDF_cosineSimilarity(JNIEnv* env,
                                                              jclass,
                                                              jlong j_view1,
                                                              jlong j_view2)
{
  try {
    auto v1 = reinterpret_cast<cudf::column_view const*>(j_view1);
    auto v2 = reinterpret_cast<cudf::column_view const*>(j_view2);
    if (v1 == nullptr || v2 == nullptr) {
      throw_java_exception(env, ILLEGAL_ARG_CLASS, "input column view is null");
      return 0;
    }
    if (v1->type().id() != v2->type().id() || v1->type().id() != cudf::type_id::LIST) {
      throw_java_exception(env, ILLEGAL_ARG_CLASS, "inputs are not list columns");
      return 0;
    }

    auto lv1 = cudf::lists_column_view(*v1);
    auto lv2 = cudf::lists_column_view(*v2);
    std::unique_ptr<cudf::column> result = cosine_similarity(lv1, lv2);
    return reinterpret_cast<jlong>(result.release());
  } catch (std::bad_alloc const& e) {
    auto message = std::string("Unable to allocate native memory: ") + e.what();
    throw_java_exception(env, RUNTIME_ERROR_CLASS, message.c_str());
  } catch (std::invalid_argument const& e) {
    throw_java_exception(env, ILLEGAL_ARG_CLASS, e.what());
  } catch (std::exception const& e) {
    throw_java_exception(env, RUNTIME_ERROR_CLASS, e.what());
  }
  return 0;
}

}
