/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "integer_multiply_by_2.hpp"

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/types.hpp>

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
Java_com_udf_IntegerMultiplyBy2NativeRapidsUDF_integerMultiplyBy2(JNIEnv* env,
                                                                  jclass,
                                                                  jlong input_view)
{
  try {
    auto input = reinterpret_cast<cudf::column_view const*>(input_view);
    if (input == nullptr) {
      throw_java_exception(env, ILLEGAL_ARG_CLASS, "input column view is null");
      return 0;
    }
    if (input->type().id() != cudf::type_id::INT32) {
      throw_java_exception(env, ILLEGAL_ARG_CLASS, "input must be INT32");
      return 0;
    }

    std::unique_ptr<cudf::column> result = integer_multiply_by_2(*input);
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
