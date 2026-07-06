/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

/**
 * Automatic resource management (ARM).
 */
object Arm {
  /**
   * Helper to auto-close GPU resources after use.
   * 
   * @param resource The AutoCloseable resource
   * @param f The function to execute with the resource
   * @return The result of the function
   */
  def withResource[T <: AutoCloseable, R](resource: T)(f: T => R): R = {
    try {
      f(resource)
    } finally {
      if (resource != null) {
        resource.close()
      }
    }
  }

  /**
   * Helper to auto-close GPU resources on an exception.
   * 
   * @param resource The AutoCloseable resource
   * @param f The function to execute with the resource
   * @return The result of the function
   */
  def closeOnExcept[T <: AutoCloseable, R](resource: T)(f: T => R): R = {
    try {
      f(resource)
    } catch {
      case e: Exception =>
        if (resource != null) {
          try {
            resource.close()
          } catch {
            case closeException: Exception =>
              e.addSuppressed(closeException)
          }
        }
        throw e
    }
  }

  /** 
   * Close all resources in an array, skipping nulls.
   * 
   * @param resources The array of resources to close
   */
  def closeAll[T <: AutoCloseable](resources: Array[T]): Unit = {
    if (resources != null) {
      resources.foreach { r =>
        if (r != null) {
          try { r.close() } catch { case _: Exception => }
        }
      }
    } 
  }
}
