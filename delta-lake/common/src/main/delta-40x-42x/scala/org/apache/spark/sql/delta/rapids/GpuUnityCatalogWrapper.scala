/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.rapids

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Modifier, Proxy}

import scala.util.control.NonFatal

import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.catalog.DeltaCatalog

/**
 * Wraps OSS Unity Catalog's `UCSingleCatalog` so that its staging calls reach a GPU Delta catalog
 * while every other catalog operation keeps its original behavior.
 *
 * UC owns staging-table allocation, managed locations, credentials, and catalog commit. Its
 * private delegate is Delta's catalog. A staging-only copy routes that delegate through the GPU
 * Delta catalog so CPU sessions and non-stage catalog operations retain the original behavior.
 */
object GpuUnityCatalogWrapper {
  val UnityCatalogClassName = "io.unitycatalog.spark.UCSingleCatalog"

  def isUnityCatalog(catalog: StagingTableCatalog): Boolean = {
    catalog.getClass.getCanonicalName == UnityCatalogClassName
  }

  /** Instance fields this class inherits, qualified by the class that declares them. */
  private def inheritedInstanceFields(clazz: Class[_]): Seq[String] = {
    Option(clazz.getSuperclass).toSeq.flatMap { superClass =>
      superClass.getDeclaredFields
        .filterNot(field => Modifier.isStatic(field.getModifiers))
        .map(field => s"${superClass.getName}.${field.getName}") ++
        inheritedInstanceFields(superClass)
    }
  }

  /**
   * Checks whether [[wrap]] understands the internals of this Unity Catalog build.
   *
   * The wrapper depends on private state that is not part of any public API. Unity Catalog
   * versions other than the one this integration was written against are still accepted, so these
   * checks run while tagging: an unrecognized build then falls back to the CPU with a reason
   * instead of failing the query part-way through plan conversion.
   *
   * @param gpuCatalogClass the GPU Delta catalog class that will become the delegate
   * @return the reason the catalog cannot be wrapped, or `None` when it can be
   */
  def unsupportedReason(
      catalog: StagingTableCatalog,
      gpuCatalogClass: Class[_ <: StagingTableCatalog]): Option[String] = {
    val catalogClass = catalog.getClass
    try {
      // A staging-only copy is built by cloning every declared field, so state declared by a
      // superclass would be silently dropped rather than reported.
      val inherited = inheritedInstanceFields(catalogClass)
      val delegateField = catalogClass.getDeclaredField("delegate")
      delegateField.setAccessible(true)
      catalogClass.getDeclaredConstructor()
      if (inherited.nonEmpty) {
        Some(s"$UnityCatalogClassName declares inherited state ${inherited.mkString(", ")} that " +
          "the GPU Delta catalog wrapper does not know how to copy")
      } else if (!delegateField.getType.isAssignableFrom(gpuCatalogClass)) {
        Some(s"$UnityCatalogClassName delegate field of type ${delegateField.getType.getName} " +
          "cannot hold the GPU Delta catalog")
      } else if (!delegateField.get(catalog).isInstanceOf[DeltaCatalog]) {
        Some(s"$UnityCatalogClassName delegate " +
          s"${delegateField.get(catalog).getClass.getName} is not a Delta catalog")
      } else {
        None
      }
    } catch {
      case NonFatal(e) =>
        Some(s"$UnityCatalogClassName internals are not recognized by the GPU Delta catalog " +
          s"wrapper: $e")
    }
  }

  /**
   * Returns the CPU Delta catalog that backs a Unity Catalog, which must already have passed
   * [[unsupportedReason]].
   */
  def deltaCatalog(catalog: StagingTableCatalog): DeltaCatalog = {
    val delegateField = catalog.getClass.getDeclaredField("delegate")
    delegateField.setAccessible(true)
    delegateField.get(catalog) match {
      case delta: DeltaCatalog => delta
      case other => throw new IllegalStateException(
        s"OSS Unity Catalog delegate ${other.getClass} is not DeltaCatalog")
    }
  }

  /**
   * Wraps OSS Unity Catalog while preserving its staging protocol.
   *
   * @param newGpuCatalog builds the GPU Delta catalog around the CPU Delta catalog
   */
  def wrap(
      catalog: StagingTableCatalog,
      gpuCatalogClass: Class[_ <: StagingTableCatalog],
      newGpuCatalog: DeltaCatalog => StagingTableCatalog): StagingTableCatalog = {
    require(isUnityCatalog(catalog), s"Expected OSS Unity Catalog, found ${catalog.getClass}")
    // Tagging rejects these catalogs, so reaching here means the check above was skipped.
    unsupportedReason(catalog, gpuCatalogClass).foreach(reason =>
      throw new IllegalStateException(reason))

    val delegateField = catalog.getClass.getDeclaredField("delegate")
    delegateField.setAccessible(true)
    val stagingCatalog = catalog.getClass.getDeclaredConstructor().newInstance()
      .asInstanceOf[StagingTableCatalog]
    catalog.getClass.getDeclaredFields
      .filterNot(field => Modifier.isStatic(field.getModifiers))
      .foreach { field =>
        field.setAccessible(true)
        field.set(stagingCatalog, field.get(catalog))
      }
    delegateField.set(stagingCatalog, newGpuCatalog(deltaCatalog(catalog)))

    val handler = new InvocationHandler {
      override def invoke(proxy: Object, method: Method, args: Array[Object]): Object = {
        def invokeCatalog(target: Object): Object = {
          try {
            method.invoke(target, Option(args).getOrElse(Array.empty[Object]): _*)
          } catch {
            case error: InvocationTargetException => throw error.getCause
          }
        }

        if (method.getName.startsWith("stage")) {
          invokeCatalog(stagingCatalog)
        } else {
          invokeCatalog(catalog)
        }
      }
    }

    Proxy.newProxyInstance(
      catalog.getClass.getClassLoader,
      Array(classOf[StagingTableCatalog]),
      handler).asInstanceOf[StagingTableCatalog]
  }
}
