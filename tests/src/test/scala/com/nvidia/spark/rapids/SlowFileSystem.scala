/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.io.OutputStream
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

/**
 * A slow filesystem wrapper that adds artificial delays to write operations
 * for testing purposes. This is useful for making Parquet write operations
 * take longer to ensure operator time metrics are properly captured.
 */
class SlowFileSystem extends FileSystem {

  private var wrappedFs: FileSystem = _
  private var writeDelayMs: Long = 100L  // Default 100ms delay per write operation

  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    
    // Get the write delay from configuration
    writeDelayMs = conf.getLong("slowfs.write.delay.ms", 100L)
    
    // Create the underlying local filesystem
    val localFs = FileSystem.getLocal(conf)
    wrappedFs = localFs
    wrappedFs.initialize(uri, conf)
  }

  override def getUri: URI = {
    if (wrappedFs != null) wrappedFs.getUri else null
  }

  // Check if path is a slowfs path
  private def isSlowfsPath(f: Path): Boolean = {
    f.toString.startsWith("slowfs:/")
  }

  // Convert slowfs:/ path to file:/ path
  private def convertPath(f: Path): Path = {
    val pathStr = f.toString
    if (pathStr.startsWith("slowfs:/")) {
      new Path(pathStr.replace("slowfs:/", "file:/"))
    } else {
      f
    }
  }

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    
    if (isSlowfsPath(f)) {
      val underlying = wrappedFs.create(convertPath(f), permission, overwrite, 
        bufferSize, replication, blockSize, progress)
      new FSDataOutputStream(new SlowOutputStream(underlying, writeDelayMs), null)
    } else {
      wrappedFs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)
    }
  }

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    if (isSlowfsPath(f)) {
      wrappedFs.open(convertPath(f), bufferSize)
    } else {
      wrappedFs.open(f, bufferSize)
    }
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    if (isSlowfsPath(f)) {
      val underlying = wrappedFs.append(convertPath(f), bufferSize, progress)
      new FSDataOutputStream(new SlowOutputStream(underlying, writeDelayMs), null)
    } else {
      wrappedFs.append(f, bufferSize, progress)
    }
  }

  override def rename(src: Path, dst: Path): Boolean = {
    val srcPath = if (isSlowfsPath(src)) convertPath(src) else src
    val dstPath = if (isSlowfsPath(dst)) convertPath(dst) else dst
    wrappedFs.rename(srcPath, dstPath)
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    if (isSlowfsPath(f)) {
      wrappedFs.delete(convertPath(f), recursive)
    } else {
      wrappedFs.delete(f, recursive)
    }
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    if (isSlowfsPath(f)) {
      wrappedFs.listStatus(convertPath(f))
    } else {
      wrappedFs.listStatus(f)
    }
  }

  override def setWorkingDirectory(new_dir: Path): Unit = {
    if (isSlowfsPath(new_dir)) {
      wrappedFs.setWorkingDirectory(convertPath(new_dir))
    } else {
      wrappedFs.setWorkingDirectory(new_dir)
    }
  }

  override def getWorkingDirectory: Path = {
    wrappedFs.getWorkingDirectory
  }

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    if (isSlowfsPath(f)) {
      wrappedFs.mkdirs(convertPath(f), permission)
    } else {
      wrappedFs.mkdirs(f, permission)
    }
  }

  override def getFileStatus(f: Path): FileStatus = {
    if (isSlowfsPath(f)) {
      wrappedFs.getFileStatus(convertPath(f))
    } else {
      wrappedFs.getFileStatus(f)
    }
  }

  override def makeQualified(path: Path): Path = {
    // Only process slowfs paths, let others pass through unchanged
    val pathStr = path.toString
    if (pathStr.startsWith("slowfs:/")) {
      path
    } else {
      // For non-slowfs paths, delegate to the underlying filesystem
      wrappedFs.makeQualified(path)
    }
  }

  override def close(): Unit = {
    if (wrappedFs != null) {
      wrappedFs.close()
    }
    super.close()
  }
}

/**
 * Output stream wrapper that adds artificial delay to write operations
 */
class SlowOutputStream(underlying: OutputStream, delayMs: Long) extends OutputStream {

  override def write(b: Int): Unit = {
    addDelay()
    underlying.write(b)
  }

  override def write(b: Array[Byte]): Unit = {
    addDelay()
    underlying.write(b)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    addDelay()
    underlying.write(b, off, len)
  }

  override def flush(): Unit = {
    underlying.flush()
  }

  override def close(): Unit = {
    underlying.close()
  }

  private def addDelay(): Unit = {
    if (delayMs > 0) {
      try {
        Thread.sleep(delayMs)
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
      }
    }
  }
}
