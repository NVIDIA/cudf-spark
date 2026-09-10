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

package com.nvidia.spark.rapids.fileio.hadoop;

import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * A {@link SeekableInputStream} implementation that wraps a Hadoop {@link FSDataInputStream}.
 * <br/>
 * This class provides methods to read from the input stream and to seek to a specific position.
 */
public class HadoopInputStream extends SeekableInputStream {
    private final FSDataInputStream in;
    private boolean closed;

    public HadoopInputStream(FSDataInputStream in) {
        requireNonNull(in, "in can't be null");
        this.in = in;
        this.closed = false;
    }

    @Override
    public long getPos() throws IOException {
        return in.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
        in.seek(newPos);
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
          in.close();
          this.closed = true;
        }
    }
}
