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

import com.nvidia.spark.rapids.jni.fileio.RapidsOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

/**
 * A {@link RapidsOutputStream} implementation that wraps a Hadoop {@link FSDataOutputStream}.
 * <br/>
 * This class delegates to the underlying output stream for write and close operations.
 */
public class HadoopOutputStream extends RapidsOutputStream {
    private final FSDataOutputStream out;
    private boolean closed;

    public HadoopOutputStream(FSDataOutputStream out) {
        this.out = requireNonNull(out, "out can't be null");
        this.closed = false;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            out.close();
            closed = true;
        }
    }
}
