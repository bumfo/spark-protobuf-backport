/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fastproto;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;

/**
 * Abstract base class for custom row writers.
 * Extends BaseWriter to provide row writing infrastructure using public UnsafeRowWriter
 * instead of directly accessing package-private BufferHolder.
 */
public abstract class AbstractRowWriter extends BaseWriter {

    public AbstractRowWriter(int numFields) {
        super(new UnsafeRowWriter(numFields), numFields);
    }

    public AbstractRowWriter(int numFields, int initialBufferSize) {
        super(new UnsafeRowWriter(numFields, initialBufferSize), numFields);
    }

    public AbstractRowWriter(UnsafeWriter writer, int numFields) {
        super(null, writer, numFields);
    }

    /**
     * Updates total size of the UnsafeRow using the size collected by BufferHolder, and returns
     * the UnsafeRow created at a constructor
     */
    public UnsafeRow getRow() {
        if (row != null) {
            row.setTotalSize(totalSize());
        }
        return row;
    }

    protected long getFieldOffset(int ordinal) {
        return startingOffset + nullBitsSize + 8L * ordinal;
    }
}