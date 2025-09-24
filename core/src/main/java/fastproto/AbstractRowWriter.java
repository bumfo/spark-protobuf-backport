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
 * Abstract base class for custom UnsafeRow writers.
 * <p>
 * This class provides the fundamental infrastructure for writing data to UnsafeRow format,
 * including buffer management, field offset calculation, and row size tracking.
 * <p>
 * <b>Package Placement:</b> This class must be in the Spark codegen package to access
 * the package-private BufferHolder class, which is essential for memory management.
 * <p>
 * Subclasses should implement specific null bit management strategies and field writing logic.
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

    public boolean hasRow() {
        return row != null;
    }

    /**
     * Finalizes the UnsafeRow by updating its total size and returns the completed row.
     * <p>
     * This method uses the size information collected by the BufferHolder during writing
     * to set the correct total size in the UnsafeRow header. This is essential for
     * proper serialization and memory management.
     *
     * @return the completed UnsafeRow with correct size metadata, or null for nested writers
     */
    public UnsafeRow getRow() {
        if (row != null) {
            row.setTotalSize(totalSize());
        }
        return row;
    }

    /**
     * Calculates the memory offset for a specific field's data.
     * <p>
     * UnsafeRow layout: [null bits][field0][field1]...[fieldN][variable data]
     * Each field occupies 8 bytes for either the value (primitives) or offset+size (variable-length).
     *
     * @param ordinal the field ordinal (0-based)
     * @return the absolute memory offset where this field's data should be written
     */
    protected long getFieldOffset(int ordinal) {
        return startingOffset + nullBitsSize + 8L * ordinal;
    }
}