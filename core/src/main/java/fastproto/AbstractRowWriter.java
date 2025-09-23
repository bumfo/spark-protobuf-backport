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
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolderHelper;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;

/**
 * Abstract base class for custom row writers.
 * This class provides the basic infrastructure for row writing using BufferHolderHelper
 * to access package-private BufferHolder across class loaders.
 */
public abstract class AbstractRowWriter extends UnsafeWriter {

    protected final UnsafeRow row;
    protected final int nullBitsSize;
    protected final int fixedSize;

    public AbstractRowWriter(int numFields) {
        this(new UnsafeRow(numFields));
    }

    public AbstractRowWriter(int numFields, int initialBufferSize) {
        this(new UnsafeRow(numFields), initialBufferSize);
    }

    public AbstractRowWriter(UnsafeWriter writer, int numFields) {
        this(null, BufferHolderHelper.getBufferHolder(writer), numFields);
    }

    private AbstractRowWriter(UnsafeRow row) {
        this(row, BufferHolderHelper.createBufferHolder(row), row.numFields());
    }

    private AbstractRowWriter(UnsafeRow row, int initialBufferSize) {
        this(row, BufferHolderHelper.createBufferHolder(row, initialBufferSize), row.numFields());
    }

    private AbstractRowWriter(UnsafeRow row, Object holder, int numFields) {
        super(BufferHolderHelper.castToBufferHolder(holder));
        this.row = row;
        this.nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields);
        this.fixedSize = nullBitsSize + 8 * numFields;
        this.startingOffset = cursor();
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