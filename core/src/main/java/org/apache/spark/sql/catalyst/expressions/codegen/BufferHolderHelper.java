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

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;

/**
 * Helper class to provide cross-classloader access to BufferHolder.
 * Uses MethodHandles with setAccessible to access the package-private BufferHolder class.
 * This helper must be in the same package as BufferHolder and UnsafeWriter.
 */
public class BufferHolderHelper {
    private static final MethodHandle BUFFER_HOLDER_CTOR_1;
    private static final MethodHandle BUFFER_HOLDER_CTOR_2;

    static {
        try {
            Class<?> bufferHolderClass = Class.forName(
                "org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder");
            MethodHandles.Lookup lookup = MethodHandles.lookup();

            // BufferHolder(UnsafeRow) constructor
            Constructor<?> ctor1 = bufferHolderClass.getDeclaredConstructor(UnsafeRow.class);
            ctor1.setAccessible(true);
            BUFFER_HOLDER_CTOR_1 = lookup.unreflectConstructor(ctor1);

            // BufferHolder(UnsafeRow, int) constructor
            Constructor<?> ctor2 = bufferHolderClass.getDeclaredConstructor(UnsafeRow.class, int.class);
            ctor2.setAccessible(true);
            BUFFER_HOLDER_CTOR_2 = lookup.unreflectConstructor(ctor2);
        } catch (Exception e) {
            throw new ExceptionInInitializerError("Failed to initialize BufferHolder access: " + e.getMessage());
        }
    }

    /**
     * Create a BufferHolder instance for the given UnsafeRow.
     * Returns Object to avoid exposing package-private BufferHolder type.
     */
    public static Object createBufferHolder(UnsafeRow row) {
        try {
            return BUFFER_HOLDER_CTOR_1.invoke(row);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to create BufferHolder", e);
        }
    }

    /**
     * Create a BufferHolder instance for the given UnsafeRow with initial buffer size.
     * Returns Object to avoid exposing package-private BufferHolder type.
     */
    public static Object createBufferHolder(UnsafeRow row, int initialBufferSize) {
        try {
            return BUFFER_HOLDER_CTOR_2.invoke(row, initialBufferSize);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to create BufferHolder", e);
        }
    }

    /**
     * Cast Object to BufferHolder type for passing to UnsafeWriter constructor.
     * This method can reference BufferHolder type since it's in the same package.
     */
    @SuppressWarnings({"ClassEscapesDefinedScope"})
    public static BufferHolder castToBufferHolder(Object holder) {
        return (BufferHolder) holder;
    }

    /**
     * Get BufferHolder from UnsafeWriter.
     * Returns Object to avoid exposing package-private BufferHolder type.
     */
    public static Object getBufferHolder(UnsafeWriter writer) {
        return writer.getBufferHolder();
    }
}