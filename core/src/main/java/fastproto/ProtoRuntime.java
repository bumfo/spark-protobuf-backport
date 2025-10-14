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

import com.google.protobuf.CodedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Runtime support for inline protobuf parsers generated for Spark columnar execution.
 *
 * This class provides minimal static methods designed to be called from generated
 * switch-based parsers. Methods are kept small for JIT inlining and reuse existing
 * optimized writers (NullDefaultRowWriter, PrimitiveArrayWriter).
 *
 * Key design principles:
 * - Single PrimitiveArrayWriter instance to avoid variable-length data interleaving
 * - Eager array completion before any variable-length writes
 * - BufferList array for multiple repeated message fields
 * - All methods are candidates for JIT inlining (@ForceInline would require JDK 9+)
 */
public final class ProtoRuntime {

  /**
   * Context for managing a single active PrimitiveArrayWriter.
   * Only one primitive array can be accumulated at a time to prevent
   * interleaving in the variable-length buffer region.
   */
  public static final class ArrayContext {
    private PrimitiveArrayWriter writer;
    private int fieldOrdinal = -1;

    /**
     * Complete the current array if one is active.
     * Must be called before any variable-length write operation.
     */
    public void completeIfActive(NullDefaultRowWriter parent) {
      if (writer != null && writer.size() > 0) {
        int prevCursor = parent.cursor();
        writer.complete();
        parent.writeVariableField(fieldOrdinal, prevCursor);
      }
      writer = null;
      fieldOrdinal = -1;
    }

    /**
     * Get or create a PrimitiveArrayWriter for the specified field.
     * If switching to a different field, completes the previous array first.
     */
    public PrimitiveArrayWriter getOrCreate(NullDefaultRowWriter parent, int ordinal, int elementSize) {
      if (fieldOrdinal != ordinal) {
        completeIfActive(parent);
        writer = new PrimitiveArrayWriter(parent, elementSize, 32);
        fieldOrdinal = ordinal;
      }
      return writer;
    }

    /**
     * Reset the context, completing any active array.
     */
    public void reset(NullDefaultRowWriter parent) {
      completeIfActive(parent);
    }
  }

  // ===== Single Field Writers (Primitives - no array completion needed) =====

  public static void writeInt32(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readInt32());
  }

  public static void writeInt64(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readInt64());
  }

  public static void writeUInt32(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readUInt32());
  }

  public static void writeUInt64(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readUInt64());
  }

  public static void writeSInt32(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readSInt32());
  }

  public static void writeSInt64(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readSInt64());
  }

  public static void writeFixed32(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readFixed32());
  }

  public static void writeFixed64(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readFixed64());
  }

  public static void writeSFixed32(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readSFixed32());
  }

  public static void writeSFixed64(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readSFixed64());
  }

  public static void writeFloat(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readFloat());
  }

  public static void writeDouble(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readDouble());
  }

  public static void writeBool(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readBool());
  }

  public static void writeEnum(CodedInputStream in, NullDefaultRowWriter w, int ord)
      throws IOException {
    w.write(ord, in.readEnum());
  }

  // ===== Single Field Writers (Variable-length - must complete array first) =====

  public static void writeString(CodedInputStream in, NullDefaultRowWriter w,
                                  int ord, ArrayContext arrayCtx) throws IOException {
    arrayCtx.completeIfActive(w);
    w.writeBytes(ord, in.readByteBuffer());
  }

  public static void writeBytes(CodedInputStream in, NullDefaultRowWriter w,
                                 int ord, ArrayContext arrayCtx) throws IOException {
    arrayCtx.completeIfActive(w);
    w.writeBytes(ord, in.readByteBuffer());
  }

  /**
   * Parse and write a nested message field.
   */
  public static void writeMessage(CodedInputStream in, NullDefaultRowWriter w,
                                   int ord, ArrayContext arrayCtx,
                                   StreamWireParser parser) throws IOException {
    arrayCtx.completeIfActive(w);
    ByteBuffer bb = in.readByteBuffer();
    if (parser != null) {
      int offset = w.cursor();
      RowWriter nestedWriter = parser.acquireNestedWriter(w);
      nestedWriter.resetRowWriter();
      parser.parseInto(bb, nestedWriter);
      w.writeVariableField(ord, offset);
    }
  }

  // ===== Repeated Primitive Collectors =====

  public static void collectInt32(CodedInputStream in, ArrayContext ctx,
                                   NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    w.writeInt(in.readInt32());
  }

  public static void collectPackedInt32(CodedInputStream in, ArrayContext ctx,
                                         NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeInt(in.readInt32());
    }
    in.popLimit(limit);
  }

  public static void collectInt64(CodedInputStream in, ArrayContext ctx,
                                   NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    w.writeLong(in.readInt64());
  }

  public static void collectPackedInt64(CodedInputStream in, ArrayContext ctx,
                                         NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeLong(in.readInt64());
    }
    in.popLimit(limit);
  }

  public static void collectUInt32(CodedInputStream in, ArrayContext ctx,
                                    NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    w.writeInt(in.readUInt32());
  }

  public static void collectPackedUInt32(CodedInputStream in, ArrayContext ctx,
                                          NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeInt(in.readUInt32());
    }
    in.popLimit(limit);
  }

  public static void collectUInt64(CodedInputStream in, ArrayContext ctx,
                                    NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    w.writeLong(in.readUInt64());
  }

  public static void collectPackedUInt64(CodedInputStream in, ArrayContext ctx,
                                          NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeLong(in.readUInt64());
    }
    in.popLimit(limit);
  }

  public static void collectSInt32(CodedInputStream in, ArrayContext ctx,
                                    NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    w.writeInt(in.readSInt32());
  }

  public static void collectPackedSInt32(CodedInputStream in, ArrayContext ctx,
                                          NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeInt(in.readSInt32());
    }
    in.popLimit(limit);
  }

  public static void collectSInt64(CodedInputStream in, ArrayContext ctx,
                                    NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    w.writeLong(in.readSInt64());
  }

  public static void collectPackedSInt64(CodedInputStream in, ArrayContext ctx,
                                          NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeLong(in.readSInt64());
    }
    in.popLimit(limit);
  }

  public static void collectFixed32(CodedInputStream in, ArrayContext ctx,
                                     NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    w.writeInt(in.readFixed32());
  }

  public static void collectPackedFixed32(CodedInputStream in, ArrayContext ctx,
                                           NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeInt(in.readFixed32());
    }
    in.popLimit(limit);
  }

  public static void collectFixed64(CodedInputStream in, ArrayContext ctx,
                                     NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    w.writeLong(in.readFixed64());
  }

  public static void collectPackedFixed64(CodedInputStream in, ArrayContext ctx,
                                           NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeLong(in.readFixed64());
    }
    in.popLimit(limit);
  }

  public static void collectSFixed32(CodedInputStream in, ArrayContext ctx,
                                      NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    w.writeInt(in.readSFixed32());
  }

  public static void collectPackedSFixed32(CodedInputStream in, ArrayContext ctx,
                                            NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeInt(in.readSFixed32());
    }
    in.popLimit(limit);
  }

  public static void collectSFixed64(CodedInputStream in, ArrayContext ctx,
                                      NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    w.writeLong(in.readSFixed64());
  }

  public static void collectPackedSFixed64(CodedInputStream in, ArrayContext ctx,
                                            NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeLong(in.readSFixed64());
    }
    in.popLimit(limit);
  }

  public static void collectFloat(CodedInputStream in, ArrayContext ctx,
                                   NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    w.writeFloat(in.readFloat());
  }

  public static void collectPackedFloat(CodedInputStream in, ArrayContext ctx,
                                         NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 4);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeFloat(in.readFloat());
    }
    in.popLimit(limit);
  }

  public static void collectDouble(CodedInputStream in, ArrayContext ctx,
                                    NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    w.writeDouble(in.readDouble());
  }

  public static void collectPackedDouble(CodedInputStream in, ArrayContext ctx,
                                          NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 8);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeDouble(in.readDouble());
    }
    in.popLimit(limit);
  }

  public static void collectBool(CodedInputStream in, ArrayContext ctx,
                                  NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 1);
    w.writeBoolean(in.readBool());
  }

  public static void collectPackedBool(CodedInputStream in, ArrayContext ctx,
                                        NullDefaultRowWriter parent, int ordinal) throws IOException {
    PrimitiveArrayWriter w = ctx.getOrCreate(parent, ordinal, 1);
    int length = in.readRawVarint32();
    int limit = in.pushLimit(length);
    while (!in.isAtEnd()) {
      w.writeBoolean(in.readBool());
    }
    in.popLimit(limit);
  }

  // Repeated string/bytes - must complete any active array first
  public static void collectString(CodedInputStream in, BufferList[] lists,
                                    int listIndex, ArrayContext arrayCtx,
                                    NullDefaultRowWriter parent) throws IOException {
    arrayCtx.completeIfActive(parent);
    if (lists[listIndex] == null) {
      lists[listIndex] = new BufferList();
    }
    lists[listIndex].add(in.readByteBuffer());
  }

  public static void collectBytes(CodedInputStream in, BufferList[] lists,
                                   int listIndex, ArrayContext arrayCtx,
                                   NullDefaultRowWriter parent) throws IOException {
    arrayCtx.completeIfActive(parent);
    if (lists[listIndex] == null) {
      lists[listIndex] = new BufferList();
    }
    lists[listIndex].add(in.readByteBuffer());
  }

  // ===== Repeated Message Collectors =====

  public static void collectMessage(CodedInputStream in, BufferList[] lists,
                                     int listIndex, ArrayContext arrayCtx,
                                     NullDefaultRowWriter parent) throws IOException {
    arrayCtx.completeIfActive(parent);
    if (lists[listIndex] == null) {
      lists[listIndex] = new BufferList();
    }
    lists[listIndex].add(in.readByteBuffer());
  }

  // ===== Message Array Writers =====

  /**
   * Write collected message buffers as an array.
   * Uses existing StreamWireParser batch method.
   */
  public static void flushMessageArray(BufferList list, int ordinal,
                                        StreamWireParser parser,
                                        NullDefaultRowWriter writer) {
    if (list != null && list.count > 0) {
      StreamWireParser.writeMessageArrayFromBuffers(
        list.array, list.count, ordinal, parser, writer);
    }
  }

  /**
   * Write collected string buffers as an array.
   */
  public static void flushStringArray(BufferList list, int ordinal,
                                       NullDefaultRowWriter writer) {
    if (list != null && list.count > 0) {
      StreamWireParser.writeStringArrayFromBuffers(
        list.array, list.count, ordinal, writer);
    }
  }

  /**
   * Write collected bytes buffers as an array.
   */
  public static void flushBytesArray(BufferList list, int ordinal,
                                      NullDefaultRowWriter writer) {
    if (list != null && list.count > 0) {
      StreamWireParser.writeBytesArrayFromBuffers(
        list.array, list.count, ordinal, writer);
    }
  }
}