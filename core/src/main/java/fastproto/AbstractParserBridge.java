package fastproto;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.MessageLite;

import java.io.InputStream;
import java.nio.ByteBuffer;

public abstract class AbstractParserBridge implements com.google.protobuf.Parser<MessageLite> {

    protected abstract void parse(CodedInputStream input);

    @Override
    public final MessageLite parseFrom(CodedInputStream input) {
        parse(input);
        return null;
    }

    @Override
    public final MessageLite parsePartialFrom(CodedInputStream input) {
        return parseFrom(input);
    }

    @Override
    public final MessageLite parseFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) {
        return parseFrom(input);
    }

    @Override
    public final MessageLite parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) {
        return parsePartialFrom(input);
    }

    @Override
    public final MessageLite parseFrom(ByteBuffer data) {
        return parseFrom(CodedInputStream.newInstance(data));
    }

    @Override
    public final MessageLite parseFrom(ByteBuffer data, ExtensionRegistryLite extensionRegistry) {
        return parseFrom(CodedInputStream.newInstance(data), extensionRegistry);
    }

    @Override
    public final MessageLite parseFrom(ByteString data) {
        return parseFrom(data.newCodedInput());
    }

    @Override
    public final MessageLite parseFrom(ByteString data, ExtensionRegistryLite extensionRegistry) {
        return parseFrom(data.newCodedInput(), extensionRegistry);
    }

    @Override
    public final MessageLite parsePartialFrom(ByteString data) {
        return parsePartialFrom(data.newCodedInput());
    }

    @Override
    public final MessageLite parsePartialFrom(ByteString data, ExtensionRegistryLite extensionRegistry) {
        return parsePartialFrom(data.newCodedInput(), extensionRegistry);
    }

    @Override
    public final MessageLite parseFrom(byte[] data, int off, int len) {
        return parseFrom(CodedInputStream.newInstance(data, off, len));
    }

    @Override
    public final MessageLite parseFrom(byte[] data, int off, int len, ExtensionRegistryLite extensionRegistry) {
        return parseFrom(CodedInputStream.newInstance(data, off, len), extensionRegistry);
    }

    @Override
    public final MessageLite parseFrom(byte[] data) {
        return parseFrom(CodedInputStream.newInstance(data));
    }

    @Override
    public final MessageLite parseFrom(byte[] data, ExtensionRegistryLite extensionRegistry) {
        return parseFrom(CodedInputStream.newInstance(data), extensionRegistry);
    }

    @Override
    public final MessageLite parsePartialFrom(byte[] data, int off, int len) {
        return parsePartialFrom(CodedInputStream.newInstance(data, off, len));
    }

    @Override
    public final MessageLite parsePartialFrom(byte[] data, int off, int len, ExtensionRegistryLite extensionRegistry) {
        return parsePartialFrom(CodedInputStream.newInstance(data, off, len), extensionRegistry);
    }

    @Override
    public final MessageLite parsePartialFrom(byte[] data) {
        return parsePartialFrom(CodedInputStream.newInstance(data));
    }

    @Override
    public final MessageLite parsePartialFrom(byte[] data, ExtensionRegistryLite extensionRegistry) {
        return parsePartialFrom(CodedInputStream.newInstance(data), extensionRegistry);
    }

    @Override
    public final MessageLite parseFrom(InputStream input) {
        return parseFrom(CodedInputStream.newInstance(input));
    }

    @Override
    public final MessageLite parseFrom(InputStream input, ExtensionRegistryLite extensionRegistry) {
        return parseFrom(CodedInputStream.newInstance(input), extensionRegistry);
    }

    @Override
    public final MessageLite parsePartialFrom(InputStream input) {
        return parsePartialFrom(CodedInputStream.newInstance(input));
    }

    @Override
    public final MessageLite parsePartialFrom(InputStream input, ExtensionRegistryLite extensionRegistry) {
        return parsePartialFrom(CodedInputStream.newInstance(input), extensionRegistry);
    }

    @Override
    public final MessageLite parseDelimitedFrom(InputStream input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final MessageLite parseDelimitedFrom(InputStream input, ExtensionRegistryLite extensionRegistry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final MessageLite parsePartialDelimitedFrom(InputStream input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final MessageLite parsePartialDelimitedFrom(InputStream input, ExtensionRegistryLite extensionRegistry) {
        throw new UnsupportedOperationException();
    }
}
