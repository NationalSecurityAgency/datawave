package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.query.jexl.DatawaveJexlContext;

/**
 * This marker Indicates that the current scan session was ended after exceeding the wait window. This empty Metadata is added with the WAIT_WINDOW_OVERRUN key
 * and travels with a Document so that the transformer in the client can ignore this document and iterate to the next result from the tablet server.
 */
public class WaitWindowExceededMetadata extends Attribute<WaitWindowExceededMetadata> implements Serializable {

    @Override
    public WaitWindowExceededMetadata copy() {
        return new WaitWindowExceededMetadata();
    }

    @Override
    public void write(DataOutput output) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(Kryo kryo, Output output) {

    }

    @Override
    public void read(Kryo kryo, Input input) {

    }

    @Override
    public Object getData() {
        return null;
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldnames, DatawaveJexlContext context) {
        return List.of();
    }

    @Override
    public int compareTo(WaitWindowExceededMetadata o) {
        return 0;
    }
}
