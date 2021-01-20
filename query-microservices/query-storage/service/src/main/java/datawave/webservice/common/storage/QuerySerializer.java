package datawave.webservice.common.storage;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import io.protostuff.LinkedBuffer;
import io.protostuff.Message;
import io.protostuff.ProtostuffOutput;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This is a query serializer to facilitate using Hazelcast
 */
public class QuerySerializer implements StreamSerializer<Query> {

    @Override
    public void write(ObjectDataOutput objectDataOutput, Query query) throws IOException {
        if (query instanceof Message) {
            ProtostuffOutput output = new ProtostuffOutput(LinkedBuffer.allocate(), (OutputStream)objectDataOutput);
            ((Message<Query>)query).cachedSchema().writeTo(output, query);
        } else {
            throw new IOException("Cannot serialize query class " + query.getClass());
        }
    }

    @Override
    public Query read(ObjectDataInput objectDataInput) throws IOException {
        // TODO
        return null;
    }

    @Override
    public int getTypeId() {
        return 0;
    }

    @Override
    public void destroy() {

    }
}
