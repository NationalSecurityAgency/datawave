package datawave.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;

public class ZeroUIDCountMappingIterator extends WrappingIterator implements SortedKeyValueIterator<Key,Value> {
    @Override
    public Value getTopValue() {
        Value value = super.getTopValue();
        try {
            Uid.List v = Uid.List.parseFrom(value.get());
            if (v.getCOUNT() == 0) {
                Uid.List.Builder builder = Uid.List.newBuilder();
                builder.setIGNORE(v.getIGNORE());
                builder.setCOUNT(1);
                builder.addAllUID(v.getUIDList());
                builder.addAllREMOVEDUID(v.getREMOVEDUIDList());
                value = new Value(builder.build().toByteArray());

            }
        } catch (InvalidProtocolBufferException e) {
            // return the value as is
        }
        return value;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        ZeroUIDCountMappingIterator iterator = new ZeroUIDCountMappingIterator();
        iterator.setSource(getSource().deepCopy(env));
        return iterator;
    }
}
