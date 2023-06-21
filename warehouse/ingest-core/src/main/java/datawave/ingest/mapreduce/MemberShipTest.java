package datawave.ingest.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import datawave.ingest.mapreduce.handler.DataTypeHandler;

import org.apache.accumulo.core.data.Value;

import com.google.common.hash.BloomFilter;

public class MemberShipTest {

    public static <T> BloomFilter<T> create(int expectedInsertions) {

        TermFilter<T> funnel = new TermFilter<>();
        return (BloomFilter<T>) BloomFilter.create(funnel, expectedInsertions);
    }

    public static <T> BloomFilter<T> update(BloomFilter<T> filter, T term) {
        filter.apply(term);

        return filter;
    }

    public static Value toValue(BloomFilter<?> filter) {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objOutStream;
        try {
            objOutStream = new ObjectOutputStream(byteStream);

            objOutStream.writeObject(filter);
            objOutStream.close();
            byteStream.close();
            return new Value(byteStream.toByteArray());
        } catch (IOException e) {
            return DataTypeHandler.NULL_VALUE;
        }

    }
}
