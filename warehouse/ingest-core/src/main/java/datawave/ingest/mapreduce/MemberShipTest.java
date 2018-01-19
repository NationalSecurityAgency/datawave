package datawave.ingest.mapreduce;

import com.google.common.hash.BloomFilter;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import org.apache.accumulo.core.data.Value;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class MemberShipTest {
    
    public static <T> BloomFilter<T> create(int expectedInsertions) {
        
        TermFilter<T> funnel = new TermFilter<>();
        return (BloomFilter<T>) BloomFilter.create(funnel, expectedInsertions);
    }
    
    public static <T> BloomFilter<T> update(BloomFilter<T> filter, T term) {
        filter.apply(term);
        
        return filter;
    }
    
    public static byte[] toBytes(BloomFilter<?> filter) {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objOutStream;
        try {
            objOutStream = new ObjectOutputStream(byteStream);
            
            objOutStream.writeObject(filter);
            objOutStream.close();
            byteStream.close();
            return byteStream.toByteArray();
        } catch (IOException e) {
            return new byte[] {};
        }
    }
    
    public static Value toValue(BloomFilter<?> filter) {
        byte[] bloomFilterBytes = toBytes(filter);
        if (bloomFilterBytes.length > 0)
            return new Value(bloomFilterBytes);
        else
            return DataTypeHandler.NULL_VALUE;
    }
}
