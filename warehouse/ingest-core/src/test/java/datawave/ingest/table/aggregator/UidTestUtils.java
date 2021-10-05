package datawave.ingest.table.aggregator;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.ingest.protobuf.Uid;
import org.apache.accumulo.core.data.Value;

import static java.util.Arrays.asList;

/**
 * Common utilities for testing Uid.List functionality.
 */
public final class UidTestUtils {
    private UidTestUtils() {
        // util class, do not instantiate
    }
    
    public static Uid.List valueToUidList(Value value) {
        try {
            return Uid.List.parseFrom(value.get());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse protobuf", e);
        }
    }
    
    public static Value countOnlyList(int count) {
        Uid.List.Builder b = Uid.List.newBuilder();
        b.setIGNORE(true);
        b.setCOUNT(count);
        
        return builderToValue(b);
    }
    
    public static Value uidList(String... uids) {
        Uid.List.Builder b = Uid.List.newBuilder();
        b.setIGNORE(false);
        b.addAllUID(asList(uids));
        b.setCOUNT(uids.length);
        
        return builderToValue(b);
    }
    
    public static Value removeUidList(String... uids) {
        Uid.List.Builder b = Uid.List.newBuilder();
        b.setIGNORE(false);
        b.addAllREMOVEDUID(asList(uids));
        b.setCOUNT(-1 * uids.length);
        
        return builderToValue(b);
    }
    
    public static Value legacyRemoveUidList(String... uids) {
        Uid.List.Builder b = Uid.List.newBuilder();
        b.setIGNORE(false);
        b.addAllUID(asList(uids));
        b.setCOUNT(-1 * uids.length);
        
        return builderToValue(b);
    }
    
    private static Value builderToValue(Uid.List.Builder b) {
        return new Value(b.build().toByteArray());
    }
}
