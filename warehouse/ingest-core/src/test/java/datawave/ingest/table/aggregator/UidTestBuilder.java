package datawave.ingest.table.aggregator;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.ingest.protobuf.Uid;
import org.apache.accumulo.core.data.Value;

import static java.util.Arrays.asList;

/**
 * Common utilities for testing Uid.List functionality.
 */
public final class UidTestBuilder {
    private Uid.List.Builder b;
    
    private UidTestBuilder() {
        b = Uid.List.newBuilder();
    }
    
    public static UidTestBuilder newBuilder() {
        return new UidTestBuilder();
    }
    
    public UidTestBuilder withCountOnly(int count) {
        if (b.getUIDList().size() > 0) {
            throw new IllegalStateException("Setting count-only for named uid list prohibited.");
        }
        b.setIGNORE(true);
        b.setCOUNT(count);
        return this;
    }
    
    // Only used to represent deprecated data that will no longer be created like this
    @Deprecated
    public UidTestBuilder withCountOverride(int count) {
        b.setCOUNT(count);
        return this;
    }
    
    public UidTestBuilder withUids(String... uids) {
        if (b.getIGNORE()) {
            throw new IllegalStateException("Adding uids to count-only is prohibited.");
        }
        b.setIGNORE(false);
        b.addAllUID(asList(uids));
        b.setCOUNT(uids.length);
        return this;
    }
    
    public UidTestBuilder withRemovals(String... uids) {
        b.addAllREMOVEDUID(asList(uids));
        return this;
    }
    
    public static Value uidList(String... uids) {
        return UidTestBuilder.newBuilder().withUids(uids).build();
    }
    
    public static Uid.List valueToUidList(Value value) {
        try {
            return Uid.List.parseFrom(value.get());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse protobuf", e);
        }
    }
    
    public Value build() {
        return new Value(b.build().toByteArray());
    }
}
