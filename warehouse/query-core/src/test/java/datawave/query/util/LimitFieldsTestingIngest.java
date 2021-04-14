package datawave.query.util;

import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.type.DateType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.query.QueryTestTableHelper;
import datawave.util.TableName;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * write data in accumulo for testing of the limit.fields function
 */
public class LimitFieldsTestingIngest {
    
    public enum WhatKindaRange {
        SHARD, DOCUMENT;
    }
    
    private static final Type<?> dateType = new DateType();
    private static final Type<?> lcNoDiacriticsType = new LcNoDiacriticsType();
    
    protected static final String datatype = "test";
    protected static final String date = "20130101";
    protected static final String shard = date + "_0";
    protected static final ColumnVisibility columnVisibility = new ColumnVisibility("ALL");
    protected static final Value emptyValue = new Value(new byte[0]);
    protected static final long timeStamp = 1356998400000l;
    
    /**
     *
     * 
     * @return
     */
    public static void writeItAll(Connector con, WhatKindaRange range) throws Exception {
        
        BatchWriter bw = null;
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        Mutation mutation = null;
        
        String myUID = UID.builder().newId("MyUid".getBytes(), (Date) null).toString();
        
        try {
            // write the shard table :
            bw = con.createBatchWriter(TableName.SHARD, bwConfig);
            mutation = new Mutation(shard);
            
            mutation.put(datatype + "\u0000" + myUID, "FOO_1.FOO.1.0" + "\u0000" + "yawn", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_4.FOO.4.0" + "\u0000" + "purr", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_3.FOO.3.0" + "\u0000" + "abcd", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_3_BAR.FOO.0" + "\u0000" + "abcd<cat>", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_1_BAR.FOO.0" + "\u0000" + "yawn<cat>", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_1_BAR_1.FOO.0" + "\u0000" + "Wed Mar 24 12:00:00 EDT 2021", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + myUID, "FOO_1.FOO.1.1" + "\u0000" + "yawn", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_4.FOO.4.1" + "\u0000" + "purr", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_3.FOO.3.1" + "\u0000" + "bcde", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_3_BAR.FOO.1" + "\u0000" + "bcde<cat>", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_1_BAR.FOO.1" + "\u0000" + "yawn<cat>", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + myUID, "FOO_1.FOO.1.2" + "\u0000" + "yawn", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_4.FOO.4.2" + "\u0000" + "purr", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_3.FOO.3.2" + "\u0000" + "cdef", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_3_BAR.FOO.2" + "\u0000" + "cdef<cat>", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_1_BAR.FOO.2" + "\u0000" + "yawn<cat>", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + myUID, "FOO_1.FOO.1.3" + "\u0000" + "good", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_4.FOO.4.3" + "\u0000" + "yes", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_3.FOO.3.3" + "\u0000" + "defg", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_3_BAR.FOO.3" + "\u0000" + "defg<cat>", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FOO_1_BAR.FOO.3" + "\u0000" + "good<cat>", columnVisibility, timeStamp, emptyValue);
            
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        try {
            // write shard index table:
            bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
            
            mutation = new Mutation(lcNoDiacriticsType.normalize("abcd"));
            mutation.put("FOO_3".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(lcNoDiacriticsType.normalize("bcde"));
            mutation.put("FOO_3".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(lcNoDiacriticsType.normalize("cdef"));
            mutation.put("FOO_3".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(lcNoDiacriticsType.normalize("defg"));
            mutation.put("FOO_3".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(lcNoDiacriticsType.normalize("abcd<cat>"));
            mutation.put("FOO_3_BAR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(lcNoDiacriticsType.normalize("bcde<cat>"));
            mutation.put("FOO_3_BAR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(lcNoDiacriticsType.normalize("cdef<cat>"));
            mutation.put("FOO_3_BAR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(lcNoDiacriticsType.normalize("defg<cat>"));
            mutation.put("FOO_3_BAR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(lcNoDiacriticsType.normalize("yawn<cat>"));
            mutation.put("FOO_1_BAR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(lcNoDiacriticsType.normalize("good<cat>"));
            mutation.put("FOO_1_BAR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        try {
            
            // write the field index table:
            
            bw = con.createBatchWriter(TableName.SHARD, bwConfig);
            
            mutation = new Mutation(shard);
            
            mutation.put("fi\u0000" + "FOO_3", lcNoDiacriticsType.normalize("abcd") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            
            mutation.put("fi\u0000" + "FOO_3", lcNoDiacriticsType.normalize("bcde") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            
            mutation.put("fi\u0000" + "FOO_3", lcNoDiacriticsType.normalize("cdef") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            
            mutation.put("fi\u0000" + "FOO_3", lcNoDiacriticsType.normalize("defg") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            
            mutation.put("fi\u0000" + "FOO_3_BAR", lcNoDiacriticsType.normalize("abcd<cat>") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility,
                            timeStamp, emptyValue);
            
            mutation.put("fi\u0000" + "FOO_3_BAR", lcNoDiacriticsType.normalize("bcde<cat>") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility,
                            timeStamp, emptyValue);
            
            mutation.put("fi\u0000" + "FOO_3_BAR", lcNoDiacriticsType.normalize("cdef<cat>") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility,
                            timeStamp, emptyValue);
            
            mutation.put("fi\u0000" + "FOO_3_BAR", lcNoDiacriticsType.normalize("defg<cat>") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility,
                            timeStamp, emptyValue);
            
            mutation.put("fi\u0000" + "FOO_1_BAR", lcNoDiacriticsType.normalize("yawn<cat>") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility,
                            timeStamp, emptyValue);
            
            mutation.put("fi\u0000" + "FOO_1_BAR", lcNoDiacriticsType.normalize("good<cat>") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility,
                            timeStamp, emptyValue);
            
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        try {
            // write metadata table:
            bw = con.createBatchWriter(QueryTestTableHelper.MODEL_TABLE_NAME, bwConfig);
            
            mutation = new Mutation("FOO_3");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("FOO_4");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            bw.addMutation(mutation);
            
            mutation = new Mutation("FOO_1");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            bw.addMutation(mutation);
            
            mutation = new Mutation("FOO_3_BAR");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("FOO_1_BAR");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("FOO_1_BAR_1");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + dateType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
    }
    
    private static Value getValueForBuilderFor(String... in) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        for (String s : in) {
            builder.addUID(s);
        }
        builder.setCOUNT(in.length);
        builder.setIGNORE(false);
        return new Value(builder.build().toByteArray());
    }
    
    /**
     * forces a shard range
     * 
     * @return
     */
    private static Value getValueForNuthinAndYourHitsForFree() {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setCOUNT(50); // better not be zero!!!!
        builder.setIGNORE(true); // better be true!!!
        return new Value(builder.build().toByteArray());
    }
}
