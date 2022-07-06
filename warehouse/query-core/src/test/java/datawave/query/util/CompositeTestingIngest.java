package datawave.query.util;

import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.normalizer.AbstractNormalizer;
import datawave.data.type.BaseType;
import datawave.data.type.DateType;
import datawave.data.type.IpAddressType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.ingest.protobuf.Uid;
import datawave.query.QueryTestTableHelper;
import datawave.query.parser.JavaRegexAnalyzer;
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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CompositeTestingIngest {
    
    public enum WhatKindaRange {
        SHARD, DOCUMENT;
    }
    
    private static final Type<?> lcNoDiacriticsType = new LcNoDiacriticsType();
    private static final Type<?> ipAddressType = new IpAddressType();
    private static final Type<?> numberType = new NumberType();
    private static final Type<?> dateType = new DateType();
    private static final Type<?> ucType = new UcType();
    
    protected static final String datatype = "test";
    protected static final String date = "20130101";
    protected static final String shard = date + "_0";
    protected static final ColumnVisibility columnVisibility = new ColumnVisibility("ALL");
    protected static final Value emptyValue = new Value(new byte[0]);
    protected static final long timeStamp = 1356998400000l;
    
    protected static String normalizeColVal(Map.Entry<String,String> colVal) throws Exception {
        if ("FROM_ADDRESS".equals(colVal.getKey()) || "TO_ADDRESS".equals(colVal.getKey())) {
            return ipAddressType.normalize(colVal.getValue());
        } else {
            return lcNoDiacriticsType.normalize(colVal.getValue());
        }
    }
    
    protected static String normalizerForColumn(String column) {
        if ("AGE".equals(column) || "MAGIC".equals(column) || "ETA".equals(column)) {
            return numberType.getClass().getName();
        } else if ("FROM_ADDRESS".equals(column) || "TO_ADDRESS".equals(column)) {
            return ipAddressType.getClass().getName();
        } else {
            return lcNoDiacriticsType.getClass().getName();
        }
    }
    
    /**
     * gparent - parent - child -
     * 
     * @return
     */
    public static void writeItAll(Connector con, WhatKindaRange range) throws Exception {
        
        BatchWriter bw = null;
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        Mutation mutation = null;
        
        String oneUUID = UID.builder().newId("One".getBytes(), (Date) null).toString();
        String twoUUID = UID.builder().newId("Two".toString().getBytes(), (Date) null).toString();
        String threeUUID = UID.builder().newId("Three".toString().getBytes(), (Date) null).toString();
        
        try {
            // write the shard table :
            bw = con.createBatchWriter(TableName.SHARD, bwConfig);
            mutation = new Mutation(shard);
            
            mutation.put(datatype + "\u0000" + oneUUID, "COLOR.0" + "\u0000" + "red", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + oneUUID, "COLOR.1" + "\u0000" + "blue", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + oneUUID, "MAKE.0" + "\u0000" + "Ford", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + oneUUID, "MAKE.1" + "\u0000" + "Chevy", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + oneUUID, "UUID.0" + "\u0000" + "One", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + twoUUID, "COLOR.0" + "\u0000" + "pink", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + twoUUID, "COLOR.1" + "\u0000" + "green", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + twoUUID, "MAKE.0" + "\u0000" + "Toyota", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + twoUUID, "MAKE.1" + "\u0000" + "VW", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + twoUUID, "UUID.0" + "\u0000" + "Two", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + threeUUID, "COLOR.0" + "\u0000" + "cyan", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + threeUUID, "COLOR.1" + "\u0000" + "purple", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + threeUUID, "MAKE.0" + "\u0000" + "Subaru", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + threeUUID, "MAKE.1" + "\u0000" + "Nissan", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + threeUUID, "UUID.0" + "\u0000" + "CAPONE", columnVisibility, timeStamp, emptyValue);
            
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        try {
            // write shard index table:
            bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
            // Ones
            // uuid
            mutation = new Mutation(lcNoDiacriticsType.normalize("One"));
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(oneUUID));
            bw.addMutation(mutation);
            // colors
            mutation = new Mutation(ucType.normalize("red"));
            mutation.put("COLOR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(oneUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("blue"));
            mutation.put("COLOR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(oneUUID));
            bw.addMutation(mutation);
            // makes
            mutation = new Mutation(ucType.normalize("Ford"));
            mutation.put("MAKE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(oneUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("Chevy"));
            mutation.put("MAKE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(oneUUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(ucType.normalize("Ford" + CompositeIngest.DEFAULT_SEPARATOR + "red"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(oneUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("Ford" + CompositeIngest.DEFAULT_SEPARATOR + "blue"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(oneUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("Chevy" + CompositeIngest.DEFAULT_SEPARATOR + "red"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(oneUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("Chevy" + CompositeIngest.DEFAULT_SEPARATOR + "blue"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(oneUUID));
            bw.addMutation(mutation);
            
            // Two
            // uuid
            mutation = new Mutation(lcNoDiacriticsType.normalize("Two"));
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(twoUUID));
            bw.addMutation(mutation);
            // colors
            mutation = new Mutation(ucType.normalize("pink"));
            mutation.put("COLOR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(twoUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("green"));
            mutation.put("COLOR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(twoUUID));
            bw.addMutation(mutation);
            // makes
            mutation = new Mutation(ucType.normalize("Toyota"));
            mutation.put("MAKE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(twoUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("VW"));
            mutation.put("MAKE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(twoUUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(ucType.normalize("Toyota" + CompositeIngest.DEFAULT_SEPARATOR + "pink"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(twoUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("Toyota" + CompositeIngest.DEFAULT_SEPARATOR + "green"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(twoUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("VW" + CompositeIngest.DEFAULT_SEPARATOR + "pink"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(twoUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("VW" + CompositeIngest.DEFAULT_SEPARATOR + "green"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(twoUUID));
            bw.addMutation(mutation);
            
            // Three
            // uuid
            mutation = new Mutation(lcNoDiacriticsType.normalize("Three"));
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(threeUUID));
            bw.addMutation(mutation);
            // colors
            mutation = new Mutation(ucType.normalize("cyan"));
            mutation.put("COLOR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(threeUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("purple"));
            mutation.put("COLOR".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(threeUUID));
            bw.addMutation(mutation);
            // makes
            mutation = new Mutation(ucType.normalize("Subaru"));
            mutation.put("MAKE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(threeUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("Nissan"));
            mutation.put("MAKE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(threeUUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(ucType.normalize("Subaru" + CompositeIngest.DEFAULT_SEPARATOR + "cyan"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(threeUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("Subaru" + CompositeIngest.DEFAULT_SEPARATOR + "purple"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(threeUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("Nissan" + CompositeIngest.DEFAULT_SEPARATOR + "cyan"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(threeUUID));
            bw.addMutation(mutation);
            mutation = new Mutation(ucType.normalize("Nissan" + CompositeIngest.DEFAULT_SEPARATOR + "purple"));
            mutation.put("MAKE_COLOR", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(threeUUID));
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
            // corleones
            // uuid
            mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("One") + "\u0000" + datatype + "\u0000" + oneUUID, columnVisibility, timeStamp,
                            emptyValue);
            // names
            mutation.put("fi\u0000" + "COLOR", ucType.normalize("Red") + "\u0000" + datatype + "\u0000" + oneUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "COLOR", ucType.normalize("Blue") + "\u0000" + datatype + "\u0000" + oneUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE", ucType.normalize("Ford") + "\u0000" + datatype + "\u0000" + oneUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE", ucType.normalize("Chevy") + "\u0000" + datatype + "\u0000" + oneUUID, columnVisibility, timeStamp, emptyValue);
            
            // sopranos
            // uuid
            mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("Two") + "\u0000" + datatype + "\u0000" + twoUUID, columnVisibility, timeStamp,
                            emptyValue);
            // names
            mutation.put("fi\u0000" + "COLOR", ucType.normalize("Pink") + "\u0000" + datatype + "\u0000" + twoUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "COLOR", ucType.normalize("Green") + "\u0000" + datatype + "\u0000" + twoUUID, columnVisibility, timeStamp, emptyValue);
            // genders
            mutation.put("fi\u0000" + "MAKE", ucType.normalize("Toyota") + "\u0000" + datatype + "\u0000" + twoUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE", ucType.normalize("Chevy") + "\u0000" + datatype + "\u0000" + twoUUID, columnVisibility, timeStamp, emptyValue);
            // capones
            // uuid
            mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("Three") + "\u0000" + datatype + "\u0000" + threeUUID, columnVisibility, timeStamp,
                            emptyValue);
            // names
            mutation.put("fi\u0000" + "COLOR", ucType.normalize("Cyan") + "\u0000" + datatype + "\u0000" + threeUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "COLOR", ucType.normalize("Purple") + "\u0000" + datatype + "\u0000" + threeUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE", ucType.normalize("Subaru") + "\u0000" + datatype + "\u0000" + threeUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE", ucType.normalize("Nissan") + "\u0000" + datatype + "\u0000" + threeUUID, columnVisibility, timeStamp, emptyValue);
            
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("Ford" + CompositeIngest.DEFAULT_SEPARATOR + "red") + "\u0000" + datatype + "\u0000"
                            + oneUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("Ford" + CompositeIngest.DEFAULT_SEPARATOR + "blue") + "\u0000" + datatype + "\u0000"
                            + oneUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("Chevy" + CompositeIngest.DEFAULT_SEPARATOR + "red") + "\u0000" + datatype + "\u0000"
                            + oneUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("Chevy" + CompositeIngest.DEFAULT_SEPARATOR + "blue") + "\u0000" + datatype + "\u0000"
                            + oneUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("Toyota" + CompositeIngest.DEFAULT_SEPARATOR + "pink") + "\u0000" + datatype + "\u0000"
                            + twoUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("Toyota" + CompositeIngest.DEFAULT_SEPARATOR + "green") + "\u0000" + datatype + "\u0000"
                            + twoUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("VW" + CompositeIngest.DEFAULT_SEPARATOR + "pink") + "\u0000" + datatype + "\u0000"
                            + twoUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("VW" + CompositeIngest.DEFAULT_SEPARATOR + "green") + "\u0000" + datatype + "\u0000"
                            + twoUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("Subaru" + CompositeIngest.DEFAULT_SEPARATOR + "cyan") + "\u0000" + datatype + "\u0000"
                            + threeUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("Subaru" + CompositeIngest.DEFAULT_SEPARATOR + "purple") + "\u0000" + datatype + "\u0000"
                            + threeUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("Nissan" + CompositeIngest.DEFAULT_SEPARATOR + "cyan") + "\u0000" + datatype + "\u0000"
                            + threeUUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "MAKE_COLOR", ucType.normalize("Nissan" + CompositeIngest.DEFAULT_SEPARATOR + "purple") + "\u0000" + datatype + "\u0000"
                            + threeUUID, columnVisibility, timeStamp, emptyValue);
            
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        try {
            // write metadata table:
            bw = con.createBatchWriter(QueryTestTableHelper.MODEL_TABLE_NAME, bwConfig);
            
            mutation = new Mutation("UUID");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + normalizerForColumn("UUID")), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("COLOR");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(10L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + UcType.class.getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("MAKE");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(19L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + UcType.class.getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("MAKE_COLOR");
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_CI, new Text(datatype + "\u0000" + "MAKE,COLOR"), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_CITD, new Text(datatype + "\u0000" + "20010101 000000.000"), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_CISEP, new Text(datatype + "\u0000" + CompositeIngest.DEFAULT_SEPARATOR), emptyValue);
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
    
    public static class PigLatinNormalizer extends AbstractNormalizer<String> {
        
        final String vowels = "aeiou";
        
        public String normalize(String fieldValue) {
            fieldValue = fieldValue.toLowerCase();
            if (!fieldValue.isEmpty()) {
                char first = fieldValue.charAt(0);
                if (vowels.indexOf(first) == -1) {
                    return fieldValue.substring(1) + first + "ay";
                } else {
                    return fieldValue + "yay";
                }
            }
            return fieldValue;
        }
        
        public String normalizeRegex(String fieldRegex) {
            if (null == fieldRegex) {
                return null;
            }
            try {
                JavaRegexAnalyzer regex = new JavaRegexAnalyzer(fieldRegex);
                regex.applyRegexCaseSensitivity(false);
                return regex.getRegex();
            } catch (JavaRegexAnalyzer.JavaRegexParseException e) {
                throw new IllegalArgumentException("Unable to parse regex " + fieldRegex, e);
            }
        }
        
        @Override
        public String normalizeDelegateType(String delegateIn) {
            return normalize(delegateIn);
        }
        
        @Override
        public String denormalize(String in) {
            return in;
        }
    }
    
    public static class PigLatinType extends BaseType<String> {
        
        private static final long serialVersionUID = -5102714749195917406L;
        
        public PigLatinType() {
            super(new PigLatinNormalizer());
        }
        
        public PigLatinType(String delegateString) {
            super(delegateString, new PigLatinNormalizer());
        }
        
    }
    
    public static class UcNormalizer extends AbstractNormalizer<String> {
        
        public String normalize(String fieldValue) {
            return fieldValue.toUpperCase(Locale.ENGLISH);
        }
        
        public String normalizeRegex(String fieldRegex) {
            if (null == fieldRegex) {
                return null;
            }
            try {
                JavaRegexAnalyzer regex = new JavaRegexAnalyzer(fieldRegex);
                regex.applyRegexCaseSensitivity(false);
                return regex.getRegex();
            } catch (JavaRegexAnalyzer.JavaRegexParseException e) {
                throw new IllegalArgumentException("Unable to parse regex " + fieldRegex, e);
            }
        }
        
        @Override
        public String normalizeDelegateType(String delegateIn) {
            return normalize(delegateIn);
        }
        
        @Override
        public String denormalize(String in) {
            return in;
        }
    }
    
    public static class UcType extends BaseType<String> {
        
        private static final long serialVersionUID = -5102714749195917406L;
        
        public UcType() {
            super(new UcNormalizer());
        }
        
        public UcType(String delegateString) {
            super(delegateString, new UcNormalizer());
        }
        
    }
    
}
