package datawave.query.util;

import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
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

public class VisibilityWiseGuysIngest {
    
    public enum WhatKindaRange {
        SHARD, DOCUMENT;
    }
    
    private static final Type<?> lcNoDiacriticsType = new LcNoDiacriticsType();
    private static final Type<?> numberType = new NumberType();
    
    protected static final String datatype = "test";
    protected static final String date = "20130101";
    protected static final String shard = date + "_0";
    protected static final ColumnVisibility columnVisibility = new ColumnVisibility("ALL");
    protected static final ColumnVisibility columnVisibilityEnglish = new ColumnVisibility("ALL&E");
    protected static final ColumnVisibility columnVisibilityItalian = new ColumnVisibility("ALL&I");
    protected static final Value emptyValue = new Value(new byte[0]);
    protected static final long timeStamp = 1356998400000l;
    
    public static final String corleoneUID = UID.builder().newId("Corleone".getBytes(), (Date) null).toString();
    public static final String sopranoUID = UID.builder().newId("Soprano".getBytes(), (Date) null).toString();
    public static final String caponeUID = UID.builder().newId("Capone".getBytes(), (Date) null).toString();
    
    /**
     * 
     *
     * @return
     */
    public static void writeItAll(Connector con, WhatKindaRange range) throws Exception {
        
        BatchWriter bw = null;
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        Mutation mutation = null;
        
        try {
            // write the shard table :
            bw = con.createBatchWriter(TableName.SHARD, bwConfig);
            mutation = new Mutation(shard);
            
            mutation.put(datatype + "\u0000" + corleoneUID, "NAME.0" + "\u0000" + "SANTINO", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "NAME.1" + "\u0000" + "FREDO", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "NAME.2" + "\u0000" + "MICHAEL", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "NAME.3" + "\u0000" + "CONSTANZIA", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "NAME.4" + "\u0000" + "LUCA", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "NAME.5" + "\u0000" + "VINCENT", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENDER.0" + "\u0000" + "MALE", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENDER.1" + "\u0000" + "MALE", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENDER.2" + "\u0000" + "MALE", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENDER.3" + "\u0000" + "FEMALE", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENDER.4" + "\u0000" + "MALE", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENDER.5" + "\u0000" + "MALE", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "AGE.0" + "\u0000" + "24", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "AGE.1" + "\u0000" + "22", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "AGE.2" + "\u0000" + "20", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "AGE.3" + "\u0000" + "18", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "AGE.4" + "\u0000" + "40", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "AGE.5" + "\u0000" + "22", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "BIRTHDAY.0" + "\u0000" + "1", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "BIRTHDAY.1" + "\u0000" + "2", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "BIRTHDAY.2" + "\u0000" + "3", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "BIRTHDAY.3" + "\u0000" + "4", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "BIRTHDAY.4" + "\u0000" + "5", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "BIRTHDAY.5" + "\u0000" + "22", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "UUID.0" + "\u0000" + "CORLEONE", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "RECORD" + "\u0000" + "1", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "RECORD" + "\u0000" + "2", columnVisibilityItalian, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + sopranoUID, "NAME.0" + "\u0000" + "ANTHONY", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "NAME.1" + "\u0000" + "MEADOW", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "GENDER.0" + "\u0000" + "MALE", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "GENDER.1" + "\u0000" + "FEMALE", columnVisibilityEnglish, timeStamp, emptyValue);
            // to test whether singleton values correctly get matched using the function set methods, only add AGE.1
            // mutation.put(datatype + "\u0000" + sopranoUID, "AGE.0" + "\u0000" + "16", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "AGE.0" + "\u0000" + "16", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "AGE.1" + "\u0000" + "18", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "UUID.0" + "\u0000" + "SOPRANO", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "RECORD" + "\u0000" + "1", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "RECORD" + "\u0000" + "2", columnVisibilityItalian, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + caponeUID, "NAME.0" + "\u0000" + "ALPHONSE", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "NAME.1" + "\u0000" + "FRANK", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "NAME.2" + "\u0000" + "RALPH", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "NAME.3" + "\u0000" + "MICHAEL", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "GENDER.0" + "\u0000" + "MALE", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "GENDER.1" + "\u0000" + "MALE", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "GENDER.2" + "\u0000" + "MALE", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "GENDER.3" + "\u0000" + "MALE", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "AGE.0" + "\u0000" + "30", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "AGE.1" + "\u0000" + "34", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "AGE.2" + "\u0000" + "20", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "AGE.3" + "\u0000" + "40", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "UUID.0" + "\u0000" + "CAPONE", columnVisibilityEnglish, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "RECORD" + "\u0000" + "1", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "RECORD" + "\u0000" + "2", columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "RECORD" + "\u0000" + "3", columnVisibilityItalian, timeStamp, emptyValue);
            
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        try {
            // write shard index table:
            bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
            // corleones
            // uuid
            mutation = new Mutation(lcNoDiacriticsType.normalize("CORLEONE"));
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityItalian, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            
            // sopranos
            // uuid
            mutation = new Mutation(lcNoDiacriticsType.normalize("SOPRANO"));
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            // capones
            // uuid
            mutation = new Mutation(lcNoDiacriticsType.normalize("CAPONE"));
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            
            // corleone names
            mutation = new Mutation(lcNoDiacriticsType.normalize("SANTINO"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityItalian, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("FREDO"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityItalian, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("MICHAEL"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityItalian, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID, caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("CONSTANZIA"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityItalian, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("LUCA"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityItalian, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("VINCENT"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityItalian, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            
            // soprano names
            mutation = new Mutation(lcNoDiacriticsType.normalize("ANTHONY"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("MEADOW"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            
            // capone names
            mutation = new Mutation(lcNoDiacriticsType.normalize("ALPHONSE"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("FRANK"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("RALPH"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            
            // genders
            mutation = new Mutation(lcNoDiacriticsType.normalize("MALE"));
            mutation.put("GENDER".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID, caponeUID, corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("FEMALE"));
            mutation.put("GENDER".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID, corleoneUID));
            bw.addMutation(mutation);
            
            // ages
            mutation = new Mutation(numberType.normalize("24"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityItalian, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("22"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityItalian, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("20"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityItalian, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("18"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID, corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("40"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityItalian, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID, caponeUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(numberType.normalize("16"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(numberType.normalize("30"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("34"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("20"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibilityEnglish, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
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
            mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("CORLEONE") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            // names
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("SANTINO") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("FREDO") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("MICHAEL") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("CONSTANZIA") + "\u0000" + datatype + "\u0000" + corleoneUID,
                            columnVisibilityItalian, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("LUCA") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("VINCENT") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian,
                            timeStamp, emptyValue);
            // genders
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("FEMALE") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian,
                            timeStamp, emptyValue);
            // ages
            mutation.put("fi\u0000" + "AGE", numberType.normalize("24") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("22") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("20") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("18") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("40") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibilityItalian, timeStamp,
                            emptyValue);
            
            // sopranos
            // uuid
            mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("SOPRANO") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            // names
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("ANTHONY") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("MEADOW") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            // genders
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("FEMALE") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            // ages
            mutation.put("fi\u0000" + "AGE", numberType.normalize("16") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibilityEnglish, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("18") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibilityEnglish, timeStamp,
                            emptyValue);
            
            // capones
            // uuid
            mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("CAPONE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            // names
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("ALPHONSE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("FRANK") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("RALPH") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("MICHAEL") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            // genders
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish,
                            timeStamp, emptyValue);
            // ages
            mutation.put("fi\u0000" + "AGE", numberType.normalize("30") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("34") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("20") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("40") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibilityEnglish, timeStamp,
                            emptyValue);
            
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        try {
            // write metadata table:
            bw = con.createBatchWriter(QueryTestTableHelper.MODEL_TABLE_NAME, bwConfig);
            
            mutation = new Mutation("NAME");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(10L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("GENDER");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(11L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("AGE");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(12L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + numberType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("UUID");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("BIRTHDAY");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(12L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + numberType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("RECORD");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(7L)));
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
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
