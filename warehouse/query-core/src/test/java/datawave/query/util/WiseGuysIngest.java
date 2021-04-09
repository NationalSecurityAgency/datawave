package datawave.query.util;

import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.type.DateType;
import datawave.data.type.IpAddressType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.query.QueryTestTableHelper;
import datawave.util.TableName;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WiseGuysIngest {
    
    public enum WhatKindaRange {
        SHARD, DOCUMENT;
    }
    
    private static final Type<?> lcNoDiacriticsType = new LcNoDiacriticsType();
    private static final Type<?> ipAddressType = new IpAddressType();
    private static final Type<?> numberType = new NumberType();
    private static final Type<?> dateType = new DateType();
    
    protected static final String datatype = "test";
    protected static final String date = "20130101";
    protected static final String shard = date + "_0";
    protected static final ColumnVisibility columnVisibility = new ColumnVisibility("ALL");
    protected static final Value emptyValue = new Value(new byte[0]);
    protected static final long timeStamp = 1356998400000l;
    
    public static final String corleoneUID = UID.builder().newId("Corleone".getBytes(), (Date) null).toString();
    public static final String corleoneChildUID = UID.builder().newId("Corleone".getBytes(), (Date) null, "1").toString();
    public static final String sopranoUID = UID.builder().newId("Soprano".toString().getBytes(), (Date) null).toString();
    public static final String caponeUID = UID.builder().newId("Capone".toString().getBytes(), (Date) null).toString();
    
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
        
        try {
            // write the shard table :
            bw = con.createBatchWriter(TableName.SHARD, bwConfig);
            mutation = new Mutation(shard);
            
            mutation.put(datatype + "\u0000" + corleoneUID, "NOME.0" + "\u0000" + "SANTINO", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "NOME.1" + "\u0000" + "FREDO", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "NOME.2" + "\u0000" + "MICHAEL", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "NOME.3" + "\u0000" + "CONSTANZIA", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "NOME.4" + "\u0000" + "LUCA", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "NOME.5" + "\u0000" + "VINCENT", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.0" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.1" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.2" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.3" + "\u0000" + "FEMALE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.4" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.5" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "ETA.0" + "\u0000" + "24", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "ETA.1" + "\u0000" + "22", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "ETA.2" + "\u0000" + "20", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "ETA.3" + "\u0000" + "18", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "ETA.4" + "\u0000" + "40", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "ETA.5" + "\u0000" + "22", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "MAGIC.0" + "\u0000" + "18", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "UUID.0" + "\u0000" + "CORLEONE", columnVisibility, timeStamp, emptyValue);
            // CORLEONE date delta is 70 years
            mutation.put(datatype + "\u0000" + corleoneUID, "BIRTH_DATE" + "\u0000" + "1930-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "DEATH_DATE" + "\u0000" + "2000-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneUID, "QUOTE" + "\u0000" + "Im gonna make him an offer he cant refuse", columnVisibility, timeStamp,
                            emptyValue);
            
            mutation.put(datatype + "\u0000" + corleoneChildUID, "UUID.0" + "\u0000" + "ANDOLINI", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneChildUID, "ETA.0" + "\u0000" + "12", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneChildUID, "BIRTH_DATE" + "\u0000" + "1930-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + corleoneChildUID, "DEATH_DATE" + "\u0000" + "2000-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + sopranoUID, "NAME.0" + "\u0000" + "ANTHONY", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "NAME.1" + "\u0000" + "MEADOW", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "GENDER.0" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "GENDER.1" + "\u0000" + "FEMALE", columnVisibility, timeStamp, emptyValue);
            // to test whether singleton values correctly get matched using the function set methods, only add AGE.1
            // mutation.put(datatype + "\u0000" + sopranoUID, "AGE.0" + "\u0000" + "16", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "AGE.0" + "\u0000" + "16", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "AGE.1" + "\u0000" + "18", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "MAGIC.0" + "\u0000" + "18", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "UUID.0" + "\u0000" + "SOPRANO", columnVisibility, timeStamp, emptyValue);
            // soprano date delta is 50 years
            mutation.put(datatype + "\u0000" + sopranoUID, "BIRTH_DATE" + "\u0000" + "1950-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "DEATH_DATE" + "\u0000" + "2000-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + sopranoUID, "QUOTE" + "\u0000" + "If you can quote the rules then you can obey them", columnVisibility,
                            timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + caponeUID, "NAME.0" + "\u0000" + "ALPHONSE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "NAME.1" + "\u0000" + "FRANK", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "NAME.2" + "\u0000" + "RALPH", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "NAME.3" + "\u0000" + "MICHAEL", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "GENDER.0" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "GENDER.1" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "GENDER.2" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "GENDER.3" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "AGE.0" + "\u0000" + "30", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "AGE.1" + "\u0000" + "34", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "AGE.2" + "\u0000" + "20", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "AGE.3" + "\u0000" + "40", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "MAGIC.0" + "\u0000" + "18", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "UUID.0" + "\u0000" + "CAPONE", columnVisibility, timeStamp, emptyValue);
            
            // capone date delta is 89 or 90 years
            mutation.put(datatype + "\u0000" + caponeUID, "BIRTH_DATE.0" + "\u0000" + "1910-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
            // add a second date to test function taking an Iterable
            mutation.put(datatype + "\u0000" + caponeUID, "BIRTH_DATE.1" + "\u0000" + "1911-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "DEATH_DATE.0" + "\u0000" + "2000-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + caponeUID, "QUOTE" + "\u0000"
                            + "You can get much farther with a kind word and a gun than you can with a kind word alone", columnVisibility, timeStamp,
                            emptyValue);
            
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
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(lcNoDiacriticsType.normalize("ANDOLINI"));
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneChildUID));
            bw.addMutation(mutation);
            
            // names
            mutation = new Mutation(lcNoDiacriticsType.normalize("SANTINO"));
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("FREDO"));
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("MICHAEL"));
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("CONSTANZIA"));
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("LUCA"));
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("VINCENT"));
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            
            // genders
            mutation = new Mutation(lcNoDiacriticsType.normalize("MALE"));
            mutation.put("GENDER".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID, caponeUID));
            mutation.put("GENERE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("FEMALE"));
            mutation.put("GENDER".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            mutation.put("GENERE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            
            // ages
            mutation = new Mutation(numberType.normalize("24"));
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("22"));
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("20"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("18"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("40"));
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("12"));
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneChildUID));
            bw.addMutation(mutation);
            
            // sopranos
            // uuid
            mutation = new Mutation(lcNoDiacriticsType.normalize("SOPRANO"));
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            // names
            mutation = new Mutation(lcNoDiacriticsType.normalize("ANTHONY"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("MEADOW"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            // genders
            
            // ages
            mutation = new Mutation(numberType.normalize("16"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("18"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            
            // capones
            // uuid
            mutation = new Mutation(lcNoDiacriticsType.normalize("CAPONE"));
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            // names
            mutation = new Mutation(lcNoDiacriticsType.normalize("ALPHONSE"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("FRANK"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("RALPH"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("MICHAEL"));
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            // genders
            // ages
            mutation = new Mutation(numberType.normalize("30"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("34"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("20"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("40"));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(numberType.normalize("12"));
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneChildUID));
            bw.addMutation(mutation);
            
            // add some index-only fields
            mutation = new Mutation("chicago");
            mutation.put("LOCATION", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation("newyork");
            mutation.put("POSIZIONE", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation("newjersey");
            mutation.put("LOCATION", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            mutation = new Mutation("11y");
            mutation.put("SENTENCE", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            
            // add some tokens
            addTokens(bw, range, "QUOTE", "Im gonna make him an offer he cant refuse", corleoneUID);
            addTokens(bw, range, "QUOTE", "If you can quote the rules then you can obey them", sopranoUID);
            addTokens(bw, range, "QUOTE", "You can get much farther with a kind word and a gun than you can with a kind word alone", caponeUID);
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        try {
            
            bw = con.createBatchWriter(TableName.SHARD_RINDEX, bwConfig);
            // write the reverse index table:
            // corleones
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("CORLEONE")).reverse());
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("ANDOLINI")).reverse());
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneChildUID));
            bw.addMutation(mutation);
            
            // names
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("SANTINO")).reverse());
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("FREDO")).reverse());
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MICHAEL")).reverse());
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("CONSTANZIA")).reverse());
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("LUCA")).reverse());
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("VINCENT")).reverse());
            mutation.put("NOME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            // genders
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MALE")).reverse());
            mutation.put("GENERE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MALE")).reverse());
            mutation.put("GENERE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MALE")).reverse());
            mutation.put("GENERE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("FEMALE")).reverse());
            mutation.put("GENERE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MALE")).reverse());
            mutation.put("GENERE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MALE")).reverse());
            mutation.put("GENERE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            // ages
            mutation = new Mutation(new StringBuilder(numberType.normalize("24")).reverse());
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(numberType.normalize("22")).reverse());
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(numberType.normalize("20")).reverse());
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(numberType.normalize("18")).reverse());
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(numberType.normalize("40")).reverse());
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(numberType.normalize("12")).reverse());
            mutation.put("ETA".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneChildUID));
            bw.addMutation(mutation);
            
            // sopranos
            // uuid
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("SOPRANO")).reverse());
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            // names
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("ANTHONY")).reverse());
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MEADOW")).reverse());
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            // genders
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MALE")).reverse());
            mutation.put("GENDER".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("FEMALE")).reverse());
            mutation.put("GENDER".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            // ages
            mutation = new Mutation(new StringBuilder(numberType.normalize("16")).reverse());
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(numberType.normalize("18")).reverse());
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
            bw.addMutation(mutation);
            
            // capones
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("CAPONE")).reverse());
            mutation.put("UUID".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            // names
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("ALPHONSE")).reverse());
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("FRANK")).reverse());
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("RALPH")).reverse());
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MICHAEL")).reverse());
            mutation.put("NAME".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            // genders
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MALE")).reverse());
            mutation.put("GENDER".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MALE")).reverse());
            mutation.put("GENDER".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MALE")).reverse());
            mutation.put("GENDER".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(lcNoDiacriticsType.normalize("MALE")).reverse());
            mutation.put("GENDER".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            // ages
            mutation = new Mutation(new StringBuilder(numberType.normalize("30")).reverse());
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(numberType.normalize("34")).reverse());
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(numberType.normalize("20")).reverse());
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder(numberType.normalize("40")).reverse());
            mutation.put("AGE".toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            
            // add some index-only fields
            mutation = new Mutation(new StringBuilder("chicago").reverse());
            mutation.put("LOCATION", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(caponeUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder("newyork").reverse());
            mutation.put("POSIZIONE", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(corleoneUID));
            bw.addMutation(mutation);
            mutation = new Mutation(new StringBuilder("newjersey").reverse());
            mutation.put("LOCATION", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(sopranoUID));
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
            mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("CORLEONE") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility,
                            timeStamp, emptyValue);
            
            // uuid
            mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("ANDOLINI") + "\u0000" + datatype + "\u0000" + corleoneChildUID, columnVisibility,
                            timeStamp, emptyValue);
            
            // names
            mutation.put("fi\u0000" + "NOME", lcNoDiacriticsType.normalize("SANTINO") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NOME", lcNoDiacriticsType.normalize("FREDO") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NOME", lcNoDiacriticsType.normalize("MICHAEL") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NOME", lcNoDiacriticsType.normalize("CONSTANZIA") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NOME", lcNoDiacriticsType.normalize("LUCA") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "NOME", lcNoDiacriticsType.normalize("VINCENT") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility,
                            timeStamp, emptyValue);
            // genders
            mutation.put("fi\u0000" + "GENERE", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "GENERE", lcNoDiacriticsType.normalize("FEMALE") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility,
                            timeStamp, emptyValue);
            // ages
            mutation.put("fi\u0000" + "ETA", numberType.normalize("24") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "ETA", numberType.normalize("22") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "ETA", numberType.normalize("20") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "ETA", numberType.normalize("18") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "ETA", numberType.normalize("40") + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "ETA", numberType.normalize("12") + "\u0000" + datatype + "\u0000" + corleoneChildUID, columnVisibility, timeStamp,
                            emptyValue);
            
            // sopranos
            // uuid
            mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("SOPRANO") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibility,
                            timeStamp, emptyValue);
            // names
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("ANTHONY") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibility,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("MEADOW") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibility,
                            timeStamp, emptyValue);
            // genders
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibility,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("FEMALE") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibility,
                            timeStamp, emptyValue);
            // ages
            mutation.put("fi\u0000" + "AGE", numberType.normalize("16") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("18") + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibility, timeStamp, emptyValue);
            
            // capones
            // uuid
            mutation.put("fi\u0000" + "UUID", lcNoDiacriticsType.normalize("CAPONE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp,
                            emptyValue);
            // names
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("ALPHONSE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("FRANK") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("RALPH") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "NAME", lcNoDiacriticsType.normalize("MICHAEL") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility,
                            timeStamp, emptyValue);
            // genders
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "GENDER", lcNoDiacriticsType.normalize("MALE") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp,
                            emptyValue);
            // ages
            mutation.put("fi\u0000" + "AGE", numberType.normalize("30") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("34") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("20") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "AGE", numberType.normalize("40") + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp, emptyValue);
            
            // add some index-only fields
            mutation.put("fi\u0000" + "LOCATION", "chicago" + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "POSIZIONE", "newyork" + "\u0000" + datatype + "\u0000" + corleoneUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "LOCATION", "newjersey" + "\u0000" + datatype + "\u0000" + sopranoUID, columnVisibility, timeStamp, emptyValue);
            mutation.put("fi\u0000" + "SENTENCE", "11y" + "\u0000" + datatype + "\u0000" + caponeUID, columnVisibility, timeStamp, emptyValue);
            
            bw.addMutation(mutation);
            
            addFiTokens(bw, range, "QUOTE", "Im gonna make him an offer he cant refuse", corleoneUID);
            addFiTokens(bw, range, "QUOTE", "If you can quote the rules then you can obey them", sopranoUID);
            addFiTokens(bw, range, "QUOTE", "You can get much farther with a kind word and a gun than you can with a kind word alone", caponeUID);
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
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + normalizerForColumn("NAME")), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("NOME");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(19L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + normalizerForColumn("NOME")), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("GENDER");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(11L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + normalizerForColumn("GENDER")), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("GENERE");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(21L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + normalizerForColumn("GENERE")), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("AGE");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(12L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + normalizerForColumn("AGE")), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("ETA");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(22L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + normalizerForColumn("ETA")), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("MAGIC");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(12L)));
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + normalizerForColumn("AGE")), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("ETA");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(12L)));
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + normalizerForColumn("AGE")), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("UUID");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + normalizerForColumn("UUID")), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("BIRTH_DATE");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + dateType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("DEATH_DATE");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + dateType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            // index only fields
            mutation = new Mutation("LOCATION");
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("POSIZIONE");
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("SENTENCE");
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            // type column intentionally omitted
            bw.addMutation(mutation);
            
            // add some fields to test for null
            mutation = new Mutation("NULL1");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("NULL2");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            // add a field to test tokens
            mutation = new Mutation("QUOTE");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_TF, new Text(datatype), emptyValue);
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        try {
            // write forward model:
            bw = con.createBatchWriter(QueryTestTableHelper.MODEL_TABLE_NAME, bwConfig);
            
            mutation = new Mutation("NAM");
            mutation.put("DATAWAVE", "NAME" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "NOME" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("AG");
            mutation.put("DATAWAVE", "AGE" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "ETA" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("GEN");
            mutation.put("DATAWAVE", "GENDER" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "GENERE" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("LOC");
            mutation.put("DATAWAVE", "LOCATION" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "POSIZIONE" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("BOTH_NULL");
            mutation.put("DATAWAVE", "NULL1" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "NULL2" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("ONE_NULL");
            mutation.put("DATAWAVE", "NULL1" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "UUID" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        try {
            // write reverse model:
            bw = con.createBatchWriter(QueryTestTableHelper.MODEL_TABLE_NAME, bwConfig);
            
            mutation = new Mutation("NOME");
            mutation.put("DATAWAVE", "NAM" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("NAME");
            mutation.put("DATAWAVE", "NAM" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("AGE");
            mutation.put("DATAWAVE", "AG" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("ETA");
            mutation.put("DATAWAVE", "AG" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("GENDER");
            mutation.put("DATAWAVE", "GEN" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("GENERE");
            mutation.put("DATAWAVE", "GEN" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("LOCATION");
            mutation.put("DATAWAVE", "LOC" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("POSIZIONE");
            mutation.put("DATAWAVE", "LOC" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("NULL1");
            mutation.put("DATAWAVE", "BOTH_NULL" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("NULL2");
            mutation.put("DATAWAVE", "BOTH_NULL" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("NULL1");
            mutation.put("DATAWAVE", "ONE_NULL" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("UUID");
            mutation.put("DATAWAVE", "ONE_NULL" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
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
    
    private static void addTokens(BatchWriter bw, WhatKindaRange range, String field, String phrase, String uid) throws MutationsRejectedException {
        Mutation mutation = new Mutation(lcNoDiacriticsType.normalize(phrase));
        mutation.put(field.toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                        range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(uid));
        bw.addMutation(mutation);
        
        String[] tokens = phrase.split(" ");
        for (String token : tokens) {
            mutation = new Mutation(lcNoDiacriticsType.normalize(token));
            mutation.put(field.toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(uid));
            bw.addMutation(mutation);
        }
    }
    
    private static void addFiTokens(BatchWriter bw, WhatKindaRange range, String field, String phrase, String uid) throws MutationsRejectedException {
        Mutation fi = new Mutation(shard);
        fi.put("fi\u0000" + field.toUpperCase(), lcNoDiacriticsType.normalize(phrase) + "\u0000" + datatype + "\u0000" + uid, columnVisibility, timeStamp,
                        emptyValue);
        
        String[] tokens = phrase.split(" ");
        for (String token : tokens) {
            fi.put("fi\u0000" + field.toUpperCase(), lcNoDiacriticsType.normalize(token) + "\u0000" + datatype + "\u0000" + uid, columnVisibility, timeStamp,
                            emptyValue);
        }
        bw.addMutation(fi);
    }
}
