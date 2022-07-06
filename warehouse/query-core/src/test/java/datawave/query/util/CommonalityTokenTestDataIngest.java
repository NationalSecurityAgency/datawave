package datawave.query.util;

import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
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
 * write data in accumulo for testing of the limit.fields function on data with a commonality token
 */
public class CommonalityTokenTestDataIngest {
    
    public enum WhatKindaRange {
        SHARD, DOCUMENT;
    }
    
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
            
            mutation.put(datatype + "\u0000" + myUID, "CAT.PET.0" + "\u0000" + "tabby", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "CANINE.PET.0" + "\u0000" + "beagle", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FISH.PET.0" + "\u0000" + "beta", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "BIRD.PET.0" + "\u0000" + "parakeet", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + myUID, "CAT.PET.1" + "\u0000" + "calico", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "CANINE.PET.1" + "\u0000" + "basset", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FISH.PET.1" + "\u0000" + "goldfish", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "BIRD.PET.1" + "\u0000" + "canary", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + myUID, "CAT.PET.2" + "\u0000" + "tom", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "CANINE.PET.2" + "\u0000" + "chihuahua", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FISH.PET.2" + "\u0000" + "angelfish", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "BIRD.PET.2" + "\u0000" + "parrot", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + myUID, "CAT.PET.3" + "\u0000" + "siamese", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "CANINE.PET.3" + "\u0000" + "dachshund", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FISH.PET.3" + "\u0000" + "guppy", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "BIRD.PET.3" + "\u0000" + "budgie", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + myUID, "CAT.WILD.0" + "\u0000" + "cougar", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "CANINE.WILD.0" + "\u0000" + "wolf", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FISH.WILD.0" + "\u0000" + "shark", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "BIRD.WILD.0" + "\u0000" + "eagle", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + myUID, "CAT.WILD.1" + "\u0000" + "tiger", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "CANINE.WILD.1" + "\u0000" + "coyote", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FISH.WILD.1" + "\u0000" + "tuna", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "BIRD.WILD.1" + "\u0000" + "hawk", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + myUID, "CAT.WILD.2" + "\u0000" + "leopard", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "CANINE.WILD.2" + "\u0000" + "fox", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FISH.WILD.2" + "\u0000" + "mackerel", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "BIRD.WILD.2" + "\u0000" + "crow", columnVisibility, timeStamp, emptyValue);
            
            mutation.put(datatype + "\u0000" + myUID, "CAT.WILD.3" + "\u0000" + "puma", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "CANINE.WILD.3" + "\u0000" + "dingo", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "FISH.WILD.3" + "\u0000" + "salmon", columnVisibility, timeStamp, emptyValue);
            mutation.put(datatype + "\u0000" + myUID, "BIRD.WILD.3" + "\u0000" + "buzzard", columnVisibility, timeStamp, emptyValue);
            
            bw.addMutation(mutation);
            
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
        
        try {
            // write shard index table:
            bw = con.createBatchWriter(TableName.SHARD_INDEX, bwConfig);
            
            // all the cats
            mutation = new Mutation(lcNoDiacriticsType.normalize("tabby"));
            mutation.put("CAT", shard + "\u0000" + datatype, columnVisibility, timeStamp, range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree()
                            : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("calico"));
            mutation.put("CAT", shard + "\u0000" + datatype, columnVisibility, timeStamp, range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree()
                            : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("tom"));
            mutation.put("CAT", shard + "\u0000" + datatype, columnVisibility, timeStamp, range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree()
                            : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("siamese"));
            mutation.put("CAT", shard + "\u0000" + datatype, columnVisibility, timeStamp, range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree()
                            : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("cougar"));
            mutation.put("CAT", shard + "\u0000" + datatype, columnVisibility, timeStamp, range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree()
                            : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("tiger"));
            mutation.put("CAT", shard + "\u0000" + datatype, columnVisibility, timeStamp, range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree()
                            : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("leopard"));
            mutation.put("CAT", shard + "\u0000" + datatype, columnVisibility, timeStamp, range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree()
                            : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("puma"));
            mutation.put("CAT", shard + "\u0000" + datatype, columnVisibility, timeStamp, range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree()
                            : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            // all the canines
            mutation = new Mutation(lcNoDiacriticsType.normalize("beagle"));
            mutation.put("CANINE", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("basset"));
            mutation.put("CANINE", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("chihuahua"));
            mutation.put("CANINE", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("dachshund"));
            mutation.put("CANINE", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("wolf"));
            mutation.put("CANINE", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("coyote"));
            mutation.put("CANINE", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("fox"));
            mutation.put("CANINE", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("dingo"));
            mutation.put("CANINE", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            // all the fish
            mutation = new Mutation(lcNoDiacriticsType.normalize("beta"));
            mutation.put("FISH", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("goldfish"));
            mutation.put("FISH", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("angelfish"));
            mutation.put("FISH", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("guppy"));
            mutation.put("FISH", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("shark"));
            mutation.put("FISH", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("tuna"));
            mutation.put("FISH", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("mackerel"));
            mutation.put("FISH", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("salmon"));
            mutation.put("FISH", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            
            // all the birds
            mutation = new Mutation(lcNoDiacriticsType.normalize("parakeet"));
            mutation.put("BIRD", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("canary"));
            mutation.put("BIRD", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("parrot"));
            mutation.put("BIRD", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("budgie"));
            mutation.put("BIRD", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("eagle"));
            mutation.put("BIRD", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("hawk"));
            mutation.put("BIRD", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("crow"));
            mutation.put("BIRD", shard + "\u0000" + datatype, columnVisibility, timeStamp,
                            range == WhatKindaRange.SHARD ? getValueForNuthinAndYourHitsForFree() : getValueForBuilderFor(myUID));
            bw.addMutation(mutation);
            mutation = new Mutation(lcNoDiacriticsType.normalize("buzzard"));
            mutation.put("BIRD", shard + "\u0000" + datatype, columnVisibility, timeStamp,
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
            // cats
            mutation.put("fi\u0000" + "CAT", lcNoDiacriticsType.normalize("tabby") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CAT", lcNoDiacriticsType.normalize("calico") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CAT", lcNoDiacriticsType.normalize("tom") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CAT", lcNoDiacriticsType.normalize("siamese") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CAT", lcNoDiacriticsType.normalize("cougar") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CAT", lcNoDiacriticsType.normalize("tiger") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CAT", lcNoDiacriticsType.normalize("leopard") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CAT", lcNoDiacriticsType.normalize("puma") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            // dogs
            mutation.put("fi\u0000" + "CANINE", lcNoDiacriticsType.normalize("beagle") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CANINE", lcNoDiacriticsType.normalize("basset") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CANINE", lcNoDiacriticsType.normalize("chihuahua") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "CANINE", lcNoDiacriticsType.normalize("dachshund") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility,
                            timeStamp, emptyValue);
            mutation.put("fi\u0000" + "CANINE", lcNoDiacriticsType.normalize("wolf") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CANINE", lcNoDiacriticsType.normalize("coyote") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CANINE", lcNoDiacriticsType.normalize("fox") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "CANINE", lcNoDiacriticsType.normalize("dingo") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            
            // fish
            mutation.put("fi\u0000" + "FISH", lcNoDiacriticsType.normalize("beta") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "FISH", lcNoDiacriticsType.normalize("goldfish") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "FISH", lcNoDiacriticsType.normalize("angelfish") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "FISH", lcNoDiacriticsType.normalize("guppy") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "FISH", lcNoDiacriticsType.normalize("shark") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "FISH", lcNoDiacriticsType.normalize("tuna") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "FISH", lcNoDiacriticsType.normalize("mackerel") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "FISH", lcNoDiacriticsType.normalize("salmon") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            
            // birds
            mutation.put("fi\u0000" + "BIRD", lcNoDiacriticsType.normalize("parakeet") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "BIRD", lcNoDiacriticsType.normalize("canary") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "BIRD", lcNoDiacriticsType.normalize("parrot") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "BIRD", lcNoDiacriticsType.normalize("budgie") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "BIRD", lcNoDiacriticsType.normalize("eagle") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "BIRD", lcNoDiacriticsType.normalize("hawk") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "BIRD", lcNoDiacriticsType.normalize("crow") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
                            emptyValue);
            mutation.put("fi\u0000" + "BIRD", lcNoDiacriticsType.normalize("buzzard") + "\u0000" + datatype + "\u0000" + myUID, columnVisibility, timeStamp,
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
            
            mutation = new Mutation("CAT");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("CANINE");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("BIRD");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + lcNoDiacriticsType.getClass().getName()), emptyValue);
            bw.addMutation(mutation);
            
            mutation = new Mutation("FISH");
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
            mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(3L)));
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype), emptyValue);
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
