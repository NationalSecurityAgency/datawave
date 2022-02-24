package datawave.ingest.mapreduce.handler.dateindex;

import java.util.BitSet;

import datawave.data.normalizer.DateNormalizer;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.RawRecordContainerImplTest;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.DataTypeHelper.Properties;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.table.config.DateIndexTableConfigHelper;
import datawave.policy.IngestPolicyEnforcer;

import datawave.util.TypeRegistryTestSetup;
import datawave.util.TableName;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class DateIndexDataTypeHandlerTest {
    
    private Configuration conf;
    private DateIndexDataTypeHandler<Text> handler;
    private TestBaseIngestHelper helper = new TestBaseIngestHelper();
    private DateNormalizer dateNormalizer = new DateNormalizer();
    
    @Before
    public void setup() throws Exception {
        
        conf = new Configuration();
        conf.set("num.shards", "11");
        conf.set("data.name", "testdatatype");
        conf.set("testdatatype.ingest.helper.class", TestBaseIngestHelper.class.getName());
        conf.set("testdatatype.handler.classes", DateIndexDataTypeHandler.class.getName());
        
        // date index configuration
        conf.set("date.index.table.name", TableName.DATE_INDEX);
        conf.set("date.index.table.loader.priority", "30");
        conf.set("DateIndex.table.config.class", DateIndexTableConfigHelper.class.getName());
        conf.set("date.index.table.locality.groups", "activity:ACTIVITY,loaded:LOADED");
        
        // some date index configuration for our "testdatatype" events
        conf.set("testdatatype.date.index.type.to.field.map", "ACTIVITY=ACTIVITY_DATE,LOADED=LOAD_DATE");
        conf.set("all" + Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        
        TypeRegistryTestSetup.resetTypeRegistry(conf);
        
        handler = new DateIndexDataTypeHandler<>();
        handler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testMissingConfigSetup() {
        DateIndexDataTypeHandler<Text> handler = new DateIndexDataTypeHandler<>();
        handler.setup(new TaskAttemptContextImpl(new Configuration(), new TaskAttemptID()));
    }
    
    @Test
    public void testHandler() {
        // create a sample event
        String data = "00000000-0000-0000-0000-000000000000,UUID,FOO,null,2013-04-30T11:53:11Z,2013-04-29T11:53:10Z";
        RawRecordContainer event = getEvent(data);
        String shardId = new ShardIdFactory(conf).getShardId(event);
        int shard1 = ShardIdFactory.getShard(shardId);
        
        // create some fields
        Multimap<String,NormalizedContentInterface> eventFields = helper.getEventFields(event);
        
        // add a load date
        NormalizedContentInterface loadDate = new NormalizedFieldAndValue("LOAD_DATE", "2014-01-01T13:39:39Z");
        loadDate.setIndexedFieldValue(dateNormalizer.normalize(loadDate.getEventFieldValue()));
        eventFields.put("LOAD_DATE", loadDate);
        
        // process the event
        Multimap<BulkIngestKey,Value> mutations = handler.processBulk(new Text("1"), event, eventFields, null);
        Assert.assertEquals(0, mutations.size());
        // verify the mutations which are now placed in the metadata
        handler.getMetadata().addEvent(helper, event, eventFields);
        mutations = handler.getMetadata().getBulkMetadata();
        Assert.assertEquals(2, mutations.size());
        
        // verify that clear clears out the mutations
        handler.getMetadata().clear();
        mutations = handler.getMetadata().getBulkMetadata();
        Assert.assertEquals(0, mutations.size());
        
        // verify that the mutations are not duplicated
        handler.getMetadata().addEvent(helper, event, eventFields);
        handler.getMetadata().addEvent(helper, event, eventFields);
        Assert.assertEquals(2, mutations.size());
        
        BitSet expectedValue = DateIndexUtil.getBits(shard1);
        for (BulkIngestKey bulkIngestKey : mutations.keySet()) {
            Assert.assertEquals(1, mutations.get(bulkIngestKey).size());
            Value value = mutations.get(bulkIngestKey).iterator().next();
            Assert.assertEquals(TableName.DATE_INDEX, bulkIngestKey.getTableName().toString());
            Key key = bulkIngestKey.getKey();
            Assert.assertEquals(expectedValue, BitSet.valueOf(value.get()));
            if ("ACTIVITY".equals(key.getColumnFamily().toString())) {
                Assert.assertEquals("20130429_4", key.getRow().toString());
                Assert.assertEquals("20130430\0testdatatype\0ACTIVITY_DATE", key.getColumnQualifier().toString());
                Assert.assertEquals(dateNormalizer.denormalize("2013-04-29T00:00:00Z").getTime(), key.getTimestamp());
            } else if ("LOADED".equals(key.getColumnFamily().toString())) {
                Assert.assertEquals("20140101_3", key.getRow().toString());
                Assert.assertEquals("20130430\0testdatatype\0LOAD_DATE", key.getColumnQualifier().toString());
                Assert.assertEquals(dateNormalizer.denormalize("2014-01-01T00:00:00Z").getTime(), key.getTimestamp());
            } else {
                Assert.fail("Unexpected colf: " + key.getColumnFamily());
            }
        }
        
    }
    
    @Test
    public void testHandlerMultiEventReduction() {
        // create a sample event
        String data = "00000000-0000-0000-0000-000000000000,UUID,FOO,null,2013-04-30T11:53:11Z,2013-04-29T11:53:10Z";
        RawRecordContainer event = getEvent(data);
        String shardId = new ShardIdFactory(conf).getShardId(event);
        int shard1 = ShardIdFactory.getShard(shardId);
        
        // create some fields
        Multimap<String,NormalizedContentInterface> eventFields = helper.getEventFields(event);
        
        // add a load date
        NormalizedContentInterface loadDate = new NormalizedFieldAndValue("LOAD_DATE", "2014-01-01T13:39:39Z");
        loadDate.setIndexedFieldValue(dateNormalizer.normalize(loadDate.getEventFieldValue()));
        eventFields.put("LOAD_DATE", loadDate);
        
        // process the event
        handler.getMetadata().addEvent(helper, event, eventFields);
        Multimap<BulkIngestKey,Value> mutations = handler.getMetadata().getBulkMetadata();
        Assert.assertEquals(2, mutations.size());
        
        // create a second sample event
        data = "00000000-0000-0000-0000-000000000000,UUID,FOO,null,2013-04-30T00:00:00Z,2013-04-29T11:11:11Z";
        event = getEvent(data);
        shardId = new ShardIdFactory(conf).getShardId(event);
        int shard2 = ShardIdFactory.getShard(shardId);
        eventFields = helper.getEventFields(event);
        // do not put the load date in this one: eventFields.put("LOAD_DATE", loadDate);
        
        // ensure we have a different shard for testing purposes
        Assert.assertNotEquals(shard1, shard2);
        
        // verify that the mutations are reduced
        handler.getMetadata().addEvent(helper, event, eventFields);
        mutations = handler.getMetadata().getBulkMetadata();
        Assert.assertEquals(2, mutations.size());
        
        for (BulkIngestKey bulkIngestKey : mutations.keySet()) {
            Assert.assertEquals(1, mutations.get(bulkIngestKey).size());
            Value value = mutations.get(bulkIngestKey).iterator().next();
            Assert.assertEquals(TableName.DATE_INDEX, bulkIngestKey.getTableName().toString());
            Key key = bulkIngestKey.getKey();
            if ("ACTIVITY".equals(key.getColumnFamily().toString())) {
                BitSet expectedValue = DateIndexUtil.merge(DateIndexUtil.getBits(shard1), DateIndexUtil.getBits(shard2));
                Assert.assertEquals(expectedValue, BitSet.valueOf(value.get()));
                Assert.assertEquals("20130429_4", key.getRow().toString());
                Assert.assertEquals("20130430\0testdatatype\0ACTIVITY_DATE", key.getColumnQualifier().toString());
                Assert.assertEquals(dateNormalizer.denormalize("2013-04-29T00:00:00Z").getTime(), key.getTimestamp());
            } else if ("LOADED".equals(key.getColumnFamily().toString())) {
                BitSet expectedValue = DateIndexUtil.getBits(shard1);
                Assert.assertEquals(expectedValue, BitSet.valueOf(value.get()));
                Assert.assertEquals("20140101_3", key.getRow().toString());
                Assert.assertEquals("20130430\0testdatatype\0LOAD_DATE", key.getColumnQualifier().toString());
                Assert.assertEquals(dateNormalizer.denormalize("2014-01-01T00:00:00Z").getTime(), key.getTimestamp());
            } else {
                Assert.fail("Unexpected colf: " + key.getColumnFamily());
            }
        }
        
    }
    
    private RawRecordContainer getEvent(String data) {
        RawRecordContainerImplTest.ValidatingRawRecordContainerImpl event = new RawRecordContainerImplTest.ValidatingRawRecordContainerImpl();
        event.setDataType(TypeRegistry.getType("testdatatype"));
        event.setSecurityMarkings(null);
        event.setVisibility(new ColumnVisibility("A&B"));
        event.setDate(getTime(data));
        event.setRawFileName("DateIndexDataTypeHandlerTest.data");
        event.setRawRecordNumber(1l);
        event.setRawData(data.getBytes());
        event.generateId(null);
        event.validate();
        return event;
    }
    
    private static String getIndexedValue(String dataStr, int index) {
        String[] data = StringUtils.split(dataStr, ',');
        return (data.length > index ? data[index] : null);
    }
    
    private static String getId(String data) {
        return getIndexedValue(data, 0);
    }
    
    private static String getType(String data) {
        return getIndexedValue(data, 1);
    }
    
    private static String getState(String data) {
        return getIndexedValue(data, 2);
    }
    
    private static String getOriginator(String data) {
        String o = getIndexedValue(data, 3);
        if (o == null) {
            return "";
        } else {
            return (o.equals("null") ? "" : o);
        }
    }
    
    private static String getDate(String data) {
        return getIndexedValue(data, 4);
    }
    
    private static String getActivityDate(String data) {
        return getIndexedValue(data, 5);
    }
    
    private static long getTime(String data) {
        String date = getDate(data);
        if (date != null) {
            DateNormalizer dateNormalizer = new DateNormalizer();
            return dateNormalizer.denormalize(dateNormalizer.normalize(date)).getTime();
        } else {
            return 0;
        }
    }
    
    public static class TestBaseIngestHelper extends BaseIngestHelper {
        private DateNormalizer dateNormalizer = new DateNormalizer();
        
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            
            String data = new String(event.getRawData());
            eventFields.put("ID", new BaseNormalizedContent("ID", DateIndexDataTypeHandlerTest.getId(data)));
            eventFields.put("TYPE", new BaseNormalizedContent("TYPE", DateIndexDataTypeHandlerTest.getType(data)));
            eventFields.put("STATE", new BaseNormalizedContent("STATE", DateIndexDataTypeHandlerTest.getState(data)));
            eventFields.put("ORIGINATOR", new BaseNormalizedContent("ORIGINATOR", DateIndexDataTypeHandlerTest.getOriginator(data)));
            eventFields.put("DATE", new BaseNormalizedContent("DATE", DateIndexDataTypeHandlerTest.getDate(data)));
            NormalizedContentInterface activityDate = new NormalizedFieldAndValue("ACTIVITY_DATE", DateIndexDataTypeHandlerTest.getActivityDate(data));
            activityDate.setIndexedFieldValue(dateNormalizer.normalize(activityDate.getEventFieldValue()));
            eventFields.put("ACTIVITY_DATE", activityDate);
            
            return eventFields;
        }
    }
}
