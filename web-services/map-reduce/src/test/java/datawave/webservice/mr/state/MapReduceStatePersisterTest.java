package datawave.webservice.mr.state;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.webservice.mr.state.MapReduceStatePersisterBean.MapReduceState;
import datawave.webservice.results.mr.MapReduceInfoResponse;
import datawave.webservice.results.mr.MapReduceInfoResponseList;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import javax.ejb.EJBContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.UUID;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.createStrictMock;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;
import static org.powermock.api.support.membermodification.MemberMatcher.field;

public class MapReduceStatePersisterTest {

    private static final String userDN = "CN=Guy Some Other soguy, OU=acme";
    private static final String sid = "soguy";
    private static final String[] auths = new String[] {"AUTHS"};
    private static final String TABLE_NAME = MapReduceStatePersisterBean.TABLE_NAME;
    private static final String INDEX_TABLE_NAME = MapReduceStatePersisterBean.INDEX_TABLE_NAME;
    private static final String workingDirectory = "/BulkResults/test";
    private static final String mapReduceJobId = "job_20120101_0001";
    private static final String hdfs = "hdfs://localhost:8021";
    private static final String jt = "localhost:5555";
    private static final String jobName = "testJob";
    private static final String resultsDirectory = "/BulkResults/test/results";
    private static final String runtimeParameters = "queryId=1234567890";
    private static final String NULL = "\u0000";
    private static String id = UUID.randomUUID().toString();

    private InMemoryInstance instance = new InMemoryInstance("test instance");
    private AccumuloClient client = null;
    private DatawavePrincipal principal = null;

    private AccumuloConnectionFactory connectionFactory = null;
    private EJBContext ctx = null;
    private MapReduceStatePersisterBean bean = null;

    @Before
    public void setup() throws Exception {
        System.setProperty(DnUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        client = new InMemoryAccumuloClient("root", instance);
        if (client.tableOperations().exists(TABLE_NAME))
            client.tableOperations().delete(TABLE_NAME);
        if (client.tableOperations().exists(INDEX_TABLE_NAME))
            client.tableOperations().delete(INDEX_TABLE_NAME);
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "CN=ca, OU=acme"), UserType.USER, Arrays.asList(auths), null, null, 0L);
        principal = new DatawavePrincipal(Collections.singletonList(user));
        connectionFactory = createMock(AccumuloConnectionFactory.class);
        ctx = createStrictMock(EJBContext.class);
        bean = new MapReduceStatePersisterBean();
        field(MapReduceStatePersisterBean.class, "connectionFactory").set(bean, connectionFactory);
        field(MapReduceStatePersisterBean.class, "ctx").set(bean, ctx);
        Logger.getLogger(MapReduceStatePersisterBean.class).setLevel(Level.OFF);
    }

    @Test
    public void testPersistentCreate() throws Exception {
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);

        HashMap<String,String> trackingMap = new HashMap<>();
        expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getClient(EasyMock.eq(null), EasyMock.eq(null), EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN),
                        EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        replayAll();
        bean.create(id, hdfs, jt, workingDirectory, mapReduceJobId, resultsDirectory, runtimeParameters, jobName);
        verifyAll();

        assertTrue(client.tableOperations().exists(TABLE_NAME));
        assertTrue(client.tableOperations().exists(INDEX_TABLE_NAME));

        String row = id;
        Key dirKey = new Key(row, sid, MapReduceStatePersisterBean.WORKING_DIRECTORY);
        Value dirValue = new Value(workingDirectory.getBytes());
        Key hdfsKey = new Key(row, sid, MapReduceStatePersisterBean.HDFS);
        Value hdfsValue = new Value(hdfs.getBytes());
        Key jtKey = new Key(row, sid, MapReduceStatePersisterBean.JT);
        Value jtValue = new Value(jt.getBytes());
        Key outKey = new Key(row, sid, MapReduceStatePersisterBean.RESULTS_LOCATION);
        Value outValue = new Value(resultsDirectory.getBytes());
        Key paramsKey = new Key(row, sid, MapReduceStatePersisterBean.PARAMS);
        Value paramsVal = new Value(runtimeParameters.getBytes());
        Key nameKey = new Key(row, sid, MapReduceStatePersisterBean.NAME);
        Value nameVal = new Value(jobName.getBytes());
        Key stateKey = new Key(row, sid, MapReduceStatePersisterBean.STATE + NULL + mapReduceJobId);
        Value stateVal = new Value(MapReduceState.STARTED.toString().getBytes());

        boolean dir = false;
        boolean hdfs = false;
        boolean jt = false;
        boolean output = false;
        boolean params = false;
        boolean state = false;

        Scanner s = client.createScanner(TABLE_NAME, new Authorizations(auths));
        s.setRange(new Range(row));
        for (Entry<Key,Value> entry : s) {
            assertEquals(sid, entry.getKey().getColumnFamily().toString());
            String colq = entry.getKey().getColumnQualifier().toString();
            if (MapReduceStatePersisterBean.WORKING_DIRECTORY.equals(colq)) {
                assertTrue(dirKey.equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
                assertEquals(dirValue, entry.getValue());
                dir = true;
            } else if (MapReduceStatePersisterBean.HDFS.equals(colq)) {
                assertTrue(hdfsKey.equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
                assertEquals(hdfsValue, entry.getValue());
                hdfs = true;
            } else if (MapReduceStatePersisterBean.JT.equals(colq)) {
                assertTrue(jtKey.equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
                assertEquals(jtValue, entry.getValue());
                jt = true;
            } else if (MapReduceStatePersisterBean.RESULTS_LOCATION.equals(colq)) {
                assertTrue(outKey.equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
                assertEquals(outValue, entry.getValue());
                output = true;
            } else if (MapReduceStatePersisterBean.PARAMS.equals(colq)) {
                assertTrue(paramsKey.equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
                assertEquals(paramsVal, entry.getValue());
                params = true;
            } else if (colq.startsWith(MapReduceStatePersisterBean.STATE)) {
                assertTrue(stateKey.equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
                assertEquals(stateVal, entry.getValue());
                state = true;
            } else if (MapReduceStatePersisterBean.NAME.equals(colq)) {
                assertTrue(nameKey.equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
                assertEquals(nameVal, entry.getValue());
            } else {
                fail("Unknown column, key: " + entry.getKey());
            }
        }
        if (!dir || !hdfs || !jt || !output || !params || !state)
            fail("Not all K/V checked out ok");

        Key indexKey = new Key(mapReduceJobId, sid, id);
        Value indexValue = MapReduceStatePersisterBean.NULL_VALUE;
        boolean index = false;
        s = client.createScanner(INDEX_TABLE_NAME, new Authorizations(auths));
        s.setRange(new Range(mapReduceJobId));
        s.fetchColumn(new Text(sid), new Text(id));
        for (Entry<Key,Value> entry : s) {
            assertTrue(indexKey.equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
            assertEquals(indexValue, entry.getValue());
            index = true;
        }
        if (!index)
            fail("Index K/V did not check out ok");
    }

    @Test
    public void testUpdateState() throws Exception {
        // create the initial entry
        testPersistentCreate();

        PowerMock.resetAll();

        // Get ready to call updateState
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getClient(EasyMock.eq(null), EasyMock.eq(null), EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN),
                        EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getClient(EasyMock.eq(null), EasyMock.eq(null), EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN),
                        EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        replayAll();

        bean.updateState(mapReduceJobId, MapReduceState.FAILED);
        verifyAll();

        // Ensure that the new FAILED state made it into the table
        Key failedKey = new Key(id, sid, MapReduceStatePersisterBean.STATE + NULL + mapReduceJobId);
        Value failedValue = new Value(MapReduceState.FAILED.toString().getBytes());
        boolean found = false;
        Scanner s = client.createScanner(TABLE_NAME, new Authorizations(auths));
        s.setRange(new Range(id));
        s.fetchColumnFamily(new Text(sid));

        for (Entry<Key,Value> entry : s) {
            if (entry.getKey().getColumnQualifier().toString().equals(MapReduceStatePersisterBean.STATE + NULL + mapReduceJobId)) {
                if (failedKey.equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL) && failedValue.equals(entry.getValue())) {
                    found = true;
                }
            }
        }
        if (!found)
            fail("Updated state not found");

    }

    @Test
    public void testFind() throws Exception {

        // create some entries
        testPersistentCreate();
        PowerMock.resetAll();
        id = UUID.randomUUID().toString();
        testPersistentCreate();
        PowerMock.resetAll();
        id = UUID.randomUUID().toString();
        testPersistentCreate();
        PowerMock.resetAll();

        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getClient(EasyMock.eq(null), EasyMock.eq(null), EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN),
                        EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        replayAll();

        MapReduceInfoResponseList result = bean.find();
        verifyAll();

        assertEquals(3, result.getResults().size());

    }

    @Test
    public void testFindNoResults() throws Exception {
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getClient(EasyMock.eq(null), EasyMock.eq(null), EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN),
                        EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        replayAll();

        MapReduceInfoResponseList result = bean.find();
        verifyAll();

        assertEquals(0, result.getResults().size());
    }

    @Test
    public void testDontFindSomeoneElsesResults() throws Exception {

        // create some entries
        testPersistentCreate();
        PowerMock.resetAll();
        id = UUID.randomUUID().toString();
        testPersistentCreate();
        PowerMock.resetAll();
        id = UUID.randomUUID().toString();
        testPersistentCreate();
        PowerMock.resetAll();

        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("CN=Gal Some Other sogal, OU=acme", "CN=ca, OU=acme"), UserType.USER, Arrays.asList(auths),
                        null, null, 0L);
        principal = new DatawavePrincipal(Collections.singletonList(user));
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getClient(EasyMock.eq(null), EasyMock.eq(null), EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN),
                        EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        replayAll();

        MapReduceInfoResponseList result = bean.find();
        verifyAll();

        assertEquals(0, result.getResults().size());
    }

    @Test
    public void testDontFindSomeoneElsesJob() throws Exception {

        // create some entries
        testPersistentCreate();
        PowerMock.resetAll();
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("CN=Gal Some Other sogal, OU=acme", "CN=ca, OU=acme"), UserType.USER, Arrays.asList(auths),
                        null, null, 0L);
        principal = new DatawavePrincipal(Collections.singletonList(user));
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getClient(EasyMock.eq(null), EasyMock.eq(null), EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN),
                        EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        replayAll();

        MapReduceInfoResponseList result = bean.findById(id);
        verifyAll();

        assertEquals(0, result.getResults().size());
    }

    @Test
    public void testFindById() throws Exception {
        // create the initial entry
        testPersistentCreate();

        PowerMock.resetAll();

        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getClient(EasyMock.eq(null), EasyMock.eq(null), EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN),
                        EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        replayAll();
        MapReduceInfoResponseList result = bean.findById(id);
        verifyAll();

        assertEquals(1, result.getResults().size());
        assertNull(result.getExceptions());
        MapReduceInfoResponse response = result.getResults().get(0);
        assertEquals(id, response.getId());
        assertEquals(hdfs, response.getHdfs());
        assertEquals(jt, response.getJobTracker());
        assertEquals(jobName, response.getJobName());
        assertEquals(workingDirectory, response.getWorkingDirectory());
        assertEquals(resultsDirectory, response.getResultsDirectory());
        assertEquals(runtimeParameters, response.getRuntimeParameters());
        assertEquals(1, response.getJobExecutions().size());
        assertEquals(mapReduceJobId, response.getJobExecutions().get(0).getMapReduceJobId());
        assertEquals(MapReduceState.STARTED.toString(), response.getJobExecutions().get(0).getState());
    }

    @Test
    public void testRemove() throws Exception {
        // create the initial entry
        testPersistentCreate();

        PowerMock.resetAll();

        // Get ready to call remove
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getClient(EasyMock.eq(null), EasyMock.eq(null), EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN),
                        EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        expect(connectionFactory.getTrackingMap(EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getClient(EasyMock.eq(null), EasyMock.eq(null), EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN),
                        EasyMock.eq(trackingMap))).andReturn(client);
        connectionFactory.returnClient(client);
        replayAll();

        bean.remove(id);
        verifyAll();

        boolean found = false;
        Scanner s = client.createScanner(TABLE_NAME, new Authorizations(auths));
        for (Entry<Key,Value> entry : s) {
            // If any K/V are found then set found to true
            found = true;
            break;
        }
        s = client.createScanner(INDEX_TABLE_NAME, new Authorizations(auths));
        for (@SuppressWarnings("unused")
        Entry<Key,Value> entry : s) {
            // If any K/V are found then set found to true
            found = true;
            break;
        }

        if (found) {
            dump();
            fail("Remove did not remove all K/V");
        }
    }

    private void dump() throws Exception {
        Scanner s = client.createScanner(TABLE_NAME, new Authorizations(auths));
        for (Entry<Key,Value> entry : s) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        s = client.createScanner(INDEX_TABLE_NAME, new Authorizations(auths));
        for (Entry<Key,Value> entry : s) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }

    }

}
