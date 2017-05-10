package datawave.webservice.query.util;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.gt;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import datawave.webservice.query.metric.QueryMetric;
import datawave.webservice.query.metric.BaseQueryMetric.PageMetric;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ZooKeeperInstance.class, Tables.class, ServerClient.class, TabletServerBatchReader.class, QueryMetricUtil.class})
public class QueryMetricsReporterTest {
    
    @Mock
    Connector connector;
    
    @Mock
    TabletServerBatchReaderIterator iterator;
    
    @Mock
    Entry<Key,Value> iteratorEntry;
    
    @Mock
    Option option;
    
    @Mock
    Options options;
    
    @Mock
    PageMetric pageMetric;
    
    @Mock
    QueryMetric queryMetric;
    
    @Mock
    Key tableKey;
    
    @Mock
    Value tableValue;
    
    @Mock
    Text tableColumnQualifier;
    
    @Mock
    ZooCache zooCache;
    
    @Mock
    ZooCacheFactory zooCacheFactory;
    
    @SuppressWarnings("unchecked")
    @Test
    public void testRun_HappyPath() throws Exception {
        // Set test arguments
        String instanceID = "testInstanceId";
        String tableName = "BigTable";
        Date beginDate = new Date(System.currentTimeMillis() - 5000);
        Date betweenDate1 = new Date(System.currentTimeMillis() - 3000);
        Date betweenDate2 = new Date(System.currentTimeMillis() - 2000);
        Date betweenDate3 = new Date(System.currentTimeMillis() - 4000);
        Date endDate = new Date(System.currentTimeMillis() - 1000);
        
        // Set expectations
        PowerMock.expectNew(ZooCacheFactory.class).andReturn(zooCacheFactory).anyTimes();
        expect(zooCacheFactory.getZooCache(isA(String.class), gt(-100))).andReturn(zooCache).anyTimes();
        expect(this.zooCache.get(Constants.ZROOT + Constants.ZINSTANCES + '/' + instanceID)).andReturn(instanceID.getBytes()).anyTimes();
        expect(this.zooCache.get(Constants.ZROOT + '/' + instanceID)).andReturn(instanceID.getBytes()).anyTimes();
        PowerMock.mockStatic(ServerClient.class);
        ServerClient.execute(isA(ClientContext.class), isA(ClientExec.class));
        expect(this.zooCache.getChildren(isA(String.class))).andReturn(Arrays.asList(Constants.ZROOT + '/' + instanceID + Constants.ZTABLES + '/' + tableName))
                        .anyTimes();
        expect(
                        this.zooCache.get(Constants.ZROOT + '/' + instanceID + Constants.ZTABLES + '/' + Constants.ZROOT + '/' + instanceID + Constants.ZTABLES
                                        + '/' + tableName + "/name")).andReturn(tableName.getBytes()).anyTimes();
        expect(
                        this.zooCache.get(Constants.ZROOT + '/' + instanceID + Constants.ZTABLES + '/' + Constants.ZROOT + '/' + instanceID + Constants.ZTABLES
                                        + '/' + tableName + "/namespace")).andReturn(Namespaces.DEFAULT_NAMESPACE_ID.toString().getBytes()).anyTimes();
        expect(
                        this.zooCache.get(Constants.ZROOT + '/' + instanceID + Constants.ZTABLES + '/' + Constants.ZROOT + '/' + instanceID + Constants.ZTABLES
                                        + '/' + tableName + "/state")).andReturn(TableState.ONLINE.toString().getBytes()).anyTimes();
        expect(this.zooCache.get(Constants.ZROOT + '/' + instanceID + RootTable.ZROOT_TABLET_LOCATION)).andReturn(null).anyTimes();
        PowerMock.expectNew(TabletServerBatchReaderIterator.class, isA(ClientContext.class), isA(String.class), isA(Authorizations.class),
                        isA(ArrayList.class), gt(-100), isA(ExecutorService.class), isA(ScannerOptions.class), gt(-2L)).andReturn(this.iterator);
        expect(this.iterator.hasNext()).andReturn(true).times(6); // 6 sets of metrics to report. First is a bad qualifier, so no value is retrieved.
        expect(this.iterator.next()).andReturn(this.iteratorEntry).times(6);
        Key badKey = new Key(new Text("ROW"), new Text("FAMILY"), new Text("BADQUALIFIER")); // Handle bad qualifier
        expect(this.iteratorEntry.getKey()).andReturn(badKey).times(2);
        Key goodKey = new Key(new Text("ROW"), new Text("FAMILY"), new Text("\0" + betweenDate1.getTime())); // Handle good qualifier
        expect(this.iteratorEntry.getKey()).andReturn(goodKey);
        goodKey = new Key(new Text("ROW"), new Text("FAMILY"), new Text("\0" + betweenDate2.getTime())); // Handle good qualifier
        expect(this.iteratorEntry.getKey()).andReturn(goodKey);
        goodKey = new Key(new Text("ROW"), new Text("FAMILY"), new Text("\0" + betweenDate3.getTime())); // Handle good qualifier. First times causes
                                                                                                         // ClassNotFoundException.
        expect(this.iteratorEntry.getKey()).andReturn(goodKey).times(2);
        goodKey = new Key(new Text("ROW"), new Text("FAMILY"), new Text("\0" + betweenDate3.getTime())); // Handle good qualifier. Second times causes
                                                                                                         // ClassNotFoundException.
        expect(this.iteratorEntry.getKey()).andReturn(goodKey).times(2);
        goodKey = new Key(new Text("ROW"), new Text("FAMILY"), new Text("\0" + betweenDate3.getTime())); // Handle good qualifier. Third time is a charm.
        expect(this.iteratorEntry.getKey()).andReturn(goodKey);
        expect(this.iterator.hasNext()).andReturn(false);
        expect(this.iteratorEntry.getValue()).andReturn(this.tableValue).times(5);
        PowerMock.mockStatic(QueryMetricUtil.class);
        expect(QueryMetricUtil.toMetric(this.tableValue)).andReturn(this.queryMetric); // Metrics for betweenDate1
        expect(this.queryMetric.getSetupTime()).andReturn(1000L);
        expect(this.queryMetric.getPageTimes()).andReturn(Arrays.asList(this.pageMetric));
        expect(this.pageMetric.getReturnTime()).andReturn(1000L);
        expect(QueryMetricUtil.toMetric(this.tableValue)).andReturn(this.queryMetric); // Metrics for betweenDate2
        expect(this.queryMetric.getSetupTime()).andReturn(500L);
        expect(this.queryMetric.getPageTimes()).andReturn(Arrays.asList(this.pageMetric));
        expect(this.pageMetric.getReturnTime()).andReturn(500L);
        expect(QueryMetricUtil.toMetric(this.tableValue)).andThrow(new ClassNotFoundException("Intentionally thrown test exception"));
        expect(QueryMetricUtil.toMetric(this.tableValue)).andThrow(new IOException("Intentionally thrown test exception"));
        expect(QueryMetricUtil.toMetric(this.tableValue)).andReturn(this.queryMetric); // Metrics for betweenDate3
        expect(this.queryMetric.getSetupTime()).andReturn(1000L);
        expect(this.queryMetric.getPageTimes()).andReturn(Arrays.asList(this.pageMetric));
        expect(this.pageMetric.getReturnTime()).andReturn(1000L);
        
        // Run the test
        PowerMock.replayAll();
        QueryMetricsReporter subject = new QueryMetricsReporter();
        String[] args = {"--i", // Instance ID
                instanceID, "--u", // User name
                "userName", "--p", // Password
                "pa$$w0rd", "--t", // Table name
                "BigTable", "-v", // Verbose
                "-a", // Use all query pages
                "--b", // Begin date
                new SimpleDateFormat("yyyyMMddHHmmss").format(beginDate), "--e", // End date
                new SimpleDateFormat("yyyyMMddHHmmss").format(endDate), "--zk", // Zookeeper
                "/invalid/zookeeper/path"};
        int result1 = subject.run(args);
        PowerMock.verifyAll();
        
        // Verify results
        assertTrue("Calling run should have returned a value of 0", result1 == 0);
    }
    
}
