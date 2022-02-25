package datawave.query;

import com.google.common.collect.Sets;
import datawave.common.test.integration.IntegrationTest;
import datawave.helpers.PrintUtility;
import datawave.marking.MarkingFunctions;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.VisibilityWiseGuysIngest;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.util.TableName;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.cache.QueryMetricFactoryImpl;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.AbstractQueryLogicTransformer;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.runner.RunningQuery;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class LongRunningQueryTest {
    
    // variables common to all current tests
    private final AccumuloConnectionFactory.Priority connectionPriority = AccumuloConnectionFactory.Priority.NORMAL;
    private String methodAuths = "";
    private SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    private final ShardQueryLogic logic = new ShardQueryLogic();
    private static Authorizations auths = new Authorizations("ALL", "E", "I");
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private static MockAccumuloRecordWriter recordWriter;
    private DatawavePrincipal datawavePrincipal;
    private static final Logger log = Logger.getLogger(LongRunningQueryTest.class);
    private static Connector connector = null;
    
    @Before
    public void setup() throws Exception {
        
        System.setProperty(DnUtils.NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D,E,I");
        System.setProperty("file.encoding", "UTF-8");
        DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet(auths.toString().split(",")), null, null, -1L);
        datawavePrincipal = new DatawavePrincipal((Collections.singleton(user)));
        
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        
        logic.setIncludeGroupingContext(true);
        logic.setIncludeDataTypeAsField(true);
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
        logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        
        QueryTestTableHelper testTableHelper = new QueryTestTableHelper(LongRunningQueryTest.class.toString(), log);
        recordWriter = new MockAccumuloRecordWriter();
        testTableHelper.configureTables(recordWriter);
        connector = testTableHelper.connector;
        
        // Load data for the test
        VisibilityWiseGuysIngest.writeItAll(connector, VisibilityWiseGuysIngest.WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(connector, auths, TableName.SHARD);
        PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }
    
    /**
     * A groupBy query is one type of query that is allowed to be "long running", so that type of query is used in this test.
     *
     * A long running query will return a ResultsPage with zero results if it has not completed within the query execution page timeout. This test expects 2
     * pages, the first of which should have 0 results and be marked as PARTIAL. The second page should have 8 results and have a status of COMPLETE.
     *
     * @throws Exception
     */
    @Test
    @Category(IntegrationTest.class)
    public void testAllowLongRunningQueryWithShardQueryLogic() throws Exception {
        
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("group.fields", "AGE,$GENDER");
        extraParameters.put("group.fields.batch.size", "6");
        
        String queryStr = "UUID =~ '^[CS].*'";
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        QueryImpl query = new QueryImpl();
        query.setQuery(queryStr);
        query.setBeginDate(startDate);
        query.setEndDate(endDate);
        query.setQueryAuthorizations(auths.serialize());
        query.setColumnVisibility("A&E&I");
        query.setPagesize(Integer.MAX_VALUE);
        query.setParameters(extraParameters);
        query.setId(UUID.randomUUID());
        
        // this parameter is what makes the query long running. Failing to set this will let it default to 50 minutes
        // (and not the 1000 milliseconds that it is set to) which will return only 1 page of 8 results, thereby failing this test.
        logic.setQueryExecutionForPageTimeout(1000);
        logic.setLongRunningQuery(true);
        
        GenericQueryConfiguration config = logic.initialize(connector, query, Collections.singleton(auths));
        logic.setupQuery(config);
        
        RunningQuery runningQuery = new RunningQuery(connector, AccumuloConnectionFactory.Priority.NORMAL, logic, query, "", datawavePrincipal,
                        new QueryMetricFactoryImpl());
        
        TransformIterator transItr = runningQuery.getTransformIterator();
        AbstractQueryLogicTransformer et = (AbstractQueryLogicTransformer) transItr.getTransformer();
        List<ResultsPage> pages = new ArrayList<>();
        
        runningQuery.setActiveCall(true);
        ResultsPage page = runningQuery.next();
        pages.add(page);
        while (page.getStatus() != ResultsPage.Status.COMPLETE) {
            page = runningQuery.next();
            pages.add(page);
        }
        
        assertEquals(2, pages.size());
        assertEquals(0, pages.get(0).getResults().size());
        assertEquals(ResultsPage.Status.PARTIAL, pages.get(0).getStatus());
        assertEquals(8, pages.get(1).getResults().size());
        assertEquals(ResultsPage.Status.COMPLETE, pages.get(1).getStatus());
    }
}
