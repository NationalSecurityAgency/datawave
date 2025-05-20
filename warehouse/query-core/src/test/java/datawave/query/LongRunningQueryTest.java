package datawave.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.helpers.PrintUtility;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.VisibilityWiseGuysIngest;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.util.TableName;
import datawave.webservice.query.cache.RunningQueryTimingImpl;
import datawave.webservice.query.runner.RunningQuery;

/**
 * Controlling the execution order via the @FixMethodOrder. Otherwise, it seems that our accumulo instance "remembers" and executes the query faster than the
 * test can sleep and results in intermittent failures. Therefore, ensure that the test names alphabetically follow the order shortest to longest running
 * keeping in mind that queries that have been seen before will execute quicker.
 */
public class LongRunningQueryTest {

    // variables common to all current tests
    private SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    private static Authorizations auths = new Authorizations("ALL", "E", "I");
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private static MockAccumuloRecordWriter recordWriter;
    private DatawavePrincipal datawavePrincipal;
    private static final Logger log = Logger.getLogger(LongRunningQueryTest.class);
    private static AccumuloClient client = null;
    private ShardQueryLogic logic;

    @Before
    public void setup() throws Exception {
        DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet(auths.toString().split(",")), null, null, -1L);
        datawavePrincipal = new DatawavePrincipal((Collections.singleton(user)));

        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        QueryTestTableHelper testTableHelper = new QueryTestTableHelper(LongRunningQueryTest.class.toString(), log);
        recordWriter = new MockAccumuloRecordWriter();
        testTableHelper.configureTables(recordWriter);
        client = testTableHelper.client;

        // Load data for the test
        VisibilityWiseGuysIngest.writeItAll(client, VisibilityWiseGuysIngest.WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);

        logic = new ShardQueryLogic();
        logic.setIncludeGroupingContext(true);
        logic.setIncludeDataTypeAsField(true);
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
        logic.setDateIndexHelperFactory(new DateIndexHelperFactory());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        logic.setMaxEvaluationPipelines(1);
        logic.setFullTableScanEnabled(true);
        logic.setCacheModel(false);
        logic.setMaxPipelineCachedResults(0);
        logic.setIvaratorCacheBufferSize(0);
    }

    /**
     * A groupBy query is one type of query that is allowed to be "long running", so that type of query is used in this test.
     *
     * A long running query will return a ResultsPage with zero results if it has not completed within the query execution page timeout. This test expects at
     * least 2 pages (the exact number will depend on cpu speed). All but the lsat page should have 0 results and be marked as PARTIAL. The last page should
     * have 8 results and have a status of COMPLETE.
     *
     * @throws Exception
     *             if there is an issue
     */
    @Test
    public void testLongRunningGroupByQuery() throws Exception {
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
        // (and not the 200 milliseconds that it is set to) which will return only 1 page of 8 results, thereby failing this test.
        // the smaller this timeout, the more pages of results that will be returned.
        logic.setQueryExecutionForPageTimeout(1);
        GenericQueryConfiguration config = logic.initialize(client, query, Collections.singleton(auths));
        logic.setupQuery(config);

        QueryExpirationProperties conf = new QueryExpirationProperties();
        conf.setMaxLongRunningTimeoutRetries(1000);
        RunningQueryTimingImpl timing = new RunningQueryTimingImpl(conf, 1);
        RunningQuery runningQuery = new RunningQuery(null, client, AccumuloConnectionFactory.Priority.NORMAL, logic, query, "", datawavePrincipal, timing, null,
                        new QueryMetricFactoryImpl());
        List<ResultsPage> pages = new ArrayList<>();

        ResultsPage page = runningQuery.next();
        pages.add(page);

        while (page.getStatus() != ResultsPage.Status.COMPLETE) {
            page = runningQuery.next();
            pages.add(page);
        }

        // There should be at least 2 pages, more depending on cpu speed.
        assertTrue(pages.size() > 1);
        for (int i = 0; i < pages.size() - 1; ++i) {
            // check every page but the last one for 0 results and PARTIAL status
            assertEquals(0, pages.get(i).getResults().size());
            assertEquals(ResultsPage.Status.PARTIAL, pages.get(i).getStatus());
        }
        // check the last page for COMPLETE status and that the total number of results is 8
        assertEquals(8, pages.get(pages.size() - 1).getResults().size());
        assertEquals(ResultsPage.Status.COMPLETE, pages.get(pages.size() - 1).getStatus());
    }

    /**
     * A groupBy query is one type of query that is allowed to be "long running", so that type of query is used in this test.
     *
     * A long running query will return a ResultsPage with zero results if it has not completed within the query execution page timeout. This test expects at
     * least 2 pages (the exact number will depend on cpu speed). All but the lsat page should have 0 results and be marked as PARTIAL. The last page should
     * have 8 results and have a status of COMPLETE.
     *
     * @throws Exception
     *             if there is an issue
     */
    @Test
    public void testLongRunningUniqueQuery() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("unique.fields", "GROUP");

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
        // (and not the 200 milliseconds that it is set to) which will return only 1 page of 8 results, thereby failing this test.
        // the smaller this timeout, the more pages of results that will be returned.
        logic.setQueryExecutionForPageTimeout(1);
        ShardQueryConfiguration config = (ShardQueryConfiguration) logic.initialize(client, query, Collections.singleton(auths));
        logic.setupQuery(config);

        QueryExpirationProperties conf = new QueryExpirationProperties();
        conf.setMaxLongRunningTimeoutRetries(1000);
        RunningQueryTimingImpl timing = new RunningQueryTimingImpl(conf, 1);
        RunningQuery runningQuery = new RunningQuery(null, client, AccumuloConnectionFactory.Priority.NORMAL, logic, query, "", datawavePrincipal, timing, null,
                        new QueryMetricFactoryImpl());
        List<ResultsPage> pages = new ArrayList<>();
        ResultsPage page = runningQuery.next();
        Thread.sleep(15);
        while (page.getStatus() != ResultsPage.Status.NONE) {
            pages.add(page);
            page = runningQuery.next();
        }

        // There should be at least 2 pages, more depending on cpu speed.
        assertTrue(pages.size() > 1);
        boolean foundIntermediate = false;
        for (int i = 0; i < pages.size(); ++i) {
            // at least one page should be an "intermediate" result (0 results, PARTIAL status
            if (pages.get(i).getResults().size() == 0 && pages.get(i).getStatus().equals(ResultsPage.Status.PARTIAL)) {
                foundIntermediate = true;
            }
        }
        assertTrue("Did not find intermediate results", foundIntermediate);
    }

    /***
     * Tests that the code path that allows long running queries does not interfere or create a never ending query if a query legitimately doesn't have results.
     *
     * @throws Exception
     *             if there is an issue
     */
    @Test
    public void testLongRunningQueryWithNoResults() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("group.fields", "AGE,$GENDER");
        extraParameters.put("group.fields.batch.size", "6");

        // There should be no results for this query
        String queryStr = "UUID =~ '^[NAN].*' AND UUID =~ '^[BAN].*' AND UUID =~ '^[MAN].*' AND UUID =~ '^[PAN].*'";
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
        // (and not the 1 millisecond that it is set to)
        logic.setQueryExecutionForPageTimeout(1);
        GenericQueryConfiguration config = logic.initialize(client, query, Collections.singleton(auths));
        logic.setupQuery(config);

        RunningQuery runningQuery = new RunningQuery(null, client, AccumuloConnectionFactory.Priority.NORMAL, logic, query, "", datawavePrincipal, null, null,
                        new QueryMetricFactoryImpl());
        List<ResultsPage> pages = new ArrayList<>();

        ResultsPage page = runningQuery.next();
        Thread.sleep(15);
        pages.add(page);

        while (page.getStatus() != ResultsPage.Status.NONE) {
            page = runningQuery.next();
            pages.add(page);
        }

        // There should be 1 end result
        assertTrue(pages.size() == 1);

        // check the last page for COMPLETE status and that the total number of results is 0
        assertEquals(0, pages.get(pages.size() - 1).getResults().size());
        assertEquals(ResultsPage.Status.NONE, pages.get(pages.size() - 1).getStatus());
    }

    /**
     * Tests that having results returned over multiple pages (by setting the page size to be less than the number of results that will be returned) behaves
     * even with long running queries exceeding their execution timeout for a page. Should get at least 1 page of 0 results and a PARTIAL status, but could be
     * more depending on cpu speed due hitting the execution timeout. The last two pages should have 4 results each, but the last page should have a status of
     * COMPLETE, and the next to last page should have a status of PARTIAL.
     *
     * @throws Exception
     *             if there is an issue
     */
    @Test
    public void testLongRunningQueryWithSmallPageSize() throws Exception {
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

        // We expect 8 results, so this allows us to test getting those results over 2 pages
        query.setPagesize(4);
        query.setParameters(extraParameters);
        query.setId(UUID.randomUUID());

        // this parameter is what makes the query long running. Failing to set this will let it default to 50 minutes
        // (and not the 500 milliseconds that it is set to) which will return only 1 page of 8 results, thereby failing this test.
        // the smaller this timeout, the more pages of results that will be returned.
        logic.setQueryExecutionForPageTimeout(5);
        GenericQueryConfiguration config = logic.initialize(client, query, Collections.singleton(auths));
        logic.setupQuery(config);

        RunningQuery runningQuery = new RunningQuery(null, client, AccumuloConnectionFactory.Priority.NORMAL, logic, query, "", datawavePrincipal, null, null,
                        new QueryMetricFactoryImpl());
        List<ResultsPage> pages = new ArrayList<>();

        ResultsPage page = runningQuery.next();
        pages.add(page);

        // guarantee the need for at least a second page (make the wait slightly longer than the page timeout is set to)
        Thread.sleep(10);

        while (page.getStatus() != ResultsPage.Status.NONE) {
            page = runningQuery.next();
            pages.add(page);
        }

        // There should be at least 4 pages, more depending on cpu speed. AT LEAST 1 of PARTIAL status results indicating it hit the intermediate result,
        // 2 of COMPLETE results with a results size of 4 which is what we set the page size to, and 1 with NONE status indicating it's done. Order is not
        // guaranteed, except the last page will definitely be the NONE page.
        int pagesWithFourResultsFoundCount = 0;
        assertTrue(pages.size() > 3);
        for (int i = 0; i < pages.size() - 1; i++) {
            ResultsPage p = pages.get(i);
            if (p.getStatus() == ResultsPage.Status.PARTIAL) {
                assertEquals(0, p.getResults().size());
            } else if (p.getStatus() == ResultsPage.Status.COMPLETE) {
                assertEquals(4, p.getResults().size());
                pagesWithFourResultsFoundCount++;
            }
        }

        // we should have gotten two pages with 4 results among all the PARTIAL, zero result pages
        assertEquals(2, pagesWithFourResultsFoundCount);
        // check the last page for NONE status
        assertEquals(0, pages.get(pages.size() - 1).getResults().size());
        assertEquals(ResultsPage.Status.NONE, pages.get(pages.size() - 1).getStatus());
    }
}
