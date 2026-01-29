package datawave.query.tables.keyword;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.helpers.PrintUtility;
import datawave.microservice.query.QueryImpl;
import datawave.query.ExcerptTest;
import datawave.query.QueryTestTableHelper;
import datawave.query.tables.ResponseQueryDriver;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.tables.keyword.transform.TagCloudPartitionTransformer;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.keyword.DefaultTagCloud;
import datawave.webservice.result.keyword.DefaultTagCloudResponse;

@RunWith(Arquillian.class)
public class KeywordUUIDQueryFunctionalTest {
    protected static AccumuloClient connector = null;

    private static final Logger log = Logger.getLogger(KeywordUUIDQueryFunctionalTest.class);
    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Set.of(auths);

    @Inject
    @SpringBean(name = "KeywordUUIDQuery")
    protected KeywordChainedUUIDQueryLogic logic;

    private final TagCloudPartitionTransformer tagCloudPartitionTransformer = TagCloudPartitionTransformer.getInstance();

    private final Map<String,String> extraParameters = new HashMap<>();
    private final List<DefaultTagCloud> expectedResults = new ArrayList<>();

    private ResponseQueryDriver<Map.Entry<Key,Value>> queryDriver;

    @Deployment
    public static JavaArchive createDeployment() throws Exception {

        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    @BeforeClass
    public static void setUp() throws Exception {
        QueryTestTableHelper qtth = new QueryTestTableHelper(ExcerptTest.DocumentRangeTest.class.toString(), log);
        connector = qtth.client;

        Logger.getLogger(PrintUtility.class).setLevel(Level.DEBUG);

        WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        Authorizations auths = new Authorizations("ALL");
        PrintUtility.printTable(connector, auths, TableName.SHARD);
        PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @Before
    public void setup() throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        log.setLevel(Level.TRACE);

        queryDriver = new ResponseQueryDriver<>(logic);
    }

    @Test
    public void injectionTest() {
        assertNotNull(logic);
    }

    @Test
    public void emptyQueryTest() throws Exception {
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery("UUID:ABC");
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, 2000);
        settings.setBeginDate(calendar.getTime());
        settings.setEndDate(new Date());
        settings.setParameters(extraParameters);
        settings.setId(UUID.randomUUID());

        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);

        BaseQueryResponse response = queryDriver.drive(config);
        assertFalse(response.isPartialResults());
        assertFalse(response.getHasResults());
        assertTrue(response instanceof DefaultTagCloudResponse);
        DefaultTagCloudResponse tagCloudResponse = (DefaultTagCloudResponse) response;
        assertNull(tagCloudResponse.getTagClouds());
    }

    // TODO-crwill9 add more tests here
}
