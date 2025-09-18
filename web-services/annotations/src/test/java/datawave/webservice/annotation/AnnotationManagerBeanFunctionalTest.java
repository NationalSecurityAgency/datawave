package datawave.webservice.annotation;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.util.Set;
import java.util.TimeZone;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.ExcerptTest;
import datawave.query.QueryTestTableHelper;
import datawave.query.metrics.QueryMetricQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.security.authorization.UserOperations;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.AccumuloConnectionRequestBean;
import datawave.webservice.query.runner.QueryExecutor;

@RunWith(Arquillian.class)
public class AnnotationManagerBeanFunctionalTest {
    protected static AccumuloClient client = null;

    private static final Logger log = Logger.getLogger(AnnotationManagerBeanFunctionalTest.class);
    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Set.of(auths);

    // @Mock
    // private static EJBContext ctx;

    @Mock
    @Produces
    private static AccumuloConnectionFactory connectionFactory;

    @Mock
    @Produces
    private static QueryExecutor queryExecutor;

    @Mock
    @Produces
    private static QueryLogicFactory queryLogicFactory;

    @Mock
    @Produces
    private static ResponseObjectFactory responseObjectFactory;

    @Mock
    @Produces
    private static UserOperations userOperations;

    @Mock
    private static AccumuloConnectionRequestBean accumuloConnectionRequestBean;

    @Inject
    @SpringBean(name = "AnnotationManager")
    protected AnnotationManager annotationManager;

    @Deployment
    public static JavaArchive createDeployment() throws Exception {

        //@formatter:off
        return ShrinkWrap.create(JavaArchive.class)
                .addPackages(true,
                        "org.apache.deltaspike",
                        "io.astefanutti.metrics.cdi",
                        "datawave.query",
                        "org.jboss.logging",
                        "datawave.webservice.query.result.event",
                        "datawave.webservice.annotation")
                .addClass(AccumuloConnectionFactory.class)
                .addClass(QueryExecutor.class)
                .addClass(QueryLogicFactory.class)
                .addClass(ResponseObjectFactory.class)
                .addClass(UserOperations.class)
                .addClass(AccumuloConnectionRequestBean.class)
                .addClass(AnnotationManager.class)
                .addClass(AnnotationManagerBean.class)
                .deleteClass(DefaultEdgeEventQueryLogic.class)
                .deleteClass(RemoteEdgeDictionary.class)
                .deleteClass(QueryMetricQueryLogic.class)
                .addAsManifestResource(new StringAsset(
                                "<alternatives>" +
                                        "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" +
                                        "</alternatives>"),
                        "beans.xml");
        //@formatter:on
    }

    @BeforeClass
    public static void setUp() throws Exception {
        /*
         * mockEJBContext = EasyMock.createMock(EJBContext.class); mockAccumuloConnectionFactory = EasyMock.createMock(AccumuloConnectionFactory.class);
         * mockQueryExecutor = EasyMock.createMock(QueryExecutor.class); mockQueryLogicFactory = EasyMock.createMock(QueryLogicFactory.class);
         * mockResponseObjectFactory = EasyMock.createMock(ResponseObjectFactory.class); mockUserOperations = EasyMock.createMock(UserOperations.class);
         * mockAccumuloConnectionRequestBean = EasyMock.createMock(AccumuloConnectionRequestBean.class); ?
         *
         */
        QueryTestTableHelper queryTestTableHelper = new QueryTestTableHelper(ExcerptTest.DocumentRangeTest.class.toString(), log);
        client = queryTestTableHelper.client;

        TableOperations tops = client.tableOperations();
        tops.create("annotations");

        Logger.getLogger(PrintUtility.class).setLevel(Level.DEBUG);

        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        Authorizations auths = new Authorizations("ALL");
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @Test
    public void simpleTest() throws Exception {
        assertEquals(2, 1 + 1);
    }

    @Before
    public void setup() throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        log.setLevel(Level.TRACE);
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    /*
     * @Configuration static class Config {
     *
     *
     * @Bean public EJBContext context() { return mockEJBContext; }
     *
     * @Bean public AccumuloConnectionFactory accumuloConnectionFactory() { return mockAccumuloConnectionFactory; }
     *
     * @Bean public QueryExecutor queryExecutor() { return mockQueryExecutor; }
     *
     * @Bean public QueryLogicFactory queryLogicFactory() { return mockQueryLogicFactory; }
     *
     * @Bean public ResponseObjectFactory responseObjectFactory() { return mockResponseObjectFactory; }
     *
     * @Bean public UserOperations userOperationsBean() { return mockUserOperations; }
     *
     * @Bean public AccumuloConnectionRequestBean accumuloConnectionRequestBean() { return mockAccumuloConnectionRequestBean; }
     *
     * }
     */
}
