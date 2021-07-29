package datawave.query.planner;

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.marking.MarkingFunctions;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.composite.CompositeMetadata;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.language.parser.ParseException;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.BaseEdgeQueryTest;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.CompositeTestingIngest;
import datawave.query.util.TypeMetadata;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.QueryData;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

@RunWith(Arquillian.class)
public class CompositeExpansionTest {
    
    protected static Connector connector = null;
    
    private static final Logger log = Logger.getLogger(CompositeExpansionTest.class);
    
    protected Authorizations auths = new Authorizations("ALL");
    
    protected Set<Authorizations> authSet = Collections.singleton(auths);
    
    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    
    protected KryoDocumentDeserializer deserializer;
    
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    @BeforeClass
    public static void setUp() throws Exception {
        
        QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeExpansionTest.class.toString(), log);
        connector = qtth.connector;
        
        CompositeTestingIngest.writeItAll(connector, CompositeTestingIngest.WhatKindaRange.SHARD);
        Authorizations auths = new Authorizations("ALL");
        PrintUtility.printTable(connector, auths, TableName.SHARD);
        PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(connector, auths, BaseEdgeQueryTest.MODEL_TABLE_NAME);
    }
    
    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        System.setProperty("cdi.bean.context", "queryBeanRefContext.xml");
        
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class)
                        .deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }
    
    @Before
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        
        Logger.getLogger(DefaultQueryPlanner.class).setLevel(Level.TRACE);
        logic.setFullTableScanEnabled(true);
        logic.setExpandAllTerms(true);
        // we will have a higher depth threshold due to this ASTEvaluationOnly marker nodes.
        // we won't go as high as the default ( 2500 ) but we do need some additional room.
        logic.setMaxDepthThreshold(10);
        deserializer = new KryoDocumentDeserializer();
    }
    
    protected void runTestQuery(String expectedQuery, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
        log.debug("runTestQuery");
        log.trace("Creating QueryImpl");
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(querystr);
        settings.setParameters(extraParms);
        settings.setId(UUID.randomUUID());
        
        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);
        
        String plannedScript = logic.getQueryPlanner().getPlannedScript();
        Assert.assertTrue("Unexpected query:" + plannedScript + " not " + expectedQuery, plannedScript.equals(expectedQuery));
    }
    
    @Test
    public void baseExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testCompositeBaseExpansion");
        }
        
        runTestQuery("MAKE_COLOR == 'FORD" + CompositeIngest.DEFAULT_SEPARATOR + "RED'", "COLOR == 'RED' && MAKE == 'FORD'", format.parse("20091231"),
                        format.parse("20150101"), extraParameters);
        
    }
    
    @Test
    public void regexTrailingWildcardExpansionRhs() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("regexTrailingWildcardExpansionRhs");
        }
        
        runTestQuery("MAKE_COLOR == 'FORD􏿿RED' && ((_Eval_ = true) && (MAKE == 'FORD' && COLOR =~ 'RED.*'))", "COLOR =~ 'RED.*' && MAKE == 'FORD'",
                        format.parse("20091231"), format.parse("20150101"), extraParameters);
        
    }
    
    @Test
    public void regexTrailingWildcardExpansionLhs() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("regexTrailingWildcardExpansionLhs");
        }
        
        /**
         * We set expand all terms. If we do not do that, MAKE =~ 'FOR.*' will be delayed. The purpose of this test is to show that the LHS of the composite
         * expression will be expanded via ParallelIndexExpansion and, if allowed via configuration, placed into the composite term(s).
         */
        runTestQuery("MAKE_COLOR == 'FORD" + CompositeIngest.DEFAULT_SEPARATOR + "RED'", "COLOR == 'RED' && MAKE =~ 'FOR.*'", format.parse("20091231"),
                        format.parse("20150101"), extraParameters);
        
    }
    
    @Test
    public void regexTrailingWildcardExpansionBoth() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("regexTrailingWildcardExpansionBoth");
        }
        /**
         * Only allowed because full table scans are enabled.
         */
        runTestQuery("((_Eval_ = true) && (COLOR =~ 'R.*')) && ((_Eval_ = true) && (MAKE =~ 'F.*'))", "COLOR =~ 'R.*' && MAKE =~ 'F.*'",
                        format.parse("20091231"), format.parse("20150101"), extraParameters);
        
    }
    
}
