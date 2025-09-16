package datawave.webservice.annotation;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.util.Set;
import java.util.TimeZone;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
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

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.ExcerptTest;
import datawave.query.QueryTestTableHelper;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

@RunWith(Arquillian.class)
public class AnnotationManagerBeanFunctionalTest {
    protected static AccumuloClient client = null;

    private static final Logger log = Logger.getLogger(AnnotationManagerBeanFunctionalTest.class);
    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Set.of(auths);

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
                        "datawave.webservice.query.result.event")
                .deleteClass(DefaultEdgeEventQueryLogic.class)
                .deleteClass(RemoteEdgeDictionary.class)
                .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                .addAsManifestResource(new StringAsset(
                                "<alternatives>" +
                                        "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" +
                                        "</alternatives>"),
                        "beans.xml");
        //@formatter:on
    }

    @BeforeClass
    public static void setUp() throws Exception {
        QueryTestTableHelper queryTestTableHelper = new QueryTestTableHelper(ExcerptTest.DocumentRangeTest.class.toString(), log);
        client = queryTestTableHelper.client;

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
}
