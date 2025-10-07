package datawave.query.function;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.google.common.collect.Sets;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.data.type.DateType;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.LimitFieldsTestingIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * Tests the {@code limit.fields} feature to ensure that hit terms are always included and that associated fields at the same grouping context are included
 * along with the field that hit on the query
 */
public abstract class HitsAreAlwaysIncludedTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @RunWith(Arquillian.class)
    public static class ShardRange extends HitsAreAlwaysIncludedTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            client = HitsAreAlwaysIncludedTest.initClient(ShardRange.class.toString(), LimitFieldsTestingIngest.WhatKindaRange.SHARD);
        }

        @Before
        public void setup() throws ParseException {
            super.setup();
            logic.setCollapseUids(true);
        }

        @Override
        protected AccumuloClient getClient() {
            return client;
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends HitsAreAlwaysIncludedTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {
            client = HitsAreAlwaysIncludedTest.initClient(DocumentRange.class.toString(), LimitFieldsTestingIngest.WhatKindaRange.DOCUMENT);
        }

        @Before
        public void setup() throws ParseException {
            super.setup();
            logic.setCollapseUids(false);
        }

        @Override
        protected AccumuloClient getClient() {
            return client;
        }
    }

    private static final Logger log = Logger.getLogger(HitsAreAlwaysIncludedTest.class);
    private static final Authorizations auths = new Authorizations("ALL");
    private static final Set<Authorizations> authSet = Collections.singleton(auths);
    // Under certain conditions a Date-normalized field value is returned without normalization.
    private static final Set<String> dateFields = Set.of("FOO_1_BAR_1.FOO.0", "FOO_1_BAR_1");

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    protected KryoDocumentDeserializer deserializer;

    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private final Map<String,String> queryParameters = new HashMap<>();
    private final Set<String> expectedHits = new HashSet<>();
    private final Set<String> expectedEntries = new HashSet<>();

    private String query;
    private Date startDate;
    private Date endDate;

    @Deployment
    public static JavaArchive createDeployment() throws Exception {

        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event", "datawave.core.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    private static AccumuloClient initClient(String instanceName, LimitFieldsTestingIngest.WhatKindaRange range) throws Exception {
        QueryTestTableHelper qtth = new QueryTestTableHelper(instanceName, log);
        AccumuloClient client = qtth.client;

        LimitFieldsTestingIngest.writeItAll(client, range);
        Authorizations auths = new Authorizations("ALL");
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);

        return client;
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    @Before
    public void setup() throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        logic.setFullTableScanEnabled(true);
        deserializer = new KryoDocumentDeserializer();
        this.startDate = format.parse("20091231");
        this.endDate = format.parse("20150101");
    }

    @After
    public void tearDown() throws Exception {
        this.queryParameters.clear();
        this.query = null;
        this.startDate = null;
        this.endDate = null;
        this.expectedHits.clear();
        this.expectedEntries.clear();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void givenQueryParameter(String key, String value) {
        this.queryParameters.put(key, value);
    }

    private void expectEntry(String entry) {
        this.expectedEntries.add(entry);
    }

    private void expectHit(String hit) {
        this.expectedHits.add(hit);
    }

    protected abstract AccumuloClient getClient();

    private void runTestQuery() throws Exception {
        setupQuery();
        Document document = assertAndGetSingularDocument();
        assertHits(document);
        assertEntries(document);
    }

    private void setupQuery() throws Exception {
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(this.startDate);
        settings.setEndDate(this.endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(this.query);
        settings.setParameters(this.queryParameters);
        settings.setId(UUID.randomUUID());
        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());

        GenericQueryConfiguration config = logic.initialize(getClient(), settings, authSet);
        logic.setupQuery(config);
    }

    private Document assertAndGetSingularDocument() {
        Iterator<Entry<Key,Value>> iterator = logic.iterator();
        Assert.assertTrue("No documents were returned", iterator.hasNext());
        Entry<Key,Value> entry = iterator.next();
        Document document = deserializer.apply(entry).getValue();
        if (log.isTraceEnabled()) {
            log.trace(entry.getKey() + " => " + document);
        }
        int count = 1;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        Assert.assertEquals("Expected exactly one document", 1, count);
        return document;
    }

    private void assertHits(Document document) {
        log.debug("Expected hits: " + document);

        if (expectedHits.isEmpty()) {
            return;
        }

        Attribute<?> hitAttribute = document.get(JexlEvaluation.HIT_TERM_FIELD);
        Assert.assertNotNull("Did not find hit term field " + JexlEvaluation.HIT_TERM_FIELD, hitAttribute);

        Set<String> hits = getContents(hitAttribute);

        Set<String> missingHits = Sets.difference(expectedHits, hits);
        Assert.assertTrue("Expected hits missing: " + missingHits, missingHits.isEmpty());

        Set<String> unexpectedHits = Sets.difference(hits, expectedHits);
        Assert.assertTrue("Unexpected hits found: " + unexpectedHits, unexpectedHits.isEmpty());
    }

    private static Set<String> getContents(Attribute<?> hitAttribute) {
        Set<String> hits = Sets.newHashSet();
        if (hitAttribute instanceof Attributes) {
            Attributes hitAttributes = (Attributes) hitAttribute;
            for (Attribute<?> attribute : hitAttributes.getAttributes()) {
                if (attribute instanceof Content) {
                    Content content = (Content) attribute;
                    hits.add(content.getContent());
                }
            }
        } else if (hitAttribute instanceof Content) {
            Content content = (Content) hitAttribute;
            hits.add(content.getContent());
        }
        return hits;
    }

    private void assertEntries(Document document) {
        log.debug("Expected entries: " + expectedEntries);
        Map<String,Attribute<? extends Comparable<?>>> dictionary = document.getDictionary();
        log.debug("Dictionary: " + dictionary);

        Set<String> entries = Sets.newHashSet();
        for (Entry<String,Attribute<? extends Comparable<?>>> entry : dictionary.entrySet()) {
            String key = entry.getKey();

            // Ignore the hit term field and record id.
            if (key.equals(JexlEvaluation.HIT_TERM_FIELD) || key.equals(Document.DOCKEY_FIELD_NAME)) {
                continue;
            }

            Attribute<? extends Comparable<?>> attribute = entry.getValue();
            if (attribute instanceof Attributes) {
                Attributes attributes = (Attributes) attribute;
                for (Attribute<?> attr : attributes.getAttributes()) {
                    if (!key.endsWith(LimitFields.ORIGINAL_COUNT_SUFFIX)) {
                        entries.add(key + ":" + attr);
                    }
                }
            } else {
                if (dateFields.contains(key)) {
                    DateType dateType = new DateType(attribute.getData().toString());
                    entries.add(key + ":" + dateType.getNormalizedValue());
                } else if (!key.endsWith(LimitFields.ORIGINAL_COUNT_SUFFIX)) {
                    entries.add(key + ":" + attribute);
                }
            }
        }

        Set<String> missingEntries = Sets.difference(expectedEntries, entries);
        Assert.assertTrue("Expected entries missing: " + missingEntries, missingEntries.isEmpty());

        Set<String> unexpectedEntries = Sets.difference(entries, expectedEntries);
        Assert.assertTrue("Unexpected entries found: " + unexpectedEntries, unexpectedEntries.isEmpty());
    }

    @Test
    public void testHitForIndexedQueryTerm() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>'");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");
        expectEntry("FOO_1.FOO.1.3:good");
        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_4.FOO.4.1:purr");

        expectHit("FOO_3_BAR.FOO.3:defg<cat>");

        runTestQuery();
    }

    @Test
    public void testHitForIndexedQueryTermWithOptionsInQueryFunction() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' and f:options('include.grouping.context', 'true', "
                        + "'hit.list', 'true', 'limit.fields', 'FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0')");

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");
        expectEntry("FOO_1.FOO.1.3:good");
        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_4.FOO.4.1:purr");

        expectHit("FOO_3_BAR.FOO.3:defg<cat>");

        runTestQuery();

        // runTestQuery(expectedHits, goodResults);
    }

    @Test
    public void testHitForIndexedQueryOnUnrealmed() throws Exception {
        givenQuery("FOO_3 == 'defg'");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");
        expectEntry("FOO_1.FOO.1.3:good");
        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_4.FOO.4.1:purr");

        expectHit("FOO_3.FOO.3.3:defg");

        runTestQuery();
    }

    @Test
    public void testHitForIndexedQueryAndAnyfieldLimit() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>'");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "_ANYFIELD_=2,BAR_1=0,BAR_2=0,BAR_3=0");

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");
        expectEntry("FOO_1.FOO.1.3:good");

        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_4.FOO.4.0:purr");

        expectHit("FOO_3_BAR.FOO.3:defg<cat>");

        runTestQuery();
    }

    @Test
    public void testHitForIndexedAndUnindexedQueryAndAnyfieldLimit() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' and FOO_1 == 'good'");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "_ANYFIELD_=2,BAR_1=0,BAR_2=0,BAR_3=0");

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");

        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_1.FOO.1.3:good");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_4.FOO.4.0:purr");

        expectHit("FOO_3_BAR.FOO.3:defg<cat>");
        expectHit("FOO_1.FOO.1.3:good");

        runTestQuery();
    }

    @Test
    public void testHitWithoutGroupingContext() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>'");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        // the hit
        expectEntry("FOO_3_BAR:defg<cat>");

        // the additional values included per the limits
        expectEntry("FOO_1:yawn");
        expectEntry("FOO_1:good");
        expectEntry("FOO_1_BAR:yawn<cat>");
        expectEntry("FOO_1_BAR:good<cat>");
        expectEntry("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3:abcd");
        expectEntry("FOO_3:bcde");
        expectEntry("FOO_3_BAR:abcd<cat>");
        expectEntry("FOO_4:purr");
        expectEntry("FOO_4:yes");

        expectHit("FOO_3_BAR:defg<cat>");

        runTestQuery();

    }

    @Test
    public void testHitWithRange() throws Exception {
        givenQuery("((_Bounded_ = true) && (FOO_1_BAR_1 >= '2021-03-01 00:00:00' && FOO_1_BAR_1 <= '2021-04-01 00:00:00'))");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        expectHit("FOO_1_BAR_1:Wed Mar 24 16:00:00 GMT 2021");

        // the hit
        expectEntry("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");

        // the additional values included per the limits
        expectEntry("FOO_1:yawn");
        expectEntry("FOO_1:good");
        expectEntry("FOO_1_BAR:yawn<cat>");
        expectEntry("FOO_1_BAR:good<cat>");
        expectEntry("FOO_3:abcd");
        expectEntry("FOO_3:bcde");
        expectEntry("FOO_3_BAR:abcd<cat>");
        expectEntry("FOO_3_BAR:bcde<cat>");
        expectEntry("FOO_4:purr");
        expectEntry("FOO_4:yes");

        runTestQuery();
    }

    @Test
    public void testHitWithDate() throws Exception {
        givenQuery("FOO_1_BAR_1 == '2021-03-24T16:00:00.000Z'");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        expectHit("FOO_1_BAR_1:Wed Mar 24 16:00:00 GMT 2021");

        // the hit
        expectEntry("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");

        // the additional values included per the limits
        expectEntry("FOO_1:yawn");
        expectEntry("FOO_1:good");
        expectEntry("FOO_1_BAR:yawn<cat>");
        expectEntry("FOO_1_BAR:good<cat>");
        expectEntry("FOO_3:abcd");
        expectEntry("FOO_3:bcde");
        expectEntry("FOO_3_BAR:abcd<cat>");
        expectEntry("FOO_3_BAR:bcde<cat>");
        expectEntry("FOO_4:purr");
        expectEntry("FOO_4:yes");

        runTestQuery();
    }

    @Test
    public void testHitWithExceededOrThreshold() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=1,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        // the hits
        expectEntry("FOO_3_BAR:defg<cat>");
        expectEntry("FOO_3_BAR:abcd<cat>");

        // the additional values included per the limits
        expectEntry("FOO_1:yawn");
        expectEntry("FOO_1:good");
        expectEntry("FOO_1_BAR:yawn<cat>");
        expectEntry("FOO_1_BAR:good<cat>");
        expectEntry("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");
        expectEntry("FOO_3:abcd");
        expectEntry("FOO_3:bcde");
        expectEntry("FOO_4:purr");
        expectEntry("FOO_4:yes");

        expectHit("FOO_3_BAR:defg<cat>");
        expectHit("FOO_3_BAR:abcd<cat>");

        runTestQuery();
    }

    @Test
    public void testHitsOnly() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        expectEntry("FOO_3_BAR:defg<cat>");
        expectEntry("FOO_3_BAR:abcd<cat>");

        expectHit("FOO_3_BAR:defg<cat>");
        expectHit("FOO_3_BAR:abcd<cat>");

        runTestQuery();

    }

    @Test
    public void testGroupedHitsOnly() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // the hit and associated fields in the same group
        expectEntry("FOO_1_BAR.FOO.3:good<cat>");
        expectEntry("FOO_3_BAR.FOO.3:defg<cat>");
        expectEntry("FOO_3.FOO.3.3:defg");
        expectEntry("FOO_4.FOO.4.3:yes");

        // the additional values included per the limits
        expectEntry("FOO_1.FOO.1.3:good");
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");

        expectHit("FOO_3_BAR.FOO.3:defg<cat>");
        expectHit("FOO_3_BAR.FOO.0:abcd<cat>");

        runTestQuery();
    }

    @Test
    public void testGroupedHitsWithMatchingField() throws Exception {
        givenQuery("FOO_3_BAR == 'abcd<cat>'");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        givenQueryParameter("matching.field.sets", "FOO_4");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // the hit and associated fields in the same group
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");

        // the additional values included per the matching field sets
        expectEntry("FOO_1.FOO.1.1:yawn");
        expectEntry("FOO_4.FOO.4.1:purr");
        expectEntry("FOO_3.FOO.3.1:bcde");
        expectEntry("FOO_3_BAR.FOO.1:bcde<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1.FOO.1.2:yawn");
        expectEntry("FOO_4.FOO.4.2:purr");
        expectEntry("FOO_3.FOO.3.2:cdef");
        expectEntry("FOO_3_BAR.FOO.2:cdef<cat>");
        expectEntry("FOO_1_BAR.FOO.2:yawn<cat>");

        expectHit("FOO_3_BAR.FOO.0:abcd<cat>");

        runTestQuery();
    }

    @Test
    public void testGroupedHitsWithMatchingFields() throws Exception {
        givenQuery("FOO_3_BAR == 'abcd<cat>'");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        givenQueryParameter("matching.field.sets", "FOO_4=BAR_1");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // the hit and associated fields in the same group
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");

        // the additional values included per the matching field sets
        expectEntry("FOO_1.FOO.1.1:yawn");
        expectEntry("FOO_4.FOO.4.1:purr");
        expectEntry("FOO_3.FOO.3.1:bcde");
        expectEntry("FOO_3_BAR.FOO.1:bcde<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1.FOO.1.2:yawn");
        expectEntry("FOO_4.FOO.4.2:purr");
        expectEntry("FOO_3.FOO.3.2:cdef");
        expectEntry("FOO_3_BAR.FOO.2:cdef<cat>");
        expectEntry("FOO_1_BAR.FOO.2:yawn<cat>");
        expectEntry("BAR_1.BAR.1.3:purr");
        expectEntry("BAR_2.BAR.2.3:tiger");
        expectEntry("BAR_3.BAR.3.3:spotted");

        expectHit("FOO_3_BAR.FOO.0:abcd<cat>");

        runTestQuery();
    }

    @Test
    public void testGroupedHitsWithMoreMatchingFields() throws Exception {
        givenQuery("FOO_3_BAR == 'abcd<cat>'");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        givenQueryParameter("matching.field.sets", "FOO_4=BAR_1=FOO_1");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // the hit and associated fields in the same group
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");

        // the additional values included per the matching field sets
        expectEntry("FOO_1.FOO.1.1:yawn");
        expectEntry("FOO_4.FOO.4.1:purr");
        expectEntry("FOO_3.FOO.3.1:bcde");
        expectEntry("FOO_3_BAR.FOO.1:bcde<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1.FOO.1.2:yawn");
        expectEntry("FOO_4.FOO.4.2:purr");
        expectEntry("FOO_3.FOO.3.2:cdef");
        expectEntry("FOO_3_BAR.FOO.2:cdef<cat>");
        expectEntry("FOO_1_BAR.FOO.2:yawn<cat>");
        expectEntry("BAR_1.BAR.1.2:yawn");
        expectEntry("BAR_2.BAR.2.2:siberian");
        expectEntry("BAR_3.BAR.3.2:pink");
        expectEntry("BAR_1.BAR.1.3:purr");
        expectEntry("BAR_2.BAR.2.3:tiger");
        expectEntry("BAR_3.BAR.3.3:spotted");

        expectHit("FOO_3_BAR.FOO.0:abcd<cat>");

        runTestQuery();
    }

    @Test
    public void testGroupedHitsWithMatchingFieldSets() throws Exception {
        givenQuery("FOO_3_BAR == 'abcd<cat>'");

        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenQueryParameter(QueryParameters.HIT_LIST, "true");
        givenQueryParameter(QueryParameters.LIMIT_FIELDS, "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        givenQueryParameter("matching.field.sets", "FOO_4=BAR_1,FOO_1=BAR_1");

        logic.setMaxOrExpansionThreshold(1);

        ivaratorConfig();

        // the hit and associated fields in the same group
        expectEntry("FOO_3_BAR.FOO.0:abcd<cat>");
        expectEntry("FOO_1.FOO.1.0:yawn");
        expectEntry("FOO_4.FOO.4.0:purr");
        expectEntry("FOO_3.FOO.3.0:abcd");
        expectEntry("FOO_1_BAR.FOO.0:yawn<cat>");
        expectEntry("FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");

        // the additional values included per the matching field sets
        expectEntry("FOO_1.FOO.1.1:yawn");
        expectEntry("FOO_4.FOO.4.1:purr");
        expectEntry("FOO_3.FOO.3.1:bcde");
        expectEntry("FOO_3_BAR.FOO.1:bcde<cat>");
        expectEntry("FOO_1_BAR.FOO.1:yawn<cat>");
        expectEntry("FOO_1.FOO.1.2:yawn");
        expectEntry("FOO_4.FOO.4.2:purr");
        expectEntry("FOO_3.FOO.3.2:cdef");
        expectEntry("FOO_3_BAR.FOO.2:cdef<cat>");
        expectEntry("FOO_1_BAR.FOO.2:yawn<cat>");
        expectEntry("BAR_1.BAR.1.2:yawn");
        expectEntry("BAR_2.BAR.2.2:siberian");
        expectEntry("BAR_3.BAR.3.2:pink");
        expectEntry("BAR_1.BAR.1.3:purr");
        expectEntry("BAR_2.BAR.2.3:tiger");
        expectEntry("BAR_3.BAR.3.3:spotted");

        expectHit("FOO_3_BAR.FOO.0:abcd<cat>");

        runTestQuery();
    }

    protected void ivaratorConfig() throws IOException {
        final URL hdfsConfig = this.getClass().getResource("/testhadoop.config");
        Assert.assertNotNull("Failed to fetch testhadoop.config URL", hdfsConfig);
        this.logic.setHdfsSiteConfigURLs(hdfsConfig.toExternalForm());

        final List<String> dirs = new ArrayList<>();
        Path ivCache = Paths.get(tempFolder.newFolder().toURI());
        dirs.add(ivCache.toUri().toString());
        String uriList = String.join(",", dirs);
        log.debug("hdfs dirs(" + uriList + ")");
        this.logic.setIvaratorCacheDirConfigs(dirs.stream().map(IvaratorCacheDirConfig::new).collect(Collectors.toList()));
    }

}
