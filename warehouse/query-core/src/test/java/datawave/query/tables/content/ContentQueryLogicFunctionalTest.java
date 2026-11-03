package datawave.query.tables.content;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumulo;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryTestTableHelper;
import datawave.query.util.WiseGuysIngest;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.util.TypedValue;

@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class ContentQueryLogicFunctionalTest {

    private static final Logger log = Logger.getLogger(ContentQueryLogicFunctionalTest.class);
    private static final String CAPONE_DOCUMENT = "DOCUMENT:20130101_0/test/" + WiseGuysIngest.caponeUID;
    private static final String CORLEONE_DOCUMENT = "DOCUMENT:20130101_0/test/" + WiseGuysIngest.corleoneUID;

    protected static AccumuloClient connector;

    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Set.of(auths);

    @Autowired
    @Qualifier("ContentQuery")
    protected ContentQueryLogic logic;

    private final Map<String,String> extraParameters = new HashMap<>();
    private final Set<String> expectedResults = new HashSet<>();

    @BeforeAll
    public static void setUp() throws Exception {
        InMemoryAccumuloClient client = new InMemoryAccumuloClient("root", InMemoryAccumulo.getInstance(ContentQueryLogicFunctionalTest.class.toString()));
        QueryTestTableHelper qtth = new QueryTestTableHelper(client, log);
        connector = qtth.client;
        WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
    }

    @AfterAll
    public static void tearDownClient() {
        connector.close();
    }

    @BeforeEach
    public void setup() {
        expectedResults.clear();
        extraParameters.clear();
    }

    @AfterEach
    public void tearDown() {
        logic.close();
    }

    @Test
    public void simpleTest() throws Exception {
        addExpectedCaponeContent();
        runTestQuery(CAPONE_DOCUMENT);
    }

    @Test
    public void springInjectsMaxIds() {
        assertEquals(10, logic.getMaxIds());
    }

    @Test
    public void acceptsBatchAtLimit() throws Exception {
        addExpectedKnownContent();

        runTestQuery(recordIds(logic.getMaxIds()));
    }

    @Test
    public void acceptsDefaultMaxIds() throws Exception {
        int defaultMaxIds = new ContentQueryLogic().getMaxIds();
        assertEquals(100, defaultMaxIds);
        logic.setMaxIds(defaultMaxIds);
        addExpectedKnownContent();

        runTestQuery(recordIds(defaultMaxIds));
    }

    @Test
    public void rejectsBatchOverLimit() {
        assertLimitExceeded(recordIds(logic.getMaxIds() + 1));
    }

    @Test
    public void rejectsMoreThanDefaultBatchSize() {
        int defaultMaxIds = new ContentQueryLogic().getMaxIds();
        assertEquals(100, defaultMaxIds);
        logic.setMaxIds(defaultMaxIds);

        assertLimitExceeded(recordIds(defaultMaxIds + 1));
    }

    @Test
    public void countsRepeatedRecordIds() {
        String repeatedIds = String.join(" ", Collections.nCopies(logic.getMaxIds() + 1, CAPONE_DOCUMENT));
        assertLimitExceeded(repeatedIds);
    }

    @Test
    public void queryParameterCannotRaiseConfiguredLimit() {
        extraParameters.put(ContentQueryLogic.MAX_IDS, "11");
        assertLimitExceeded(recordIds(logic.getMaxIds() + 1));
    }

    @Test
    public void queryParameterCannotLowerConfiguredLimit() throws Exception {
        extraParameters.put(ContentQueryLogic.MAX_IDS, "1");
        addExpectedKnownContent();

        runTestQuery(recordIds(logic.getMaxIds()));
    }

    @Test
    public void rejectsInvalidConfiguredMaxIds() {
        assertThrows(IllegalArgumentException.class, () -> logic.setMaxIds(0));
        assertEquals(10, logic.getMaxIds());
    }

    private void addExpectedCaponeContent() {
        addExpectedResult("20130101_0:test:" + WiseGuysIngest.caponeUID
                        + ":CONTENT:You can get much farther with a kind word and a gun than you can with a kind word alone");
        addExpectedResult("20130101_0:test:" + WiseGuysIngest.caponeUID + ":CONTENT2:A lawyer and his briefcase can steal more than ten men with guns.");
    }

    private void addExpectedKnownContent() {
        addExpectedCaponeContent();
        addExpectedResult("20130101_0:test:" + WiseGuysIngest.corleoneUID + ":CONTENT:Im gonna make him an offer he cant refuse");
    }

    private String recordIds(int count) {
        return IntStream.range(0, count).mapToObj(index -> {
            if (index == 0) {
                return CAPONE_DOCUMENT;
            }
            if (index == 1) {
                return CORLEONE_DOCUMENT;
            }
            return "DOCUMENT:20130101_0/test/content-limit-" + index;
        }).collect(Collectors.joining(" "));
    }

    private void assertLimitExceeded(String queryString) {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> logic.initialize(connector, createQuery(queryString), authSet));
        assertEquals("Query has requested greater than " + ContentQueryLogic.MAX_IDS + " of " + logic.getMaxIds() + " record IDs", exception.getMessage());
    }

    protected void addExpectedResult(String result) {
        if (StringUtils.isNotBlank(result)) {
            expectedResults.add(result);
        }
    }

    protected void runTestQuery(String queryString) throws Exception {
        QueryImpl settings = createQuery(queryString);

        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());

        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);

        QueryLogicTransformer<Map.Entry<Key,Value>,EventBase> transformer = logic.getTransformer(config.getQuery());
        Set<String> unexpectedFields = new HashSet<>();

        for (Map.Entry<Key,Value> entry : logic) {
            EventBase event = transformer.transform(entry);
            List<FieldBase> fields = event.getFields();
            Metadata md = event.getMetadata();

            for (FieldBase field : fields) {
                String name = field.getName();
                String toFind = md.getRow() + ":" + md.getDataType() + ":" + md.getInternalId() + ":" + name;
                TypedValue tv = field.getTypedValue();

                if (tv.getType().equals(TypedValue.XSD_BASE64BINARY)) {
                    String content = new String((byte[]) tv.getValue());
                    toFind += ":" + content;
                }

                boolean found = expectedResults.remove(toFind);
                if (found)
                    log.debug("removed " + toFind);
                else {
                    unexpectedFields.add(toFind);
                }

            }
        }

        assertTrue(unexpectedFields.isEmpty(), "unexpected fields returned: " + unexpectedFields);
        assertTrue(expectedResults.isEmpty(), expectedResults + " was not empty");
    }

    private QueryImpl createQuery(String queryString) {
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(queryString);
        settings.setParameters(extraParameters);
        settings.setId(UUID.randomUUID());
        return settings;
    }
}
