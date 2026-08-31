package datawave.query.tables.content;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

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
@ComponentScan(basePackages = {"datawave.configuration.spring", "datawave.query"})
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
        QueryTestTableHelper qtth = new QueryTestTableHelper(ContentQueryLogicFunctionalTest.class.toString(), log);
        connector = qtth.client;
        WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
    }

    @BeforeEach
    public void setup() {
        expectedResults.clear();
        extraParameters.clear();
    }

    @Test
    public void simpleTest() throws Exception {
        String queryString = "DOCUMENT:20130101_0/test/-cvy0gj.tlf59s.-duxzua";

        // not sure why the timestamp and delete flag are present
        addExpectedResult(
                        "20130101_0:test:-cvy0gj.tlf59s.-duxzua:CONTENT:You can get much farther with a kind word and a gun than you can with a kind word alone");
        addExpectedResult("20130101_0:test:-cvy0gj.tlf59s.-duxzua:CONTENT2:A lawyer and his briefcase can steal more than ten men with guns.");

        runTestQuery(queryString);
    }

    protected void addExpectedResult(String result) {
        if (StringUtils.isNotBlank(result)) {
            expectedResults.add(result);
        }
    }

    protected void runTestQuery(String queryString) throws Exception {
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(queryString);
        settings.setParameters(extraParameters);
        settings.setId(UUID.randomUUID());

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
}
