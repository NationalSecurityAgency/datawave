package datawave.query.tables.keyword;

import static datawave.query.tables.keyword.KeywordQueryLogic.TAG_CLOUD_VERSION;
import static datawave.query.tables.keyword.KeywordUUIDChainStrategy.CATEGORY_PARAMETER;
import static datawave.query.tables.keyword.TagCloudTestUtil.CAPONE_UUID;
import static datawave.query.tables.keyword.TagCloudTestUtil.CORLEONE_UUID;
import static datawave.query.tables.keyword.TagCloudTestUtil.SOPRANO_UUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
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
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryTestTableHelper;
import datawave.query.tables.ResponseQueryDriver;
import datawave.query.util.WiseGuysIngest;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.keyword.DefaultTagCloud;
import datawave.webservice.result.keyword.DefaultTagCloudResponse;
import datawave.webservice.result.keyword.TagCloudBase;

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
public class KeywordUUIDQueryFunctionalTest {

    private static final Logger log = Logger.getLogger(KeywordUUIDQueryFunctionalTest.class);

    protected static AccumuloClient connector;

    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Set.of(auths);

    @Autowired
    @Qualifier("KeywordUUIDQuery")
    protected KeywordChainedUUIDQueryLogic logic;

    private String query;
    private Map<String,String> extraParameters = new HashMap<>();
    private List<DefaultTagCloud> expectedResults = new ArrayList<>();

    private ResponseQueryDriver<Map.Entry<Key,Value>> queryDriver;
    private final TagCloudTestUtil tagCloudTestUtil = new TagCloudTestUtil();

    @BeforeAll
    public static void setUp() throws Exception {
        QueryTestTableHelper qtth = new QueryTestTableHelper(KeywordUUIDQueryFunctionalTest.class.toString(), log);
        connector = qtth.client;
        WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
    }

    @BeforeEach
    public void setup() {
        queryDriver = new ResponseQueryDriver<>(logic);
        expectedResults = new ArrayList<>();
        extraParameters = new HashMap<>();
        query = "";
    }

    @Test
    public void injectionTest() {
        assertNotNull(logic);
    }

    @Test
    public void emptyQueryTest() throws Exception {
        withQuery("UUID:ABC");

        test();
    }

    @Test
    public void emptyQueryNoHitsExtractorTest() throws Exception {
        withExtraParameter(CATEGORY_PARAMETER, "name");

        withQuery("UUID:ABC");

        test();
    }

    @Test
    public void extractorTest() throws Exception {
        // two clouds for gendered-age, subtype gender and subtype age. One cloud for name
        withExtraParameter(CATEGORY_PARAMETER, "gendered-age,name");
        withExtraParameter(TAG_CLOUD_VERSION, "2");

        // three tag clouds expected, one for each category/subtype
        // @formatter:off
        withExpectedResult(tagCloudTestUtil.getExpectedCloud("2", Map.of("type", "gendered-age", "subType", "age"),
                List.of(tagCloudTestUtil.createTagCloudEntry("16", 1.0, 1, List.of(SOPRANO_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("18", 1.0, 1, List.of(SOPRANO_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("20", 1.0, 1, List.of(CAPONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("30", 1.0, 1, List.of(CAPONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("34", 1.0, 1, List.of(CAPONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("40", 1.0, 1, List.of(CAPONE_UUID)))
        ));
        withExpectedResult(tagCloudTestUtil.getExpectedCloud("2", Map.of("type", "gendered-age", "subType", "gender"),
                List.of(tagCloudTestUtil.createTagCloudEntry("MALE", 1.0, 2, List.of(SOPRANO_UUID, CAPONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("FEMALE", 1.0, 1, List.of(SOPRANO_UUID)))
        ));
        withExpectedResult(tagCloudTestUtil.getExpectedCloud("2", Map.of("type", "name"),
                List.of(tagCloudTestUtil.createTagCloudEntry("MICHAEL", 1.0, 2, List.of(CORLEONE_UUID, CAPONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("ALPHONSE", 1.0, 1, List.of(CAPONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("ANTHONY", 1.0, 1, List.of(SOPRANO_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("CONSTANZIA", 1.0, 1, List.of(CORLEONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("FRANK", 1.0, 1, List.of(CAPONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("FREDO", 1.0, 1, List.of(CORLEONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("LUCA", 1.0, 1, List.of(CORLEONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("MEADOW", 1.0, 1, List.of(SOPRANO_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("RALPH", 1.0, 1, List.of(CAPONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("SANTINO", 1.0, 1, List.of(CORLEONE_UUID)),
                        tagCloudTestUtil.createTagCloudEntry("VINCENT", 1.0, 1, List.of(CORLEONE_UUID)))
        ));
        // @formatter:on

        withQuery("UUID:CAPONE OR UUID:CORLEONE OR UUID: SOPRANO");

        test();
    }

    private void test() throws Exception {
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(query);
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
        if (expectedResults.isEmpty()) {
            assertNull(tagCloudResponse.getTagClouds());
            return;
        }

        assertNotNull(tagCloudResponse.getTagClouds());
        assertEquals(expectedResults.size(), tagCloudResponse.getTagClouds().size());
        for (TagCloudBase result : tagCloudResponse.getTagClouds()) {
            DefaultTagCloud tagCloud = (DefaultTagCloud) result;
            assertTrue(expectedResults.contains(tagCloud), "tagCloud " + result.getMetadata() + " not expected.");
        }
    }

    private void withQuery(String query) {
        this.query = query;
    }

    private void withExpectedResult(DefaultTagCloud tagCloud) {
        expectedResults.add(tagCloud);
    }

    private void withExtraParameter(String key, String value) {
        extraParameters.put(key, value);
    }
}
