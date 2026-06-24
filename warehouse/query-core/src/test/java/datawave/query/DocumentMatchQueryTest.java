package datawave.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.base.Preconditions;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.function.DocumentMatchContext;
import datawave.query.function.DocumentMatchResults;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.functions.DocumentFunctions;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.WiseGuysIngest;

/**
 * MiniAccumulo-backed integration tests for {@code document:match(...)}.
 * <p>
 * These tests exercise the full query path, including query parsing, planner wiring, shard-table document materialization, evaluation-phase document matching,
 * and publication of the {@code DOCUMENT_MATCHES} attribute on returned documents.
 */
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
public class DocumentMatchQueryTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(DocumentMatchQueryTest.class);
    private static final Authorizations auths = new Authorizations("ALL");
    private static final String PASSWORD = "password";

    @TempDir
    public static Path folder;

    protected static MiniAccumuloCluster mac;
    protected static AccumuloClient client;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private final Map<String,Map<String,Map<String,List<Integer>>>> expectedDocumentMatches = new HashMap<>();
    private final Map<String,Map<String,ColumnVisibility>> expectedDocumentMatchVisibilities = new HashMap<>();
    private Boolean expectedDocumentMatchContextRequired;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        System.setProperty("type.metadata.dir", folder.toFile().getAbsolutePath());

        MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.toFile(), PASSWORD);
        cfg.setNumTservers(1);
        mac = new MiniAccumuloCluster(cfg);
        mac.start();

        client = mac.createAccumuloClient("root", new PasswordToken(PASSWORD));
        client.securityOperations().changeUserAuthorizations("root", auths);
        new QueryTestTableHelper(client, log);
        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);
    }

    @BeforeEach
    public void beforeEach() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        setClientForTest(client);

        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        Preconditions.checkNotNull(hadoopConfig);
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(new IvaratorCacheDirConfig(folder.toUri().toString())));
        logic.setMaxFieldIndexRangeSplit(1);
        logic.setCollapseUids(false);
        logic.setFullTableScanEnabled(false);
        logic.setDocumentMatchMaxDecodedSize(DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE);

        givenParameter(QueryParameters.HIT_LIST, "true");
        logic.setHitList(true);
        givenDate("20091231", "20150101");
    }

    @AfterEach
    public void afterEach() {
        super.afterEach();
        expectedDocumentMatches.clear();
        expectedDocumentMatchVisibilities.clear();
        expectedDocumentMatchContextRequired = null;
    }

    @AfterAll
    public static void afterAll() throws Exception {
        if (mac != null) {
            mac.stop();
        }
        TypeRegistry.reset();
    }

    @Override
    protected void extraConfigurations() {
        // no-op
    }

    /**
     * Verifies that returned documents expose the expected {@code DOCUMENT_MATCHES} payload when the current test configured one.
     */
    @Override
    protected void extraAssertions() {
        if (expectedDocumentMatchContextRequired != null) {
            if (expectedDocumentMatchContextRequired) {
                assertTrue(logic.getConfig().isDocumentMatchContextRequired(), "planned query did not require document-match context lookup");
            } else {
                assertFalse(logic.getConfig().isDocumentMatchContextRequired(), "planned query unexpectedly required document-match context lookup");
            }
        }

        for (Document result : results) {
            Attribute<?> uuid = result.get("UUID");
            assertNotNull(uuid, "result did not contain UUID");

            String uuidValue = getUUID(uuid);
            Map<String,Map<String,List<Integer>>> expected = expectedDocumentMatches.get(uuidValue);
            if (expected != null) {
                Attribute<?> matches = result.get(DocumentFunctions.DOCUMENT_MATCHES);
                assertNotNull(matches, "result did not contain DOCUMENT_MATCHES");
                assertEquals(expected, getDocumentMatchesByView(matches));
            }

            Map<String,ColumnVisibility> expectedVisibilities = expectedDocumentMatchVisibilities.get(uuidValue);
            if (expectedVisibilities != null) {
                Attribute<?> matches = result.get(DocumentFunctions.DOCUMENT_MATCHES);
                assertNotNull(matches, "result did not contain DOCUMENT_MATCHES");
                assertEquals(expectedVisibilities, getDocumentMatchVisibilities(matches));
            }
        }
    }

    /**
     * Verifies that JEXL {@code document:match(STRING)} evaluates across all views and returns the expected offsets.
     */
    @Test
    public void testDocumentMatchJexlAllViews() throws Exception {
        givenQuery("UUID == 'CAPONE' && document:match('can')");
        expectPlan("UUID == 'capone' && document:match(documentMatchContext, 'can')");
        expectedDocumentMatchContextRequired = true;
        expectResultCount(1);
        expectUUIDs(java.util.Set.of("CAPONE"));
        expectedDocumentMatches.put("CAPONE", Map.of("CONTENT", Map.of("can", List.of(4, 61)), "CONTENT2", Map.of("can", List.of(27))));
        planAndExecuteQuery();
    }

    /**
     * Verifies that JEXL {@code document:match(VIEWNAME, STRING)} restricts evaluation to the named view.
     */
    @Test
    public void testDocumentMatchJexlSpecificView() throws Exception {
        givenQuery("UUID == 'CAPONE' && document:match('CONTENT2', 'lawyer')");
        expectPlan("UUID == 'capone' && document:match('CONTENT2', documentMatchContext, 'lawyer')");
        expectedDocumentMatchContextRequired = true;
        expectResultCount(1);
        expectUUIDs(java.util.Set.of("CAPONE"));
        expectedDocumentMatches.put("CAPONE", Map.of("CONTENT2", Map.of("lawyer", List.of(2))));
        planAndExecuteQuery();
    }

    /**
     * Verifies that multiple JEXL {@code document:match(...)} calls contribute one {@code DOCUMENT_MATCHES} value per matched {@code d}-column.
     */
    @Test
    public void testDocumentMatchJexlAddsPerEntryMatchesAcrossCalls() throws Exception {
        givenQuery("UUID == 'CAPONE' && document:match('CONTENT', 'can') && document:match('CONTENT2', 'lawyer')");
        expectPlan("UUID == 'capone' && document:match('CONTENT', documentMatchContext, 'can') && document:match('CONTENT2', documentMatchContext, 'lawyer')");
        expectedDocumentMatchContextRequired = true;
        expectResultCount(1);
        expectUUIDs(java.util.Set.of("CAPONE"));
        expectedDocumentMatches.put("CAPONE", Map.of("CONTENT", Map.of("can", List.of(4, 61)), "CONTENT2", Map.of("lawyer", List.of(2))));
        planAndExecuteQuery();
    }

    /**
     * Verifies that end-to-end {@code DOCUMENT_MATCHES} values preserve the visibilities carried by their source {@code d}-column entries.
     */
    @Test
    public void testDocumentMatchJexlPreservesPerEntryVisibilities() throws Exception {
        givenQuery("UUID == 'CAPONE' && document:match('can')");
        expectPlan("UUID == 'capone' && document:match(documentMatchContext, 'can')");
        expectedDocumentMatchContextRequired = true;
        expectResultCount(1);
        expectUUIDs(java.util.Set.of("CAPONE"));
        expectedDocumentMatches.put("CAPONE", Map.of("CONTENT", Map.of("can", List.of(4, 61)), "CONTENT2", Map.of("can", List.of(27))));
        expectedDocumentMatchVisibilities.put("CAPONE", Map.of("CONTENT", new ColumnVisibility("ALL"), "CONTENT2", new ColumnVisibility("ALL")));
        planAndExecuteQuery();
    }

    /**
     * Verifies that a wildcard view match combined with a second targeted call accumulates per-entry matches without cross-entry merging.
     */
    @Test
    public void testDocumentMatchJexlWildcardThenSpecificViewAccumulatesPerEntry() throws Exception {
        givenQuery("UUID == 'CAPONE' && document:match('CONTENT*', 'can') && document:match('CONTENT2', 'lawyer')");
        expectPlan("UUID == 'capone' && document:match('CONTENT*', documentMatchContext, 'can') && document:match('CONTENT2', documentMatchContext, 'lawyer')");
        expectedDocumentMatchContextRequired = true;
        expectResultCount(1);
        expectUUIDs(java.util.Set.of("CAPONE"));
        expectedDocumentMatches.put("CAPONE", Map.of("CONTENT", Map.of("can", List.of(4, 61)), "CONTENT2", Map.of("can", List.of(27), "lawyer", List.of(2))));
        expectedDocumentMatchVisibilities.put("CAPONE", Map.of("CONTENT", new ColumnVisibility("ALL"), "CONTENT2", new ColumnVisibility("ALL")));
        planAndExecuteQuery();
    }

    /**
     * Verifies Lucene {@code #DOCUMENT_MATCH(...)} translation and wildcard view-prefix behavior in the full query path.
     */
    @Test
    public void testDocumentMatchLuceneWildcardView() throws Exception {
        givenParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQuery("UUID:CAPONE AND #DOCUMENT_MATCH(CONTENT*,can)");
        expectPlan("UUID == 'capone' && document:match('CONTENT*', documentMatchContext, 'can')");
        expectedDocumentMatchContextRequired = true;
        expectResultCount(1);
        expectUUIDs(java.util.Set.of("CAPONE"));
        expectedDocumentMatches.put("CAPONE", Map.of("CONTENT", Map.of("can", List.of(4, 61)), "CONTENT2", Map.of("can", List.of(27))));
        planAndExecuteQuery();
    }

    private Map<String,Map<String,List<Integer>>> getDocumentMatchesByView(Attribute<?> attribute) {
        Map<String,Map<String,List<Integer>>> values = new HashMap<>();
        if (attribute instanceof Attributes) {
            for (Attribute<? extends Comparable<?>> child : ((Attributes) attribute).getAttributes()) {
                addDocumentMatch(values, ((Content) child).getContent());
            }
        } else {
            addDocumentMatch(values, ((Content) attribute).getContent());
        }
        return values;
    }

    private Map<String,ColumnVisibility> getDocumentMatchVisibilities(Attribute<?> attribute) {
        Map<String,ColumnVisibility> visibilities = new HashMap<>();
        if (attribute instanceof Attributes) {
            for (Attribute<? extends Comparable<?>> child : ((Attributes) attribute).getAttributes()) {
                Content content = (Content) child;
                visibilities.put(getDocumentMatchView(content.getContent()), content.getColumnVisibility());
            }
        } else {
            Content content = (Content) attribute;
            visibilities.put(getDocumentMatchView(content.getContent()), content.getColumnVisibility());
        }
        return visibilities;
    }

    private void addDocumentMatch(Map<String,Map<String,List<Integer>>> values, String json) {
        JsonObject payload = JsonParser.parseString(json).getAsJsonObject();
        String view = payload.get(DocumentMatchResults.VIEW_FIELD).getAsString();
        JsonObject matches = payload.getAsJsonObject(DocumentMatchResults.MATCHES_FIELD);
        Map<String,List<Integer>> offsetsBySearch = new HashMap<>();
        for (Map.Entry<String,JsonElement> matchEntry : matches.entrySet()) {
            List<Integer> offsets = new java.util.ArrayList<>();
            for (JsonElement offset : matchEntry.getValue().getAsJsonArray()) {
                offsets.add(offset.getAsInt());
            }
            offsetsBySearch.put(matchEntry.getKey(), offsets);
        }
        values.put(view, offsetsBySearch);
    }

    private String getDocumentMatchView(String json) {
        return JsonParser.parseString(json).getAsJsonObject().get(DocumentMatchResults.VIEW_FIELD).getAsString();
    }

    /**
     * Verifies that a non-matching document-match term filters the document out of the result set.
     */
    @Test
    public void testDocumentMatchNoMatchFiltersDocument() throws Exception {
        givenQuery("UUID == 'CAPONE' && document:match('missing')");
        expectPlan("UUID == 'capone' && document:match(documentMatchContext, 'missing')");
        expectedDocumentMatchContextRequired = true;
        expectResultCount(0);
        planAndExecuteQuery();
    }

    /**
     * Verifies that document-match is case-sensitive in the full query path.
     */
    @Test
    public void testDocumentMatchIsCaseSensitive() throws Exception {
        givenQuery("UUID == 'CAPONE' && document:match('Can')");
        expectPlan("UUID == 'capone' && document:match(documentMatchContext, 'Can')");
        expectedDocumentMatchContextRequired = true;
        expectResultCount(0);
        planAndExecuteQuery();
    }

    /**
     * Verifies that decoded payloads larger than the configured limit are skipped as non-matching during end-to-end query execution.
     */
    @Test
    public void testDocumentMatchOversizedDecodedPayloadIsSkipped() throws Exception {
        logic.setDocumentMatchMaxDecodedSize(8);
        givenQuery("UUID == 'CAPONE' && document:match('can')");
        expectPlan("UUID == 'capone' && document:match(documentMatchContext, 'can')");
        expectedDocumentMatchContextRequired = true;
        expectResultCount(0);
        planAndExecuteQuery();
    }

    /**
     * Verifies that queries without {@code document:match(...)} do not request document-match context lookup in the integration harness.
     */
    @Test
    public void testQueryWithoutDocumentMatchDoesNotRequireContext() throws Exception {
        givenQuery("UUID == 'CAPONE'");
        expectPlan("UUID == 'capone'");
        expectedDocumentMatchContextRequired = false;
        expectResultCount(1);
        expectUUIDs(java.util.Set.of("CAPONE"));
        planAndExecuteQuery();
        assertEquals(1, results.size());
        Document result = results.iterator().next();
        assertNull(result.get(DocumentFunctions.DOCUMENT_MATCHES), "query without document:match unexpectedly emitted DOCUMENT_MATCHES");
    }
}
