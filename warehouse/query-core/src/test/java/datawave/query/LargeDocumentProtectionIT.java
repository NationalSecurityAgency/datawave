package datawave.query;

import static datawave.query.predicate.EventDataQueryEntryLimitFilter.INCOMPLETE_DOCUMENT_FIELD;
import static datawave.table.constants.TableName.SHARD_INDEX;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
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

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcType;
import datawave.query.attributes.Document;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractIngest;
import datawave.query.util.AbstractQueryTest;

/**
 * Integration test for {@code doc.agg.max.entries}, the entry-count limit on document aggregation.
 * <p>
 * The big document's event entries sort as APPLE, FILLER_00..FILLER_29, ZEBRA, ZULU. With a limit of 10 the aggregation window holds APPLE plus the first nine
 * FILLER entries; ZEBRA (indexed) and ZULU (event-only) fall outside the window. The tests pin the short-circuit behavior inside and outside that window, with
 * seeking event aggregation both off and on.
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
public class LargeDocumentProtectionIT extends AbstractQueryTest {

    private static final Authorizations auths = new Authorizations("ALL");

    private static final int LIMIT = 10;

    protected static AccumuloClient client = null;

    private int maxEntries = -1;
    private boolean seekingEventAggregation = false;

    private boolean expectMarker = false;
    private final List<String> expectedFields = new ArrayList<>();
    private final List<String> unexpectedFields = new ArrayList<>();

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @Override
    protected List<String> getIndexTableNames() {
        return List.of(SHARD_INDEX);
    }

    @Override
    protected void extraConfigurations() {
        logic.setDocAggregationMaxEntries(maxEntries);
        logic.setSeekingEventAggregation(seekingEventAggregation);
    }

    @Override
    protected void extraAssertions() {
        for (Document result : results) {
            if (expectMarker) {
                assertNotNull(result.get(INCOMPLETE_DOCUMENT_FIELD), "expected the incomplete-document marker");
            } else {
                assertNull(result.get(INCOMPLETE_DOCUMENT_FIELD), "did not expect the incomplete-document marker");
            }
            for (String field : expectedFields) {
                assertNotNull(result.get(field), "expected field " + field);
            }
            for (String field : unexpectedFields) {
                assertNull(result.get(field), "did not expect field " + field);
            }
        }
    }

    @BeforeEach
    public void beforeEach() {
        setClientForTest(client);
    }

    @AfterEach
    public void cleanup() {
        maxEntries = -1;
        seekingEventAggregation = false;
        expectMarker = false;
        expectedFields.clear();
        unexpectedFields.clear();
        logic.setDocAggregationMaxEntries(-1);
        logic.setSeekingEventAggregation(false);
    }

    @BeforeAll
    public static void setupIngest() throws AccumuloSecurityException, AccumuloException {
        InMemoryInstance instance = new InMemoryInstance(LargeDocumentProtectionIT.class.getName());
        client = new InMemoryAccumuloClient("", instance);

        AbstractIngest ingest = new AbstractIngest(client, auths);
        ingest.registerField("APPLE", new LcType());
        ingest.registerColumns("APPLE", List.of("i", "e"));
        ingest.registerField("ZEBRA", new LcType());
        ingest.registerColumns("ZEBRA", List.of("i", "e"));
        ingest.registerField("ZULU", new LcType());
        ingest.registerColumns("ZULU", List.of("e"));
        for (int i = 0; i < 30; i++) {
            String field = fillerField(i);
            ingest.registerField(field, new LcType());
            ingest.registerColumns(field, List.of("e"));
        }

        // the big document: 33 event entries, far past the limit of 10
        ingest.writeFV(1, "APPLE", "granny");
        for (int i = 0; i < 30; i++) {
            ingest.writeFV(1, fillerField(i), "filler");
        }
        ingest.writeFV(1, "ZEBRA", "stripes");
        ingest.writeFV(1, "ZULU", "target");

        // the small document: 6 event entries, under the limit
        ingest.writeFV(2, "APPLE", "fuji");
        for (int i = 0; i < 3; i++) {
            ingest.writeFV(2, fillerField(i), "filler");
        }
        ingest.writeFV(2, "ZEBRA", "spots");
        ingest.writeFV(2, "ZULU", "target");
    }

    private static String fillerField(int i) {
        return String.format("FILLER_%02d", i);
    }

    @Test
    public void testNoLimitReturnsFullDocument() throws Exception {
        givenDate("20260708");
        givenQuery("APPLE == 'granny'");
        expectPlan("APPLE == 'granny'");
        expectResultCount(1);
        expectedFields.addAll(List.of("APPLE", "ZEBRA", "ZULU", fillerField(29)));
        planAndExecuteQuery();
    }

    @Test
    public void testLimitTruncatesAndMarksDocument() throws Exception {
        maxEntries = LIMIT;
        givenDate("20260708");
        givenQuery("APPLE == 'granny'");
        expectPlan("APPLE == 'granny'");
        expectResultCount(1);
        expectMarker = true;
        expectedFields.add("APPLE");
        unexpectedFields.addAll(List.of("ZEBRA", "ZULU", fillerField(29)));
        planAndExecuteQuery();
    }

    @Test
    public void testLimitTruncatesAndMarksDocumentWithSeekingAggregation() throws Exception {
        maxEntries = LIMIT;
        seekingEventAggregation = true;
        givenDate("20260708");
        givenQuery("APPLE == 'granny'");
        expectPlan("APPLE == 'granny'");
        expectResultCount(1);
        expectMarker = true;
        expectedFields.add("APPLE");
        unexpectedFields.addAll(List.of("ZEBRA", "ZULU", fillerField(29)));
        planAndExecuteQuery();
    }

    @Test
    public void testDocumentUnderLimitIsUntouched() throws Exception {
        maxEntries = LIMIT;
        givenDate("20260708");
        givenQuery("APPLE == 'fuji'");
        expectPlan("APPLE == 'fuji'");
        expectResultCount(1);
        expectedFields.addAll(List.of("APPLE", "ZEBRA", "ZULU"));
        planAndExecuteQuery();
    }

    /**
     * Evaluation runs against the truncated document, so a query term whose event entry falls outside the aggregation window fails to match even when the term
     * is indexed. Operators enabling the limit accept this trade: a document past the entry budget may be dropped instead of returned truncated when the query
     * terms land past the cutoff.
     */
    @Test
    public void testIndexedTermOutsideWindowDropsDocument() throws Exception {
        maxEntries = LIMIT;
        givenDate("20260708");
        givenQuery("ZEBRA == 'stripes'");
        expectPlan("ZEBRA == 'stripes'");
        expectResultCount(0);
        planAndExecuteQuery();
    }

    @Test
    public void testIndexedTermOutsideWindowDropsDocumentWithSeekingAggregation() throws Exception {
        maxEntries = LIMIT;
        seekingEventAggregation = true;
        givenDate("20260708");
        givenQuery("ZEBRA == 'stripes'");
        expectPlan("ZEBRA == 'stripes'");
        expectResultCount(0);
        planAndExecuteQuery();
    }

    @Test
    public void testEventOnlyTermOutsideWindowNoLimit() throws Exception {
        givenDate("20260708");
        givenQuery("APPLE == 'granny' && ZULU == 'target'");
        expectPlan("APPLE == 'granny' && ZULU == 'target'");
        expectResultCount(1);
        expectedFields.addAll(List.of("APPLE", "ZULU"));
        planAndExecuteQuery();
    }

    @Test
    public void testEventOnlyTermOutsideWindowIsTruncatedAway() throws Exception {
        maxEntries = LIMIT;
        givenDate("20260708");
        givenQuery("APPLE == 'granny' && ZULU == 'target'");
        expectPlan("APPLE == 'granny' && ZULU == 'target'");
        expectResultCount(0);
        planAndExecuteQuery();
    }
}
