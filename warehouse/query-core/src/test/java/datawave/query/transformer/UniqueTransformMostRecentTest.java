package datawave.query.transformer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Document;
import datawave.query.attributes.TemporalGranularity;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.sortedset.FileSortedSet;

public class UniqueTransformMostRecentTest extends UniqueTransformTest {

    protected ShardQueryLogic logic = new ShardQueryLogic();

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        uniqueFields.setMostRecent(true);

        // setup the hadoop configuration
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        assertNotNull("hadoop config cannot be null", hadoopConfig);
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        // setup a directory for cache results
        File tmpDir = temporaryFolder.newFolder();
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(tmpDir.toURI().toString());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));

        QueryImpl query = new QueryImpl();
        query.setId(UUID.randomUUID());
        logic.getConfig().setQuery(query);
    }

    @Override
    protected UniqueTransform getUniqueTransform() {
        return getUniqueTransform(Long.MAX_VALUE);
    }

    @Override
    protected UniqueTransform getUniqueTransform(long queryExecutionForPageTimeout) {
        try {
            // @formatter:off
            return new UniqueTransform.Builder()
                    .withUniqueFields(uniqueFields)
                    .withQueryExecutionForPageTimeout(queryExecutionForPageTimeout)
                    .withBufferPersistThreshold(logic.getUniqueCacheBufferSize())
                    .withIvaratorCacheDirConfigs(logic.getIvaratorCacheDirConfigs())
                    .withHdfsSiteConfigURLs(logic.getHdfsSiteConfigURLs())
                    .withSubDirectory(logic.getConfig().getQuery().getId().toString())
                    .withMaxOpenFiles(logic.getIvaratorMaxOpenFiles())
                    .withNumRetries(logic.getIvaratorNumRetries())
                    .withPersistOptions(new FileSortedSet.PersistOptions(true, false, 0))
                    .build();
            // @formatter:on
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The base class asserts that a unique document comes back from apply() as a real result. On the mostRecent path every document is accumulated into the
     * backing map instead, and real results are produced only by {@link UniqueTransform#flush()}, so that assertion cannot hold here. Pacing itself now applies
     * to this path and is covered by {@link #testIntermediateResultsArePacedOnMostRecentPath()}.
     */
    @Override
    @Test
    @Ignore
    public void testIntermediateResultsArePaced_afterPageTimerReset() {
        // see testIntermediateResultsArePacedOnMostRecentPath
    }

    /**
     * Ignored for the same reason as {@link #testIntermediateResultsArePaced_afterPageTimerReset()}: the base class interleaves real results with intermediate
     * ones, and apply() never returns a real result on the mostRecent path. The resumption half of the behaviour — a further intermediate result once the
     * timeout elapses again — is covered by {@link #testIntermediateResultsArePacedOnMostRecentPath()}.
     */
    @Override
    @Test
    @Ignore
    public void testRealAndIntermediateResultsResumeAfterPageTimerReset() {
        // see testIntermediateResultsArePacedOnMostRecentPath
    }

    /**
     * Verify that field matching is case-insensitive. Query: #UNIQUE(attr0, Attr1, ATTR2)
     */
    @Test
    public void testMostRecentUniqueness() {
        givenInputDocument(1).withKeyValue("ATTR0", randomValues.get(0));
        givenInputDocument(2).withKeyValue("ATTR0", randomValues.get(1)).isExpectedToBeUnique();
        givenInputDocument(3).withKeyValue("ATTR0", randomValues.get(0)).isExpectedToBeUnique();
        givenInputDocument(1).withKeyValue("Attr1", randomValues.get(2));
        givenInputDocument(2).withKeyValue("Attr1", randomValues.get(3)).isExpectedToBeUnique();
        givenInputDocument(3).withKeyValue("Attr1", randomValues.get(2)).isExpectedToBeUnique();
        givenInputDocument(1).withKeyValue("attr2", randomValues.get(4));
        givenInputDocument(2).withKeyValue("attr2", randomValues.get(0)).isExpectedToBeUnique();
        givenInputDocument(3).withKeyValue("attr2", randomValues.get(4)).isExpectedToBeUnique();

        givenValueTransformerForFields(TemporalGranularity.ALL, "attr0", "Attr1", "ATTR2");

        assertUniqueDocuments();
    }

    /**
     * On the mostRecent path, apply() must emit a keep-alive intermediate result once the per-page timeout is exceeded, rather than always returning null while
     * accumulating into the backing map. Emitting one resets the page timer, so intermediate results are paced at one per timeout window, and the documents
     * accumulated alongside them are still returned in full by {@link UniqueTransform#flush()}.
     */
    @Test
    public void testIntermediateResultsArePacedOnMostRecentPath() {
        givenInputDocument().withKeyValue("ATTR0", randomValues.get(0)).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("ATTR0", randomValues.get(1)).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("ATTR0", randomValues.get(2)).isExpectedToBeUnique();
        givenInputDocument().withKeyValue("ATTR0", randomValues.get(3)).isExpectedToBeUnique();

        givenValueTransformerForFields(TemporalGranularity.ALL, "ATTR0");

        UniqueTransform uniqueTransform = givenPageTimedMostRecentTransform();

        // the first document accumulated past the timeout stands in for the page as an intermediate result
        Key expectedKey = inputDocuments.get(0).getMetadata();
        Map.Entry<Key,Document> result = uniqueTransform.apply(entryFor(inputDocuments.get(0)));

        assertNotNull("Expected an intermediate result once the page timeout was exceeded", result);
        assertTrue("Expected the result to be flagged as an intermediate result", result.getValue().isIntermediateResult());
        assertEquals("An intermediate result must be an empty document", 0, result.getValue().size());
        assertTrue("An intermediate result must carry no attributes", result.getValue().getDictionary().isEmpty());
        assertEquals("An intermediate result must be keyed at the document it stands in for", expectedKey, result.getKey());

        // emitting it reset the page timer, so the rest of the window is paced back to null
        assertNull("Intermediate results must be paced to one per timeout window", uniqueTransform.apply(entryFor(inputDocuments.get(1))));
        assertNull("Intermediate results must be paced to one per timeout window", uniqueTransform.apply(entryFor(inputDocuments.get(2))));

        // once the timeout elapses again, a further intermediate result is emitted
        setClockTo(uniqueTransform, PAGE_START.plusMillis((2 * PAGE_TIMEOUT_MS) + 2));
        result = uniqueTransform.apply(entryFor(inputDocuments.get(3)));
        assertNotNull("Expected a further intermediate result once the timeout elapsed again", result);
        assertTrue("Expected the result to be flagged as an intermediate result", result.getValue().isIntermediateResult());

        // neither the keep-alives nor the pacing may cost the documents accumulated into the backing map
        List<Document> flushed = new ArrayList<>();
        Map.Entry<Key,Document> next;
        while ((next = uniqueTransform.flush()) != null) {
            flushed.add(next.getValue());
        }

        Collections.sort(expectedUniqueDocuments);
        Collections.sort(flushed);
        assertEquals("Most recent unique documents do not match expected", getIds(expectedUniqueDocuments), getIds(flushed));
    }

    /**
     * Builds a mostRecent transform whose page timer has already run past {@link #PAGE_TIMEOUT_MS}, so the very next apply() is eligible to emit an
     * intermediate result. Timing is driven by a fixed clock rather than wall time so the test never sleeps or races.
     *
     * @return a mostRecent transform ready to emit an intermediate result
     */
    private UniqueTransform givenPageTimedMostRecentTransform() {
        UniqueTransform uniqueTransform = getUniqueTransform(PAGE_TIMEOUT_MS);
        setClockTo(uniqueTransform, PAGE_START);
        uniqueTransform.setQueryExecutionForPageStartTime(uniqueTransform.clock.millis());
        setClockTo(uniqueTransform, PAGE_START.plusMillis(PAGE_TIMEOUT_MS + 1));
        return uniqueTransform;
    }

}
