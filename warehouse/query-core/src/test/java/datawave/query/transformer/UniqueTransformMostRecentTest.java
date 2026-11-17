package datawave.query.transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Document;
import datawave.query.attributes.TemporalGranularity;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.sortedset.FileSortedSet;

public class UniqueTransformMostRecentTest extends UniqueTransformTest {

    protected ShardQueryLogic logic = new ShardQueryLogic();

    @TempDir
    static File temporaryFolder;

    @BeforeEach
    public void setUp() throws IOException {
        uniqueFields.setMostRecent(true);

        // setup the hadoop configuration
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        assertNotNull(hadoopConfig, "hadoop config cannot be null");
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        // setup a directory for cache results
        File tmpDir = Files.createTempDirectory(temporaryFolder.toPath(), "cache-").toFile();
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
    @Disabled
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
    @Disabled
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

        assertNotNull(result, "Expected an intermediate result once the page timeout was exceeded");
        assertTrue(result.getValue().isIntermediateResult(), "Expected the result to be flagged as an intermediate result");
        assertEquals(0, result.getValue().size(), "An intermediate result must be an empty document");
        assertTrue(result.getValue().getDictionary().isEmpty(), "An intermediate result must carry no attributes");
        assertEquals(expectedKey, result.getKey(), "An intermediate result must be keyed at the document it stands in for");

        // emitting it reset the page timer, so the rest of the window is paced back to null
        assertNull(uniqueTransform.apply(entryFor(inputDocuments.get(1))), "Intermediate results must be paced to one per timeout window");
        assertNull(uniqueTransform.apply(entryFor(inputDocuments.get(2))), "Intermediate results must be paced to one per timeout window");

        // once the timeout elapses again, a further intermediate result is emitted
        setClockTo(uniqueTransform, PAGE_START.plusMillis((2 * PAGE_TIMEOUT_MS) + 2));
        result = uniqueTransform.apply(entryFor(inputDocuments.get(3)));
        assertNotNull(result, "Expected a further intermediate result once the timeout elapsed again");
        assertTrue(result.getValue().isIntermediateResult(), "Expected the result to be flagged as an intermediate result");

        // neither the keep-alives nor the pacing may cost the documents accumulated into the backing map
        List<Document> flushed = new ArrayList<>();
        Map.Entry<Key,Document> next;
        while ((next = uniqueTransform.flush()) != null) {
            flushed.add(next.getValue());
        }

        Collections.sort(expectedUniqueDocuments);
        Collections.sort(flushed);
        assertEquals(getIds(expectedUniqueDocuments), getIds(flushed), "Most recent unique documents do not match expected");
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
