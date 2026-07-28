package datawave.query.transformer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Maps;

import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Document;
import datawave.query.attributes.TemporalGranularity;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.sortedset.FileSortedSet;

public class UniqueTransformMostRecentTest extends UniqueTransformTest {

    protected ShardQueryLogic logic = new ShardQueryLogic();

    private final Clock clock = Clock.systemUTC();

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
     * On the mostRecent path, apply() must still emit a keep-alive intermediate result once the per-page timeout is exceeded, rather than always returning null
     * while accumulating into the backing map.
     */
    @Test
    public void testIntermediateResultsAreEmitted_onMostRecentPath_whenPageTimeoutExceeded() {
        givenInputDocument().withKeyValue("ATTR0", randomValues.get(0));
        givenInputDocument().withKeyValue("ATTR0", randomValues.get(1));
        givenInputDocument().withKeyValue("ATTR0", randomValues.get(2));

        givenValueTransformerForFields(TemporalGranularity.ALL, "ATTR0");

        // 1s page timeout, and pretend the page started well in the past so the timeout is already exceeded.
        UniqueTransform uniqueTransform = getUniqueTransform(1000L);
        uniqueTransform.setQueryExecutionForPageStartTime(clock.millis() - 10_000L);

        int intermediate = 0;
        for (Document document : inputDocuments) {
            Map.Entry<Key,Document> result = uniqueTransform.apply(Maps.immutableEntry(document.getMetadata(), document));
            if (result != null && result.getValue().isIntermediateResult()) {
                intermediate++;
            }
        }
        assertTrue("expected at least one intermediate result on the mostRecent path", intermediate >= 1);
    }

}
