package datawave.query.transformer;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.UUID;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.UniqueGranularity;
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
        try {
            // @formatter:off
            return new UniqueTransform.Builder()
                    .withUniqueFields(uniqueFields)
                    .withQueryExecutionForPageTimeout(Long.MAX_VALUE)
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

        givenValueTransformerForFields(UniqueGranularity.ALL, "attr0", "Attr1", "ATTR2");

        assertUniqueDocuments();
    }

}
