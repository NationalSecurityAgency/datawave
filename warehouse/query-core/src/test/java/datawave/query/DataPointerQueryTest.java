package datawave.query;

import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import datawave.microservice.query.Query;
import datawave.query.util.WiseGuysIngest;
import org.junit.rules.TemporaryFolder;

public class DataPointerQueryTest extends BaseWiseGuyTest {
    private Set<String> expected;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        expected = new HashSet<>();
    }

    @Test
    public void testWithoutFieldQuery() throws Exception {
        expected.add(WiseGuysIngest.corleoneUID);
        Query settings = getSettings("NOME == 'fredo'", getDate("20130101 000000"), getDate("20130101 235959"));
        runQuery(settings, expected);
    }

    // betrayal off the global index
    @Test
    public void testWithPointerFieldNoRegexQuery() throws Exception {
        expected.add(WiseGuysIngest.corleoneUID);
        Query settings = getSettings("NOME == 'fredo' && PHILOSOPHY == 'betrayal'", getDate("20130101 000000"), getDate("20130101 235959"));
        runQuery(settings, expected);
    }

    // betrayal off the data pointer
    // regex is delayed
    @Test
    public void testWithPointerFieldWithRegexQuery() throws Exception {
        expected.add(WiseGuysIngest.corleoneUID);

        Query settings = getSettings("NOME == 'fredo' && PHILOSOPHY =~ '.*betrayal.*'", getDate("20130101 000000"), getDate("20130101 235959"));
        runQuery(settings, expected);
    }

    // this misses because the pointer is fetched, but truncated prior to the possibility of hitting the regex
    // regex is delayed
    @Test
    public void testWithPointerFieldAndRegexButTruncated() throws Exception {
        logic.setDataPointerMaxLength(10);

        Query settings = getSettings("NOME == 'fredo' && PHILOSOPHY =~ '.*betrayal.*'", getDate("20130101 000000"), getDate("20130101 235959"));
        runQuery(settings, expected);
    }

    // TODO why is this delayed in normal query planning?
    // timedExpandRegex, DefaultQueryPlanner:1046 is delaying it, find out why
     // TODO doc aggregation should not expand a pointer even if a regex node if its inside an ivarator?
    // force regex to an ivarator
    // matches on the untokenized string in the fi
    // TODO this could be dangerous if we allow large values here
    @Test
    public void testWithIvarator() throws Exception {
        // setup the hadoop configuration
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        // setup a directory for cache results
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(temporaryFolder.newFolder().toURI().toString());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));

        expected.add(WiseGuysIngest.corleoneUID);

        Query settings = getSettings("NOME == 'fredo' && ((_Value_ = true) && PHILOSOPHY =~ '.*et.*')", getDate("20130101 000000"), getDate("20130101 235959"));
        runQuery(settings, expected);
    }
}
