package datawave;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TabletExtentCheckerTest {
    private static final String TABLE_NAME = "testTable";

    @TempDir
    private static Path tempDir;
    private static MiniAccumuloCluster mac;

    private AccumuloClient client;

    // Tracks the range of tablet extents to compact on
    private final List<Pair<Text,Text>> expectedExtents = new ArrayList<>();
    String begin;
    String end;
    boolean mergeExtents;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        mac = new MiniAccumuloCluster(tempDir.resolve("mac").toFile(), "secret");
        mac.start();
    }

    @AfterAll
    static void afterAll() throws IOException {
        mac.close();
    }

    @BeforeEach
    void setUp() {
        begin = null;
        end = null;
        mergeExtents = false;
        client = mac.createAccumuloClient("root", new PasswordToken("secret"));
    }

    @AfterEach
    void tearDown() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        expectedExtents.clear();
        if (client.tableOperations().exists(TABLE_NAME)) {
            client.tableOperations().delete(TABLE_NAME);
        }
        client.close();
    }

    /**
     * Contains test methods that verify {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)}  tracks
     * individual unmerged tablet extents given a table, tablet range, and mergeExtents == false.
     */
    @Nested
    class MergeExtentsDisabledTests {

        @BeforeEach
        void setUp() {
            mergeExtents = false;
        }

        /**
         * Verify that given a table where all tablets require compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns multiple ranges to compact each
         * tablet individually.
         */
        @Test
        void testAllSingleTabletsNeedCompaction()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null,2500) , (2500,4000) , (4000,7000) (7000,null)
            addSplits("2500", "4000", "7000");

            expectExtent(null, "2500");
            expectExtent("2500", "4000");
            expectExtent("4000", "7000");
            expectExtent("7000", null);
            assertResult();
        }

        /**
         * Verify that given a table where only the first tablet requires compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns a single range that would compact
         * only the first tablet.
         */
        @Test
        void testOnlyFirstTabletNeedsCompaction()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null,2500) , (2500,4000) , (4000,7000) (7000,null)
            addSplits("2500", "4000", "7000");

            // Compact all tablets other than the first tablet.
            compact("2500", null);

            // Add uncompacted tablet to expectedExtents
            expectExtent(null, "2500");
            assertResult();
        }

        /**
         * Verify that given a table where only the last tablet requires compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} a single range that would compact only the
         * last tablet.
         */
        @Test
        void testOnlyLastTabletNeedsCompaction()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();
            // Tablet Extents: (null,2500) , (2500,4000) , (4000,7000) (7000,null)
            addSplits("2500", "4000", "7000");

            // Compact all tablets except the last
            compact(null, "7000");

            expectExtent("7000", null);
            assertResult();
        }

        /**
         * Verify that given a table where no tablets require compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns an empty list.
         */
        @Test
        void testNoTabletsNeedCompaction() throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null,2500) , (2500,4000) , (4000,7000) (7000,null)
            addSplits("2500", "4000", "7000");

            compact(null, null);

            assertResult();
        }

        /**
         * Verify that given a table where only the first and last tablets require compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns separate ranges for the first and
         * last compactable tablets.
         */
        @Test
        void testOnlyFirstAndLastTabletsNeedCompaction()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            addSplits("2500", "4000", "7000");

            // Compact all tablets other than first tablet and last tablet
            // Range of first tablet: null - 2500
            // Range of last tablet: 7000 - null
            compact("2500", "7000");

            expectExtent(null, "2500");
            expectExtent("7000", null);
            assertResult();
        }

        /**
         * Verify that given a table where only the last two tablets require compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns separate ranges for those tablets.
         */
        @Test
        void testLastTwoTabletsNeedCompaction() throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            addSplits("2500", "4000", "5500", "7000", "8500");

            // Compact all tablets except last two tablets
            compact(null, "7000");

            // Add last two tablets to expectedExtents
            expectExtent("7000", "8500");
            expectExtent("8500", null);
            assertResult();
        }

        /**
         * Verify that given a table where only the middle tablet requires compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns a range to compact that middle
         * tablet.
         */
        @Test
        void testMiddleTabletNeedsCompaction() throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null,2500) , (2500,4000) , (4000,7000) (7000, null), (8000, null)
            addSplits("2500", "4000", "7000", "8000");

            compact(null, "4000");
            compact("7000", null);

            expectExtent("4000", "7000");
            assertResult();
        }

        /**
         * Verify that given a table where multiple series of tablets at different locations require compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns multiple compaction ranges that
         * includes a series of contiguous compactable tablets by merging their extents.
         */
        @Test
        void testMultipleInnerCompactionRanges()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();
            addSplits("1000", "1500", "2000", "2500", "3000", "4000", "5500", "6500", "7000", "7500", "8500", "9000");
            // Tablet Extents: (null, 1000), (1000, 1500), (1500,2000), (2000,2500), (2500,3000), (3000,4000), (4000,5500),
            // (5500, 6500), (6500,7000), (7000,7500), (7500,8500) (8500,9000)

            // Compact extents to make a series of compacted tablets then track the uncompacted tablets:
            compact(null, "1000");
            compact("2500", "3000");
            compact("6500", "7000");
            compact("8500", null);

            // Track uncompacted tablets in the key extent series ("1000","2500")
            // Tablets: (1000, 1500), (1500,2000), (2000,2500)
            expectExtent("1000", "1500");
            expectExtent("1500", "2000");
            expectExtent("2000", "2500");

            // Track uncompacted tablets in the key extent series ("3000","6500")
            // Tablets: (3000,4000), (4000,5500), (5500, 6500)
            expectExtent("3000", "4000");
            expectExtent("4000", "5500");
            expectExtent("5500", "6500");

            // Track uncompacted tablets in the key extent series ("7000","8500")
            // Tablets: (7000,7500), (7500,8500)
            expectExtent("7000", "7500");
            expectExtent("7500", "8500");
            assertResult();
        }

        /**
         * Verify that given a table where all tablets starting from row 2000 to null are compacted, and given a tablet range from row 2000 to null,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns an empty list.
         */
        @Test
        void testCompactableTabletOutsideInputRange()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null, 1000), (1000, 1500), (1500,2000), (2000,2500), (2500,3000), (3000,4000), (4000,5500),
            // (5500, 6500), (6500,7000), (7000,7500), (7500,8500) (8500,9000)
            addSplits("1000", "1500", "2000", "2500", "3000", "4000", "5500", "6500", "7000", "7500", "8500", "9000");

            compact("2000", null);

            givenBegin("2000");

            // expectedExtents should be empty, so just make the assertion
            assertResult();
        }

        /**
         * Verify that given a table with a compactable tablet, and given a key extent range that is contained within a tablet's range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} will return the full extent of the tablet.
         */
        @Test
        void testInputInCompactableTabletRange()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null, 1000), (1000, 1500), (1500,2000), (2000,2500), (2500,3000), (3000,4000), (4000,5500),
            // (5500, 6500), (6500,7000), (7000,7500), (7500,8500) (8500,9000)
            addSplits("1000", "1500", "2000", "2500", "3000", "4000", "5500", "6500", "7000", "7500", "8500", "9000");

            // Compact all tablets from null to row 1000 and from row 1500 to null
            compact(null, "1000");
            compact("1500", null);

            // Compactable tablet: (1000,1500)
            givenBegin("1000");
            givenEnd("1250");
            expectExtent("1000", "1500");
            assertResult();
        }

        /**
         * Verify that given a table with compactable tablets, a key extent range, starting row and ending row, mergeExtents = false, and compactTablets = true,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} will return an empty list after compacting
         * the tablets automatically in {@link TabletExtentChecker#checkTablets(ClientContext, TabletExtentChecker.Opts)} .
         */

        @Test
        void testCompactionParameter() throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null, 1000), (1000, 1500), (1500,2000), (2000,2500), (2500,3000), (3000,4000), (4000,5500),
            // (5500, 6500), (6500,7000), (7000,7500), (7500,8500) (8500,9000)
            addSplits("1000", "1500", "2000", "2500", "3000", "4000", "5500", "6500", "7000", "7500", "8500", "9000");

            assertCompactedResult();
        }

    }

    /**
     * Contains test methods that verify {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} tracks
     * individual unmerged tablet extents given client properties, the table name, a user provided tablet range, and mergeExtents == true.
     */
    @Nested
    class MergeExtentsEnabledTests {

        @BeforeEach
        void setUp() {
            mergeExtents = true;
        }

        /**
         * Verify that given a table where all tablets require compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns a single compaction range that would
         * compact the entire table.
         */
        @Test
        void testAllTabletsNeedCompaction() throws AccumuloException, TableExistsException, AccumuloSecurityException, TableNotFoundException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null,2500) , (2500,4000) , (4000,7000) (7000,null)
            addSplits("2500", "4000", "7000");

            expectExtent(null, null);
            assertResult();
        }

        /**
         * Verify that given a table where only the first tablet requires compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns a single range that would compact
         * only the first tablet.
         */
        @Test
        void testOnlyFirstTabletNeedsCompaction()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null,2500) , (2500,4000) , (4000,7000) (7000,null)
            addSplits("2500", "4000", "7000");

            // Compact all tablets other than the first tablet.
            compact("2500", null);

            expectExtent(null, "2500");
            assertResult();
        }

        /**
         * Verify that given a table where only the last tablet requires compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns a single range that would compact
         * only the last tablet.
         */
        @Test
        void testOnlyLastTabletNeedsCompaction()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null,2500) , (2500,4000) , (4000,7000) (7000,null)
            addSplits("2500", "4000", "7000");

            // Compact all tablets except the last
            compact(null, "7000");

            expectExtent("7000", null);
            assertResult();
        }

        /**
         * Verify that given a table where no tablets require compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns an empty list.
         */
        @Test
        void testNoTabletsNeedCompaction() throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null,2500) , (2500,4000) , (4000,7000) (7000,null)
            addSplits("2500", "4000", "7000");

            // Compact all tablets
            compact(null, null);

            assertResult();
        }

        /**
         * Verify that given a table where only the first and last tablets require compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)}  returns separate ranges for the first and
         * last compactable tablets.
         */
        @Test
        void testOnlyFirstAndLastTabletsNeedCompaction()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null,2500) , (2500,4000) , (4000,7000) (7000,null)
            addSplits("2500", "4000", "7000");

            // Compact all tablets other than first tablet and last tablet
            compact("2500", "7000");

            expectExtent(null, "2500");
            expectExtent("7000", null);

            assertResult();
        }

        /**
         * Verify that given a table where only the last two tablets require compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns a single range that compacts the last
         * two tablets.
         */
        @Test
        void testLastTwoTabletsNeedCompaction() throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null,2500) , (2500,4000) , (4000,5500), (5500,7000), (7000,8500) (8500,null)
            addSplits("2500", "4000", "5500", "7000", "8500");

            // Compact all tablets except last two tablets
            compact(null, "7000");

            // Add last two tablets to expectedExtents
            expectExtent("7000", null);
            assertResult();
        }

        /**
         * Verify that given a table where only the middle tablet requires compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns a range to compact that middle
         * tablet.
         */
        @Test
        void testMiddleTabletNeedsCompaction() throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null,2500) , (2500,4000) , (4000,7000) (7000,null), (8000,null)
            addSplits("2500", "4000", "7000", "8000");

            // Compact all tablets except the tablet with the extent of (4000,7000)
            compact(null, "4000");
            compact("7000", null);

            expectExtent("4000", "7000");
            assertResult();
        }

        /**
         * Verify that given a table where multiple series of tablets at different locations require compaction, and given a tablet range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns multiple compaction ranges that
         * includes a series of contiguous compactable tablets by merging their extents.
         */
        @Test
        void testMultipleInnerCompactionRanges()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null, 1000), (1000, 1500), (1500,2000), (2000,2500), (2500,3000), (3000,4000), (4000,5500),
            // (5500, 6500), (6500,7000), (7000,7500), (7500,8500) (8500,9000)
            addSplits("1000", "1500", "2000", "2500", "3000", "4000", "5500", "6500", "7000", "7500", "8500", "9000");

            // Compact extents to make a series of compacted tablets then track the series of uncompacted tablets:
            compact(null, "1000");
            compact("2500", "3000");
            compact("6500", "7000");
            compact("8500", null);

            // Track uncompacted extent series:
            expectExtent("1000", "2500"); // Tablets: (1000, 1500), (1500,2000), (2000,2500)
            expectExtent("3000", "6500"); // Tablets: (3000,4000), (4000,5500), (5500, 6500)
            expectExtent("7000", "8500"); // Tablets: (7000,7500), (7500,8500)
            assertResult();
        }

        /**
         * Verify that given a table where all tablets starting from row 2000 to null are compacted, and given a tablet range from row 2000 to null,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} returns an empty list.
         */
        @Test
        void testCompactableTabletOutsideInputRange()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null, 1000), (1000, 1500), (1500,2000), (2000,2500), (2500,3000), (3000,4000), (4000,5500),
            // (5500, 6500), (6500,7000), (7000,7500), (7500,8500) (8500,9000)
            addSplits("1000", "1500", "2000", "2500", "3000", "4000", "5500", "6500", "7000", "7500", "8500", "9000");

            // Compact all tablets starting from row 2000
            compact("2000", null);

            // Only look at the range (2000,null) so expectedExtents should be empty
            givenBegin("2000");
            givenEnd(null);
            assertResult();
        }

        /**
         * Verify that given a table with a compactable tablet, and given a key extent range that is contained within a tablet's range,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} will return the full extent of the tablet.
         */
        @Test
        void testInputInCompactableTabletRange()
                        throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null, 1000), (1000, 1500), (1500,2000), (2000,2500), (2500,3000), (3000,4000), (4000,5500),
            // (5500, 6500), (6500,7000), (7000,7500), (7500,8500) (8500,9000)
            addSplits("1000", "1500", "2000", "2500", "3000", "4000", "5500", "6500", "7000", "7500", "8500", "9000");

            // Compact all tablets from null to row 1000 and from row 1500 to null
            compact(null, "1000");
            compact("1500", null);

            // Compactable tablet: (1000,1500)
            givenBegin("1000");
            givenEnd("1250");
            expectExtent("1000", "1500");
            assertResult();
        }

        /**
         * Verify that given a table with compactable tablets, a key extent range, starting row and ending row, mergeExtents = true, and compactTablets = true,
         * {@link TabletExtentChecker#findCompactableTablets(ClientContext, TabletExtentChecker.Opts)} will return an empty list after compacting
         * the tablets automatically in {@link TabletExtentChecker#checkTablets(ClientContext, TabletExtentChecker.Opts)}.
         */

        @Test
        void testCompactionParameter() throws AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException, IOException {
            createTableAndWriteData();

            // Tablet Extents: (null, 1000), (1000, 1500), (1500,2000), (2000,2500), (2500,3000), (3000,4000), (4000,5500),
            // (5500, 6500), (6500,7000), (7000,7500), (7500,8500) (8500,9000)
            addSplits("1000", "1500", "2000", "2500", "3000", "4000", "5500", "6500", "7000", "7500", "8500", "9000");

            assertCompactedResult();
        }

    }

    private void createTableAndWriteData() throws AccumuloException, TableExistsException, AccumuloSecurityException, TableNotFoundException {
        client.tableOperations().create(TABLE_NAME);

        try (BatchWriter writer = client.createBatchWriter(TABLE_NAME)) {
            for (int i = 0; i < 10000; i += 250) {
                String row = String.format("%04d", i);
                Mutation m = new Mutation(row);
                m.put("cf", "cq", "v");
                writer.addMutation(m);
            }
        }
        client.tableOperations().flush(TABLE_NAME);
    }

    private void addSplits(String... splits) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        SortedSet<Text> set = new TreeSet<>();
        Arrays.stream(splits).map(Text::new).forEach(set::add);
        client.tableOperations().addSplits(TABLE_NAME, set);
    }

    private void compact(String start, String end) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        client.tableOperations().compact(TABLE_NAME, getText(start), getText(end), true, true);
    }

    private Text getText(String arg) {
        return (arg == null) ? null : new Text(arg);
    }

    private void expectExtent(String start, String end) {
        expectedExtents.add(Pair.of(getText(start), getText(end)));
    }

    private void givenBegin(String begin) {
        this.begin = begin;
    }

    private void givenEnd(String end) {
        this.end = end;
    }

    private void assertResult() throws AccumuloException, TableNotFoundException, IOException {
        TabletExtentChecker.Opts opts = new TabletExtentChecker.Opts();
        opts.tableName = TABLE_NAME;
        opts.beginRow = begin == null ? null : new Text(begin);
        opts.endRow = end == null ? null : new Text(end);
        opts.mergeExtents = mergeExtents;
        opts.compactTablets = false;
        List<Pair<Text,Text>> result = TabletExtentChecker.findCompactableTablets((ClientContext) client, opts);
        assertEquals(expectedExtents, result);
    }

    private void assertCompactedResult() throws AccumuloException, TableNotFoundException, AccumuloSecurityException, IOException {
        TabletExtentChecker.Opts opts = new TabletExtentChecker.Opts();
        opts.tableName = TABLE_NAME;
        opts.beginRow = begin == null ? null : new Text(begin);
        opts.endRow = end == null ? null : new Text(end);
        opts.mergeExtents = mergeExtents;
        opts.compactTablets = true;

        // Build the list of compactable tablets
        List<Pair<Text,Text>> result = TabletExtentChecker.findCompactableTablets((ClientContext) client, opts);

        // Compact the tablets instead of suggesting compactions (compactTablets == true)
        TabletExtentChecker.checkTablets((ClientContext) client, opts);

        List<Pair<Text,Text>> result2 = TabletExtentChecker.findCompactableTablets((ClientContext) client, opts);

        // Fetch the list of compactable tablets within the same range.
        List<Pair<Text,Text>> extents = TabletExtentChecker.findCompactableTablets((ClientContext) client, opts);

        // Verify the list is empty, indicating success.
        assertTrue(extents.isEmpty());
    }

}
