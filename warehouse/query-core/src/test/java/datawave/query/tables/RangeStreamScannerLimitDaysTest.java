package datawave.query.tables;

import datawave.query.index.lookup.EntryParser;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.ScannerStream;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.Tuple2;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Integration test for the {@link RangeStreamScanner}
 */
public class RangeStreamScannerLimitDaysTest extends RangeStreamScannerTest{

    @Before
    public void augmentConfig(){
        config.getServiceConfiguration().getIndexingConfiguration().setEnableRangeScannerLimitDays(true);
    }

    @Override @Test
    public void testExceedShardsPerDayThresholdAndDocumentsPerShardThreshold() throws Exception {
        // Components that define the query: "FOO == 'boohoo'"
        String fieldName = "FOO";
        String fieldValue = "boohoo";
        ASTEQNode eqNode = (ASTEQNode) JexlNodeFactory.buildEQNode(fieldName, fieldValue);

        // Construct a ScannerStream from RangeStreamScanner, iterator, entry parser.
        RangeStreamScanner rangeStreamScanner = buildRangeStreamScanner(fieldName, fieldValue);
        EntryParser entryParser = new EntryParser(eqNode, fieldName, fieldValue, config.getIndexedFields());
        // Iterator<Tuple2<String,IndexInfo>> iterator = Iterators.transform(rangeStreamScanner, entryParser);
        ScannerStream scannerStream = ScannerStream.initialized(rangeStreamScanner, entryParser, eqNode);

        // Assert the iterator correctly iterates over the iterables without irritating the unit test.
        assertTrue(scannerStream.hasNext());
        int shardCount = 0;
        int documentCount = 0;
        while (scannerStream.hasNext()) {
            Tuple2<String,IndexInfo> entry = scannerStream.next();
            assertTrue("Expected shard to start with '20190323' but was: " + entry.first(), entry.first().startsWith("20190323"));
            shardCount++;
            documentCount += entry.second().count();
        }
        // A single range with a count of -1 means the shard ranges were collapsed into a day range.
        assertEquals(15, shardCount);
        assertEquals(375, documentCount);
        assertFalse(scannerStream.hasNext());
    }
}
