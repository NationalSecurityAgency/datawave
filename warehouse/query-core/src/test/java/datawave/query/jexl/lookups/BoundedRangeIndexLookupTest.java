package datawave.query.jexl.lookups;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import datawave.microservice.query.QueryImpl;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.jexl.LiteralRange;
import datawave.query.scanner.LocalBatchScanner;
import datawave.query.tables.ScannerFactory;

public class BoundedRangeIndexLookupTest extends EasyMockSupport {
    private BoundedRangeIndexLookup lookup;
    private ShardQueryConfiguration config;
    private ScannerFactory scannerFactory;

    @Before
    public void setup() {
        config = new ShardQueryConfiguration();
        scannerFactory = createMock(ScannerFactory.class);
    }

    @Test
    public void largeRowInBoundedRangeTest() throws TableNotFoundException {
        ExecutorService s = Executors.newSingleThreadExecutor();

        Date begin = new Date();
        Date end = new Date();
        config.setBeginDate(begin);
        config.setEndDate(end);
        config.setNumQueryThreads(1);
        // defaults to 5000
        config.setMaxValueExpansionThreshold(1);

        SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd");

        LiteralRange range = new LiteralRange("R", true, "S", false, "FOO", LiteralRange.NodeOperand.OR);
        lookup = new BoundedRangeIndexLookup(config, scannerFactory, range, s);

        // create index data to iterate over
        List<Map.Entry<Key,Value>> src = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            src.add(new AbstractMap.SimpleImmutableEntry<>(new Key("R" + i, "FOO", sdf.format(begin) + "_1" + '\0' + "myDataType"), new Value()));
        }
        SortedListKeyValueIterator itr = new SortedListKeyValueIterator(src);
        LocalBatchScanner scanner = new LocalBatchScanner(itr, true);

        // add expects for the scanner factory
        expect(scannerFactory.newScanner(eq("shardIndex"), isA(Set.class), eq(1), isA(QueryImpl.class), eq("shardIndex"))).andAnswer(() -> scanner);

        expect(scannerFactory.close(scanner)).andReturn(true);

        replayAll();

        lookup.submit();
        IndexLookupMap map = lookup.lookup();

        // verify we went over all the data even though the threshold was lower than this
        assertEquals(1, scanner.getSeekCount());

        // this represents data collapsed and sent back to the client by the WholeRowIterator
        assertEquals(10000, scanner.getNextCount());

        assertTrue(map.get("FOO").isThresholdExceeded());

        verifyAll();
    }
}
