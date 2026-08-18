package datawave.accumulo.shell;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class DataWaveScanCommandTest {

    private static final String ROW = "20260818_0";
    private static final Text EVENT_CF = text("datatype\0uid");
    private static final Text EVENT_CQ = text("FIELD\0value");

    private final DataWaveScanCommand command = new DataWaveScanCommand();

    private static Text text(String value) {
        return new Text(value.getBytes(StandardCharsets.UTF_8));
    }

    private CommandLine parse(String... args) throws Exception {
        return new DefaultParser().parse(command.getOptions(), args);
    }

    private Range range(String... args) throws Exception {
        return command.getRange(parse(args), null);
    }

    @Test
    public void testRowWithColumnFamilyAndQualifierSeeksToTheColumn() throws Exception {
        Range range = range("-r", ROW, "-cf", "datatype\\0uid", "-cq", "FIELD\\0value");

        Key start = new Key(new Text(ROW), EVENT_CF, EVENT_CQ, Long.MAX_VALUE);
        assertEquals(new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false), range);
        assertTrue(range.contains(new Key(new Text(ROW), EVENT_CF, EVENT_CQ, 1L)));
        assertFalse(range.contains(new Key(new Text(ROW), EVENT_CF, text("FIELD\0other"))));
        assertFalse(range.contains(new Key(new Text(ROW), text("datatype\0other"), EVENT_CQ)));
    }

    @Test
    public void testRowWithColumnFamilyCoversTheWholeColumnFamily() throws Exception {
        Range range = range("-r", ROW, "-cf", "datatype\\0uid");

        Key start = new Key(new Text(ROW), EVENT_CF);
        assertEquals(new Range(start, true, start.followingKey(PartialKey.ROW_COLFAM), false), range);
        assertTrue(range.contains(new Key(new Text(ROW), EVENT_CF, text(""))));
        assertTrue(range.contains(new Key(new Text(ROW), EVENT_CF, text("￿"))));
        assertFalse(range.contains(new Key(new Text(ROW), text("datatype\0uid\0"), EVENT_CQ)));
    }

    @Test
    public void testRowOnlyMatchesTheStockRowRange() throws Exception {
        assertEquals(new Range(new Text(ROW)), range("-r", ROW));
    }

    @Test
    public void testExclusiveEndpointsAreIgnoredForAnUnscopedSingleRow() throws Exception {
        // the stock command ignores -be and -ee when -r is given, and honoring them here would produce an empty range
        assertEquals(new Range(new Text(ROW)), range("-r", ROW, "-be", "-ee"));
    }

    @Test
    public void testExclusiveEndKeepsAColumnEndpointOutOfTheRange() throws Exception {
        Range range = range("-r", ROW, "-ekcf", "datatype\\0uid", "-ee");

        assertEquals(new Key(new Text(ROW), EVENT_CF), range.getEndKey());
        assertFalse(range.isEndKeyInclusive());
        assertTrue(range.contains(new Key(new Text(ROW), text("datatype\0earlier"), EVENT_CQ)));
        assertFalse(range.contains(new Key(new Text(ROW), EVENT_CF, EVENT_CQ)));
    }

    @Test
    public void testBeginKeyQualifierRequiresABeginKeyColumnFamily() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> range("-r", ROW, "-bkcq", "FIELD\\0value"));
    }

    @Test
    public void testEndKeyQualifierRequiresAnEndKeyColumnFamily() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> range("-r", ROW, "-ekcq", "FIELD\\0value"));
    }

    @Test
    public void testRowRangeWithoutColumnScopingMatchesTheStockRange() throws Exception {
        assertEquals(new Range(new Text("a"), true, new Text("b"), true), range("-b", "a", "-e", "b"));
        assertEquals(new Range(new Text("a"), false, new Text("b"), false), range("-b", "a", "-e", "b", "-be", "-ee"));
    }

    @Test
    public void testRowRangeWithAColumnFamilyDoesNotNarrowTheRange() throws Exception {
        // a column family cannot bound a range that spans rows, so -cf only filters columns there
        assertEquals(new Range(new Text("a"), true, new Text("b"), true), range("-b", "a", "-e", "b", "-cf", "datatype\\0uid"));
    }

    @Test
    public void testColumnFamilyPrefixScopesTheRange() throws Exception {
        Range range = range("-r", ROW, "-cfp", "fi\\0");

        assertEquals(new Range(new Key(new Text(ROW), text("fi\0")), true, new Key(new Text(ROW), text("fi\1")), false), range);
        assertTrue(range.contains(new Key(new Text(ROW), text("fi\0FIELD"), text("value\0datatype\0uid"))));
        assertFalse(range.contains(new Key(new Text(ROW), EVENT_CF, EVENT_CQ)));
    }

    @Test
    public void testColumnQualifierPrefixScopesTheRange() throws Exception {
        Range range = range("-r", ROW, "-cf", "fi\\0FIELD", "-cqp", "value\\0");

        Text fieldIndexCf = text("fi\0FIELD");
        assertEquals(new Range(new Key(new Text(ROW), fieldIndexCf, text("value\0")), true, new Key(new Text(ROW), fieldIndexCf, text("value\1")), false),
                        range);
        assertTrue(range.contains(new Key(new Text(ROW), fieldIndexCf, text("value\0datatype\0uid"))));
        assertFalse(range.contains(new Key(new Text(ROW), fieldIndexCf, text("valuf"))));
    }

    @Test
    public void testBeginAndEndKeyOptionsSetTheEndpoints() throws Exception {
        Range range = range("-b", "a", "-e", "b", "-bkcf", "datatype\\0uid", "-ekcf", "datatype\\0uid2");

        Key start = new Key(new Text("a"), EVENT_CF);
        Key end = new Key(new Text("b"), text("datatype\0uid2"));
        assertEquals(new Range(start, true, end.followingKey(PartialKey.ROW_COLFAM), false), range);
    }

    @Test
    public void testEndKeyQualifierIsIncludedWholeUnlessExclusive() throws Exception {
        Key end = new Key(new Text(ROW), EVENT_CF, EVENT_CQ);
        assertEquals(end.followingKey(PartialKey.ROW_COLFAM_COLQUAL), range("-r", ROW, "-ekcf", "datatype\\0uid", "-ekcq", "FIELD\\0value").getEndKey());
        assertEquals(end, range("-r", ROW, "-ekcf", "datatype\\0uid", "-ekcq", "FIELD\\0value", "-ee").getEndKey());
    }

    @Test
    public void testTimestampEndpointsBoundTheKeyExactly() throws Exception {
        Range range = range("-r", ROW, "-bkcf", "datatype\\0uid", "-bkts", "100", "-ekcf", "datatype\\0uid", "-ekts", "50");

        assertEquals(new Key(new Text(ROW), EVENT_CF, new Text(), 100L), range.getStartKey());
        assertTrue(range.contains(new Key(new Text(ROW), EVENT_CF, new Text(), 50L)));
        assertFalse(range.contains(new Key(new Text(ROW), EVENT_CF, new Text(), 49L)));
        assertFalse(range.contains(new Key(new Text(ROW), EVENT_CF, new Text(), 101L)));
    }

    @Test
    public void testEscapesCanBeDisabled() throws Exception {
        assertEquals(new Text("datatype\\0uid"), range("-r", ROW, "-cf", "datatype\\0uid", "--no-escapes").getStartKey().getColumnFamily());
    }

    @Test
    public void testRowIsDecodedToo() throws Exception {
        assertEquals(text("a\0b"), range("-r", "a\\0b").getStartKey().getRow());
        assertEquals(text("a\0b"), range("-b", "a\\0b").getStartKey().getRow());
    }

    @Test
    public void testRowAndRowRangeAreMutuallyExclusive() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> range("-r", ROW, "-b", "a"));
    }

    @Test
    public void testColumnQualifierRequiresAColumnFamily() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> range("-r", ROW, "-cq", "FIELD\\0value"));
    }

    @Test
    public void testColumnsAreMutuallyExclusiveWithColumnFamily() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> range("-r", ROW, "-c", "a:b", "-cf", "a"));
    }

    @Test
    public void testPrefixScanRequiresARow() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> range("-b", "a", "-cfp", "fi\\0"));
    }

    @Test
    public void testQualifierPrefixRequiresAColumnFamily() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> range("-r", ROW, "-cqp", "value\\0"));
    }

    @Test
    public void testPrefixOptionsCannotBeCombinedWithKeyEndpoints() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> range("-r", ROW, "-cfp", "fi\\0", "-ekcf", "fi\\0x"));
    }

    @Test
    public void testColumnScopingRequiresARow() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> range("-bkcf", "datatype\\0uid"));
    }

    @Test
    public void testMalformedTimestampIsRejected() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> range("-r", ROW, "-bkts", "yesterday"));
    }

    @Test
    public void testScopedRangeSkipsRedundantColumnFetching() throws Exception {
        ScannerBase scanner = EasyMock.createMock(ScannerBase.class);
        EasyMock.replay(scanner);

        command.fetchColumns(parse("-r", ROW, "-cf", "datatype\\0uid", "-cq", "FIELD\\0value"), scanner, null);

        EasyMock.verify(scanner);
    }

    @Test
    public void testColumnsAreFetchedWhenTheRangeSpansRows() throws Exception {
        ScannerBase scanner = EasyMock.createMock(ScannerBase.class);
        scanner.fetchColumnFamily(EVENT_CF);
        EasyMock.replay(scanner);

        command.fetchColumns(parse("-b", "a", "-e", "b", "-cf", "datatype\\0uid"), scanner, null);

        EasyMock.verify(scanner);
    }

    @Test
    public void testColumnListIsDecoded() throws Exception {
        ScannerBase scanner = EasyMock.createMock(ScannerBase.class);
        scanner.fetchColumn(EVENT_CF, EVENT_CQ);
        scanner.fetchColumnFamily(text("fi\0FIELD"));
        EasyMock.replay(scanner);

        command.fetchColumns(parse("-r", ROW, "-c", "datatype\\0uid:FIELD\\0value,fi\\0FIELD"), scanner, null);

        EasyMock.verify(scanner);
    }

    @Test
    public void testTheOutputFileAndShowFewOptionsRemainAvailable() {
        assertTrue(command.getOptions().hasOption("o"));
        assertTrue(command.getOptions().hasOption("f"));
    }

    @Test
    public void testTheCommandIsRegisteredUnderTheExtension() {
        DataWaveShellExtension extension = new DataWaveShellExtension();
        assertEquals("dw", extension.getExtensionName());
        assertEquals("scan", extension.getCommands()[0].getName());
    }
}
