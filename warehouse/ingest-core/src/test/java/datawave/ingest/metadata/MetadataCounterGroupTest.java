package datawave.ingest.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class MetadataCounterGroupTest {

    public static final Text COLUMN_FAMILY = new Text("yuuuup");
    private String date = "20140407";
    private String dataType = "sham";
    private String fieldName = "wow";

    @Test
    public void testCanAdd() {
        helperTestAdd(new MetadataCounterGroup(COLUMN_FAMILY));
        helperTestAdd(new MetadataCounterGroup("LAC", COLUMN_FAMILY));
    }

    private void helperTestAdd(MetadataCounterGroup counters) {
        counters.addToCount(1, dataType, fieldName, date);
        counters.addToCount(1, dataType, fieldName, date);
        assertOneEntryWithExpectedCount(counters, 2);
    }

    @Test
    public void testTwoDataTypes() {
        helperTestTwoDataTypes(new MetadataCounterGroup(COLUMN_FAMILY));
        helperTestTwoDataTypes(new MetadataCounterGroup("FIELD_NAME", COLUMN_FAMILY));
    }

    private void helperTestTwoDataTypes(MetadataCounterGroup counters) {
        counters.addToCount(1, dataType, fieldName, date);
        counters.addToCount(1, dataType + "2", fieldName, date);
        counters.addToCount(1, dataType + "2", fieldName, date);

        assertEquals(2, counters.getEntries().size());

        Iterator<MetadataCounterGroup.Components> iterator = counters.getEntries().iterator();
        MetadataCounterGroup.Components first = iterator.next();
        assertEquals(dataType + "2", first.getDataType());
        assertEquals(2, first.getCount());

        MetadataCounterGroup.Components second = iterator.next();
        assertEquals(dataType, second.getDataType());
        assertEquals(1, second.getCount());
    }

    private void assertOneEntryWithExpectedCount(MetadataCounterGroup counters, int expectedCount) {
        assertEquals(1, counters.getEntries().size());
        assertEquals(expectedCount, getOnlyEntry(counters).getCount());
    }

    private MetadataCounterGroup.Components getOnlyEntry(MetadataCounterGroup counters) {
        return counters.getEntries().iterator().next();
    }

    @Test
    public void testCountsDownCorrectly() {
        MetadataCounterGroup counters = new MetadataCounterGroup(COLUMN_FAMILY);
        counters.addToCount(-5, dataType, fieldName, date);
        counters.addToCount(-5, dataType, fieldName, date);
        assertOneEntryWithExpectedCount(counters, -10);
    }

    @Test
    public void testCountsUpAndDownCorrectly() {
        MetadataCounterGroup counters = new MetadataCounterGroup(COLUMN_FAMILY);
        counters.addToCount(1, dataType, fieldName, date);
        counters.addToCount(1, dataType, fieldName, date);
        counters.addToCount(-1, dataType, fieldName, date);
        assertOneEntryWithExpectedCount(counters, 1);
    }

    @Test
    public void testAssignments() {
        MetadataCounterGroup counters = new MetadataCounterGroup(COLUMN_FAMILY);
        counters.addToCount(1, dataType, fieldName, date);
        MetadataCounterGroup.Components entry = getOnlyEntry(counters);
        assertEquals(dataType, entry.getDataType());
        assertEquals(fieldName, entry.getRowId());
        assertEquals(date, entry.getDate());
    }
}
