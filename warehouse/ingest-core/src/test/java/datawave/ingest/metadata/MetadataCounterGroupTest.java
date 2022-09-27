package datawave.ingest.metadata;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

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
        
        Assertions.assertEquals(2, counters.getEntries().size());
        
        Iterator<MetadataCounterGroup.CountAndKeyComponents> iterator = counters.getEntries().iterator();
        MetadataCounterGroup.CountAndKeyComponents first = iterator.next();
        Assertions.assertEquals(dataType + "2", first.getDataType());
        Assertions.assertEquals(2, first.getCount());
        
        MetadataCounterGroup.CountAndKeyComponents second = iterator.next();
        Assertions.assertEquals(dataType, second.getDataType());
        Assertions.assertEquals(1, second.getCount());
    }
    
    private void assertOneEntryWithExpectedCount(MetadataCounterGroup counters, int expectedCount) {
        Assertions.assertEquals(1, counters.getEntries().size());
        Assertions.assertEquals(expectedCount, getOnlyEntry(counters).getCount());
    }
    
    private MetadataCounterGroup.CountAndKeyComponents getOnlyEntry(MetadataCounterGroup counters) {
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
        MetadataCounterGroup.CountAndKeyComponents entry = getOnlyEntry(counters);
        Assertions.assertEquals(dataType, entry.getDataType());
        Assertions.assertEquals(fieldName, entry.getRowId());
        Assertions.assertEquals(date, entry.getDate());
    }
}
