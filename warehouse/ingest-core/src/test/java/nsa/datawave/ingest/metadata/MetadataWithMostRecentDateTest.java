package nsa.datawave.ingest.metadata;

import org.apache.hadoop.io.Text;
import org.junit.*;

import java.util.Iterator;

public class MetadataWithMostRecentDateTest {
    
    public static final Text COLUMN_FAMILY = new Text("yuuuup");
    private long date = 1234567890;
    private String fieldName = "sham";
    private String dataTypeName = "wow";
    private String normalizerClassName = "sham.wow.pick.up.Messes";
    
    @Test
    public void testNoReplacement() {
        MetadataWithMostRecentDate counters = new MetadataWithMostRecentDate(COLUMN_FAMILY);
        counters.createOrUpdate(fieldName, dataTypeName, normalizerClassName, date);
        counters.createOrUpdate(fieldName, dataTypeName, normalizerClassName, 123L);
        assertOneEntryWithExpectedDate(counters, date);
    }
    
    @Test
    public void testReplacement() {
        MetadataWithMostRecentDate counters = new MetadataWithMostRecentDate(COLUMN_FAMILY);
        counters.createOrUpdate(fieldName, dataTypeName, normalizerClassName, 123L);
        counters.createOrUpdate(fieldName, dataTypeName, normalizerClassName, date);
        assertOneEntryWithExpectedDate(counters, date);
    }
    
    @Test
    public void testCanTrackSeparately() {
        MetadataWithMostRecentDate metadata = new MetadataWithMostRecentDate(COLUMN_FAMILY);
        metadata.createOrUpdate(fieldName, dataTypeName, normalizerClassName, 345);
        metadata.createOrUpdate(fieldName, dataTypeName + "2", normalizerClassName, 123);
        metadata.createOrUpdate(fieldName, dataTypeName + "2", normalizerClassName, 124);
        
        Assert.assertEquals(2, metadata.entries().size());
        
        Iterator<MetadataWithMostRecentDate.MostRecentEventDateAndKeyComponents> iterator = metadata.entries().iterator();
        MetadataWithMostRecentDate.MostRecentEventDateAndKeyComponents first = iterator.next();
        Assert.assertEquals(dataTypeName, first.getDataType());
        Assert.assertEquals(345, first.getMostRecentDate());
        
        MetadataWithMostRecentDate.MostRecentEventDateAndKeyComponents second = iterator.next();
        Assert.assertEquals(dataTypeName + "2", second.getDataType());
        Assert.assertEquals(124, second.getMostRecentDate());
    }
    
    private void assertOneEntryWithExpectedDate(MetadataWithMostRecentDate counters, long expectedDate) {
        Assert.assertEquals(1, counters.entries().size());
        Assert.assertEquals(expectedDate, getOnlyEntry(counters).getMostRecentDate());
    }
    
    private MetadataWithMostRecentDate.MostRecentEventDateAndKeyComponents getOnlyEntry(MetadataWithMostRecentDate counters) {
        return counters.entries().iterator().next();
    }
    
    @Test
    public void testAssignments() {
        MetadataWithMostRecentDate counters = new MetadataWithMostRecentDate(COLUMN_FAMILY);
        counters.createOrUpdate(fieldName, dataTypeName, normalizerClassName, date);
        MetadataWithMostRecentDate.MostRecentEventDateAndKeyComponents entry = getOnlyEntry(counters);
        Assert.assertEquals(fieldName, entry.getFieldName());
        Assert.assertEquals(dataTypeName, entry.getDataType());
        Assert.assertEquals(normalizerClassName, entry.getNormalizerClassName());
        Assert.assertEquals(date, entry.getMostRecentDate());
    }
}
