package datawave.ingest.metadata;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

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
        
        // Need to use a junk class here because we can't make instances of MostRecentEventDateAndKeyComponents as it is
        // not an enclosing class
        Collection<Tuple> expected = Lists.newArrayList();
        expected.add(new Tuple("sham", "wow", "sham.wow.pick.up.Messes", 345L));
        expected.add(new Tuple("sham", "wow2", "sham.wow.pick.up.Messes", 124L));
        
        Collection<Tuple> actual = Lists.newArrayList();
        for (MetadataWithMostRecentDate.MostRecentEventDateAndKeyComponents m : metadata.entries()) {
            actual.add(new Tuple(m.getFieldName(), m.getDataType(), m.getNormalizerClassName(), m.getMostRecentDate()));
        }
        
        Assert.assertTrue(CollectionUtils.isEqualCollection(expected, actual));
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
    
    /**
     * To be used for expected output comparison of MetadataWithMostRecentDate.MostRecentEventDateAndKeyComponents since that class is not declared public
     * static and not an enclosing class.
     */
    public static class Tuple {
        protected final String fieldName;
        protected final String dataTypeOutputName;
        protected final String normalizerClassName;
        protected final long mostRecentDate;
        
        public Tuple(String fieldName, String dataTypeOutputName, String normalizerClassName, long mostRecentDate) {
            this.fieldName = fieldName;
            this.dataTypeOutputName = dataTypeOutputName;
            this.normalizerClassName = normalizerClassName;
            this.mostRecentDate = mostRecentDate;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Tuple tuple = (Tuple) o;
            return mostRecentDate == tuple.mostRecentDate && Objects.equal(fieldName, tuple.fieldName)
                            && Objects.equal(dataTypeOutputName, tuple.dataTypeOutputName) && Objects.equal(normalizerClassName, tuple.normalizerClassName);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(fieldName, dataTypeOutputName, normalizerClassName, mostRecentDate);
        }
    }
}
