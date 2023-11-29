package datawave.ingest.data.config.ingest;

import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class CSVIngestHelperTest {
    @Test
    public void testNullField() {
        CSVIngestHelper helper = new CSVIngestHelper();
        Multimap fields = HashMultimap.create();
        helper.processExtraField(fields, null);
    }
}
