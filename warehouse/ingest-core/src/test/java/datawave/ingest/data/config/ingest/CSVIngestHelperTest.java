package datawave.ingest.data.config.ingest;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Test;

public class CSVIngestHelperTest {
    @Test
    public void testNullField() {
        CSVIngestHelper helper = new CSVIngestHelper();
        Multimap fields = HashMultimap.create();
        helper.processExtraField(fields, null);
    }
}
