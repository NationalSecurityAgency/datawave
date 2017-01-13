package nsa.datawave.webservice.query.configuration;

import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import nsa.datawave.query.data.UUIDType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class TestLookupUUIDConfiguration {
    @Test
    public void testSimpleCase() throws Exception {
        // Run the test
        LookupUUIDConfiguration subject = new LookupUUIDConfiguration();
        subject.setBeginDate(SimpleDateFormat.getDateInstance().format(new Date()));
        subject.setColumnVisibility("A&B");
        subject.setUuidTypes(Arrays.asList(new UUIDType()));
        
        // Verify results
        assertTrue("Test subject should not be null", null != subject);
        assertTrue("Begin date should not be null", null != subject.getBeginDate());
        assertTrue("ColumnVisibility should not be null", null != subject.getColumnVisibility());
        assertTrue("UUID types should not be null", null != subject.getUuidTypes());
        assertTrue("UUID types should include 1 item", subject.getUuidTypes().size() == 1);
    }
}
