package datawave.webservice.query.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import datawave.query.data.UUIDType;

@RunWith(PowerMockRunner.class)
public class TestLookupUUIDConfiguration {

    @Test
    public void testSimpleCase() {
        // Run the test
        LookupUUIDConfiguration subject = new LookupUUIDConfiguration();
        subject.setBeginDate(SimpleDateFormat.getDateInstance().format(new Date()));
        subject.setColumnVisibility("A&B");
        subject.setUuidTypes(Arrays.asList(new UUIDType()));

        // Verify results
        assertNotNull("Test subject should not be null", subject);
        assertNotNull("Begin date should not be null", subject.getBeginDate());
        assertNotNull("ColumnVisibility should not be null", subject.getColumnVisibility());
        assertNotNull("UUID types should not be null", subject.getUuidTypes());
        assertEquals("UUID types should include 1 item", 1, subject.getUuidTypes().size());
    }
}
