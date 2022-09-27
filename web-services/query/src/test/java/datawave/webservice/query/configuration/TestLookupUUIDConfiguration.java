package datawave.webservice.query.configuration;

import datawave.query.data.UUIDType;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestLookupUUIDConfiguration {
    
    @Test
    public void testSimpleCase() {
        // Run the test
        LookupUUIDConfiguration subject = new LookupUUIDConfiguration();
        subject.setBeginDate(SimpleDateFormat.getDateInstance().format(new Date()));
        subject.setColumnVisibility("A&B");
        subject.setUuidTypes(Arrays.asList(new UUIDType()));
        
        // Verify results
        assertNotNull(subject, "Test subject should not be null");
        assertNotNull(subject.getBeginDate(), "Begin date should not be null");
        assertNotNull(subject.getColumnVisibility(), "ColumnVisibility should not be null");
        assertNotNull(subject.getUuidTypes(), "UUID types should not be null");
        assertEquals(1, subject.getUuidTypes().size(), "UUID types should include 1 item");
    }
}
