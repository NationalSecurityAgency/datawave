package datawave.webservice.common.storage.config;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryStoragePropertiesTest {

    @Test
    public void testQueryStorageProperties() {
        QueryStorageProperties props = new QueryStorageProperties();
        props.setSynchStorage(true);
        assertTrue(props.isSynchStorage());
        props.setSynchStorage(false);
        assertFalse(props.isSynchStorage());
    }

}
