package datawave.ingest.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ResourceAvailabilityUtilTest {
    
    @Test
    public void testIsDiskAvailable_DefaultPath() {
        // Create test input
        float reasonableMinAvailability = 0.0f;
        float unreasonableMinAvailability = 1.01f;
        
        // Run the test
        boolean result1 = ResourceAvailabilityUtil.isDiskAvailable("/", reasonableMinAvailability);
        boolean result2 = ResourceAvailabilityUtil.isDiskAvailable(unreasonableMinAvailability);
        
        // Verify results
        assertTrue(reasonableMinAvailability + "% minimum disk space should always be available", result1);
        
        assertTrue(unreasonableMinAvailability + "% minimum disk space should never be available", !result2);
    }
    
    @Test
    public void testIsMemoryAvailable_DefaultPath() {
        // Create test input
        float reasonableMinAvailability = 0.0f;
        float unreasonableMinAvailability = 1.01f;
        
        // Run the test
        boolean result1 = ResourceAvailabilityUtil.isMemoryAvailable(reasonableMinAvailability);
        boolean result2 = ResourceAvailabilityUtil.isMemoryAvailable(unreasonableMinAvailability);
        
        // Verify results
        assertTrue(reasonableMinAvailability + "% minimum memory should always be available", result1);
        
        assertTrue(unreasonableMinAvailability + "% minimum memory should never be available", !result2);
    }
}
