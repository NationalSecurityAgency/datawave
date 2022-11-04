package datawave.ingest.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertTrue(result1, reasonableMinAvailability + "% minimum disk space should always be available");
        
        assertTrue(!result2, unreasonableMinAvailability + "% minimum disk space should never be available");
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
        assertTrue(result1, reasonableMinAvailability + "% minimum memory should always be available");
        
        assertTrue(!result2, unreasonableMinAvailability + "% minimum memory should never be available");
    }
}
