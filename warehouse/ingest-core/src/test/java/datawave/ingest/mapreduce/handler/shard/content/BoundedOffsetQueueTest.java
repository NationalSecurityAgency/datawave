package datawave.ingest.mapreduce.handler.shard.content;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * 
 */
public class BoundedOffsetQueueTest {
    
    @Test
    public void testAddOffset() {
        
        BoundedOffsetQueue uut = new BoundedOffsetQueue(20);
        
        for (int offset = 0; offset < uut.getCapacity(); offset++) {
            
            String token = String.format("term-%d:zone-%d", offset, offset);
            
            TermAndZone taz = new TermAndZone(token);
            
            BoundedOffsetQueue.OffsetList ol = uut.addOffset(taz, offset);
            
            Assertions.assertNull(ol, "AddOffset unexpectedly returned an offset list");
        }
        
        String token = String.format("term-%d:zone-%d", 0, 0);
        TermAndZone taz = new TermAndZone(token);
        int count = uut.getCapacity() - 1;
        for (int offset = 0; offset < count; offset++) {
            
            BoundedOffsetQueue.OffsetList ol = uut.addOffset(taz, (offset + uut.getCapacity()));
            
            Assertions.assertNotNull(ol, "AddOffset failed to return an offset list");
        }
        
        BoundedOffsetQueue.OffsetList ol = uut.addOffset(taz, (2 * uut.getCapacity()));
        Assertions.assertNotNull(ol, "AddOffset failed to return an offset list");
        Assertions.assertEquals(21, ol.offsets.size(), "AddOffset returned a OffsetList with an unexpected number of offsets.");
        Assertions.assertEquals(0, uut.size(), "AddOffset failed to correctly update the number of elements in the Queue");
        
    }
    
    @Test
    public void testOffsetListComparatorLogic() {
        
        BoundedOffsetQueue boq = new BoundedOffsetQueue(20);
        BoundedOffsetQueue.OffsetListComparator uut = boq.new OffsetListComparator();
        String token = String.format("term-%d:zone-%d", 0, 0);
        
        IllegalArgumentException iae = Assertions.assertThrows(IllegalArgumentException.class, () -> uut.compare(token, token));
        Assertions.assertEquals("Cannot compare a key that has no offsets to be found", iae.getMessage(), String.format(
                        "BoundedOffsetQueue.OffsetListComparator threw the expected exception, however it did not have the correct message: %s",
                        iae.getMessage()));
        
        TermAndZone taz = new TermAndZone(token);
        
        boq.addOffset(taz, 0);
        
        Assertions.assertTrue((0 == uut.compare(token, token)), "BoundedOffsetQueue.OffsetListComparator failed to match itself.");
        
        String token1 = String.format("term-%d:zone-%d", 1, 1);
        taz = new TermAndZone(token1);
        
        boq.addOffset(taz, 1);
        Assertions.assertTrue((0 == uut.compare(token, token1)),
                        "BoundedOffsetQueue.OffsetListComparator failed to match another TermAndZone instance with the same number of elements.");
        
        boq.addOffset(taz, 2);
        Assertions.assertTrue((0 > uut.compare(token, token1)), "BoundedOffsetQueue.OffsetListComparator failed to correctly order two TermAndZone instances.");
        Assertions.assertTrue((0 < uut.compare(token1, token)), "BoundedOffsetQueue.OffsetListComparator failed to correctly order two TermAndZone instances.");
    }
    
}
