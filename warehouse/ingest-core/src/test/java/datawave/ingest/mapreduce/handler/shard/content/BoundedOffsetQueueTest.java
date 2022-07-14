package datawave.ingest.mapreduce.handler.shard.content;

import org.junit.Assert;
import org.junit.Test;

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
            
            Assert.assertNull("AddOffset unexpectedly returned an offset list", ol);
        }
        
        String token = String.format("term-%d:zone-%d", 0, 0);
        TermAndZone taz = new TermAndZone(token);
        int count = uut.getCapacity() - 1;
        for (int offset = 0; offset < count; offset++) {
            
            BoundedOffsetQueue.OffsetList ol = uut.addOffset(taz, (offset + uut.getCapacity()));
            
            Assert.assertNotNull("AddOffset failed to return an offset list", ol);
        }
        
        BoundedOffsetQueue.OffsetList ol = uut.addOffset(taz, (2 * uut.getCapacity()));
        Assert.assertNotNull("AddOffset failed to return an offset list", ol);
        Assert.assertEquals("AddOffset returned a OffsetList with an unexpected number of offsets.", 21, ol.offsets.size());
        Assert.assertEquals("AddOffset failed to correctly update the number of elements in the Queue", 0, uut.size());
        
    }
    
    @Test
    public void testOffsetListComparatorLogic() {
        
        BoundedOffsetQueue boq = new BoundedOffsetQueue(20);
        BoundedOffsetQueue.OffsetListComparator uut = boq.new OffsetListComparator();
        String token = String.format("term-%d:zone-%d", 0, 0);
        
        try {
            
            uut.compare(token, token);
            
        } catch (IllegalArgumentException iae) {
            
            String msg = iae.getMessage();
            
            Assert.assertTrue(String.format(
                            "BoundedOffsetQueue.OffsetListComparator threw the expected exception, however it did not have the correct message: %s", msg), msg
                            .matches("Cannot compare a key that has no offsets to be found"));
        }
        
        TermAndZone taz = new TermAndZone(token);
        
        boq.addOffset(taz, 0);
        
        Assert.assertTrue("BoundedOffsetQueue.OffsetListComparator failed to match itself.", (0 == uut.compare(token, token)));
        
        String token1 = String.format("term-%d:zone-%d", 1, 1);
        taz = new TermAndZone(token1);
        
        boq.addOffset(taz, 1);
        Assert.assertTrue("BoundedOffsetQueue.OffsetListComparator failed to match another TermAndZone instance with the same number of elements.",
                        (0 == uut.compare(token, token1)));
        
        boq.addOffset(taz, 2);
        Assert.assertTrue("BoundedOffsetQueue.OffsetListComparator failed to correctly order two TermAndZone instances.", (0 > uut.compare(token, token1)));
        Assert.assertTrue("BoundedOffsetQueue.OffsetListComparator failed to correctly order two TermAndZone instances.", (0 < uut.compare(token1, token)));
    }
    
}
