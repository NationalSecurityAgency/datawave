package datawave.ingest.mapreduce.handler.shard.content;

import java.util.Arrays;
import java.util.Iterator;

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
    public void testEvictionRemovesTheSmallestEntry() {

        BoundedOffsetQueue<Integer> uut = new BoundedOffsetQueue<>(6);
        TermAndZone big = new TermAndZone("big", "ZONE");
        TermAndZone small = new TermAndZone("small", "ZONE");

        for (int offset = 0; offset < 4; offset++) {
            Assert.assertNull(uut.addOffset(big, offset));
        }
        for (int offset = 0; offset < 2; offset++) {
            Assert.assertNull(uut.addOffset(small, offset));
        }

        // one more offset passes the bound, and the two offset entry is the one that goes
        BoundedOffsetQueue.OffsetList<Integer> evicted = uut.addOffset(big, 4);

        Assert.assertNotNull("passing the bound should have evicted an entry", evicted);
        Assert.assertEquals(small, evicted.termAndZone);
        Assert.assertEquals(2, evicted.offsets.size());
        Assert.assertEquals("the evicted offsets should no longer be counted", 5, uut.size());
    }

    @Test
    public void testEvictionBreaksTiesInInsertionOrder() {

        BoundedOffsetQueue<Integer> uut = new BoundedOffsetQueue<>(3);
        TermAndZone first = new TermAndZone("first", "ZONE");

        Assert.assertNull(uut.addOffset(first, 0));
        Assert.assertNull(uut.addOffset(new TermAndZone("second", "ZONE"), 0));
        Assert.assertNull(uut.addOffset(new TermAndZone("third", "ZONE"), 0));

        BoundedOffsetQueue.OffsetList<Integer> evicted = uut.addOffset(new TermAndZone("fourth", "ZONE"), 0);

        Assert.assertNotNull("passing the bound should have evicted an entry", evicted);
        Assert.assertEquals("the oldest of the equally small entries should go first", first, evicted.termAndZone);
    }

    @Test
    public void testEntriesAreKeyedByTermAndZoneRatherThanByToken() {

        BoundedOffsetQueue<Integer> uut = new BoundedOffsetQueue<>(20);
        TermAndZone colonInZone = new TermAndZone("a", "b:c");
        TermAndZone colonInTerm = new TermAndZone("a:b", "c");

        Assert.assertEquals("the two are only distinguishable before concatenation", colonInZone.getToken(), colonInTerm.getToken());

        uut.addOffset(colonInZone, 0);

        Assert.assertTrue(uut.containsKey(colonInZone));
        Assert.assertFalse("distinct term and zone pairs must not collide", uut.containsKey(colonInTerm));

        uut.addOffset(colonInTerm, 1);

        Assert.assertEquals(Arrays.asList(0), uut.getOffsets(colonInZone));
        Assert.assertEquals(Arrays.asList(1), uut.getOffsets(colonInTerm));
    }

    @Test
    public void testOffsetsPreserveTheTermAndZone() {

        BoundedOffsetQueue<Integer> uut = new BoundedOffsetQueue<>(20);
        uut.addOffset(new TermAndZone("00:1a:2b:3c:4d:5e", "MAC_ADDRESS"), 7);
        uut.addOffset(new TermAndZone("00:1a:2b:3c:4d:5e", "MAC_ADDRESS"), 9);

        int entries = 0;
        for (BoundedOffsetQueue.OffsetList<Integer> offsets : uut.offsets()) {
            Assert.assertEquals("00:1a:2b:3c:4d:5e", offsets.termAndZone.term);
            Assert.assertEquals("MAC_ADDRESS", offsets.termAndZone.zone);
            Assert.assertEquals(Arrays.asList(7, 9), offsets.offsets);
            entries++;
        }

        Assert.assertEquals("both offsets belong to one entry", 1, entries);
    }

    @Test
    public void testIteratorRemoveKeepsTheQueueConsistent() {

        BoundedOffsetQueue<Integer> uut = new BoundedOffsetQueue<>(20);
        TermAndZone keep = new TermAndZone("keep", "ZONE");
        TermAndZone drop = new TermAndZone("drop", "ZONE");

        uut.addOffset(keep, 0);
        uut.addOffset(keep, 1);
        uut.addOffset(drop, 2);
        Assert.assertEquals(3, uut.size());

        Iterator<BoundedOffsetQueue.OffsetList<Integer>> iterator = uut.offsets().iterator();
        while (iterator.hasNext()) {
            if (drop.equals(iterator.next().termAndZone)) {
                iterator.remove();
            }
        }

        Assert.assertEquals("removing through the iterator should discount its offsets", 2, uut.size());
        Assert.assertFalse(uut.containsKey(drop));

        // an entry left behind in the size index would be chosen as the smallest and evicted with no offsets to return
        BoundedOffsetQueue.OffsetList<Integer> evicted = null;
        for (int offset = 0; evicted == null && offset < 25; offset++) {
            evicted = uut.addOffset(new TermAndZone("filler" + offset, "ZONE"), offset);
        }

        Assert.assertNotNull("the queue should have passed its bound", evicted);
        Assert.assertNotEquals(drop, evicted.termAndZone);
    }
}
