package nsa.datawave.ingest.mapreduce.handler.shard.content;

import nsa.datawave.ingest.mapreduce.handler.shard.content.BoundedOffsetQueue.OffsetList;

public interface OffsetQueue {
    
    /**
     * Add an offset. If this addition pushes the queue past its limit, then the smallest offset list entry is removed and returned.
     * 
     * @param termAndZone
     * @param offset
     * @return The removed overflow entry. Null if the queue is not full yet.
     */
    public OffsetList addOffset(TermAndZone termAndZone, int offset);
    
    public void clear();
    
    public boolean containsKey(TermAndZone termAndZone);
    
    public Iterable<OffsetList> offsets();
    
    public int[] getOffsets(TermAndZone termAndZone);
    
    public int size();
}
