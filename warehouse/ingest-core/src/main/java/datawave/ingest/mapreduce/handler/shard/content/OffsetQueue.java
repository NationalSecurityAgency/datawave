package datawave.ingest.mapreduce.handler.shard.content;

import datawave.ingest.mapreduce.handler.shard.content.BoundedOffsetQueue.OffsetList;

import java.util.List;

public interface OffsetQueue<T> {
    
    /**
     * Add an offset. If this addition pushes the queue past its limit, then the smallest offset list entry is removed and returned.
     * 
     * @param termAndZone
     * @param offset
     * @return The removed overflow entry. Null if the queue is not full yet.
     */
    OffsetList<T> addOffset(TermAndZone termAndZone, T offset);
    
    void clear();
    
    boolean containsKey(TermAndZone termAndZone);
    
    Iterable<OffsetList<T>> offsets();
    
    List<T> getOffsets(TermAndZone termAndZone);
    
    int size();
}
