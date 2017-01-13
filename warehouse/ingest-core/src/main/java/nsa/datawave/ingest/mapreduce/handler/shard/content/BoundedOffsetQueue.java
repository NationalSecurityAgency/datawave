package nsa.datawave.ingest.mapreduce.handler.shard.content;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

public class BoundedOffsetQueue implements OffsetQueue {
    private static final long serialVersionUID = 452499360525244451L;
    
    public static class OffsetList {
        public TermAndZone termAndZone;
        public int[] offsets;
    }
    
    // The offsets
    HashMap<String,int[]> offsetsMap;
    
    // The priority queue
    PriorityQueue<String> queue;
    
    // The max size
    private int maxNumOffsets;
    
    // The current size of the offset map in terms of offsets (i.e. not in terms of keys)
    private int numOffsets = 0;
    
    /**
     * Create a bounded offset queue
     */
    public BoundedOffsetQueue(int maxNumOffsets) {
        OffsetListComparator comparator = new OffsetListComparator();
        this.maxNumOffsets = maxNumOffsets;
        this.queue = new PriorityQueue<>(maxNumOffsets / 10, comparator);
        this.offsetsMap = new HashMap<>(maxNumOffsets / 10);
    }
    
    public int size() {
        return this.numOffsets;
    }
    
    public int getCapacity() {
        return this.maxNumOffsets;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.ingest.mapreduce.handler.shard.OffsetQueue#addOffset(nsa.datawave.ingest.mapreduce.handler.shard.TermAndZone, int)
     */
    @Override
    public OffsetList addOffset(TermAndZone termAndZone, int offset) {
        String key = termAndZone.getToken();
        queue.remove(key);
        int[] offsets = offsetsMap.remove(key);
        if (offsets != null) {
            offsets = Arrays.copyOf(offsets, offsets.length + 1);
            offsets[offsets.length - 1] = offset;
        } else {
            offsets = new int[] {offset};
        }
        
        offsetsMap.put(key, offsets);
        queue.add(key);
        
        numOffsets++;
        if (numOffsets > maxNumOffsets) {
            OffsetList list = new OffsetList();
            String token = queue.remove();
            list.offsets = offsetsMap.remove(token);
            numOffsets -= list.offsets.length;
            list.termAndZone = new TermAndZone(token);
            return list;
        } else {
            return null;
        }
    }
    
    @Override
    public void clear() {
        queue.clear();
        offsetsMap.clear();
        numOffsets = 0;
    }
    
    @Override
    public int[] getOffsets(TermAndZone termAndZone) {
        return offsetsMap.get(termAndZone.getToken());
    }
    
    @Override
    public boolean containsKey(TermAndZone termAndZone) {
        return offsetsMap.containsKey(termAndZone.getToken());
    }
    
    @Override
    public Iterable<OffsetList> offsets() {
        return new Iterable<OffsetList>() {
            @Override
            public Iterator<OffsetList> iterator() {
                final Iterator<Map.Entry<String,int[]>> entries = offsetsMap.entrySet().iterator();
                final OffsetList offsets = new OffsetList();
                return new Iterator<OffsetList>() {
                    
                    @Override
                    public boolean hasNext() {
                        return entries.hasNext();
                    }
                    
                    @Override
                    public OffsetList next() {
                        Map.Entry<String,int[]> entry = entries.next();
                        offsets.offsets = entry.getValue();
                        offsets.termAndZone = new TermAndZone(entry.getKey());
                        return offsets;
                    }
                    
                    @Override
                    public void remove() {
                        entries.remove();
                    }
                };
            }
        };
    }
    
    public class OffsetListComparator implements Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
            return getSize(o1) - getSize(o2);
        }
        
        private int getSize(String o) {
            int[] offsetList = offsetsMap.get(o);
            if (offsetList == null) {
                throw new IllegalArgumentException("Cannot compare a key that has no offsets to be found");
            }
            return offsetList.length;
        }
        
    }
}
