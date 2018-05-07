package datawave.ingest.mapreduce.handler.shard.content;

import java.util.*;

public class BoundedOffsetQueue<T> implements OffsetQueue<T> {
    private static final long serialVersionUID = 452499360525244451L;
    
    public static class OffsetList<T> {
        public TermAndZone termAndZone;
        public List<T> offsets;
        
        public int size() {
            return offsets.size();
        }
    }
    
    // The offsets
    HashMap<String,List<T>> offsetsMap;
    
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
     * @see datawave.ingest.mapreduce.handler.shard.OffsetQueue#addOffset(datawave.ingest.mapreduce.handler.shard.TermAndZone, T)
     */
    @Override
    public OffsetList addOffset(TermAndZone termAndZone, T offset) {
        String key = termAndZone.getToken();
        queue.remove(key);
        
        List<T> offsets = offsetsMap.remove(key);
        if (null == offsets) {
            offsets = new ArrayList<>();
        }
        offsets.add(offset);
        
        offsetsMap.put(key, offsets);
        queue.add(key);
        
        numOffsets++;
        if (numOffsets > maxNumOffsets) {
            OffsetList<T> list = new OffsetList<>();
            String token = queue.remove();
            list.offsets = offsetsMap.remove(token);
            numOffsets -= list.offsets.size();
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
    public List<T> getOffsets(TermAndZone termAndZone) {
        return offsetsMap.get(termAndZone.getToken());
    }
    
    @Override
    public boolean containsKey(TermAndZone termAndZone) {
        return offsetsMap.containsKey(termAndZone.getToken());
    }
    
    @Override
    public Iterable<OffsetList<T>> offsets() {
        return () -> {
            final Iterator<Map.Entry<String,List<T>>> entries = offsetsMap.entrySet().iterator();
            final OffsetList<T> offsets = new OffsetList<>();
            return new Iterator<OffsetList<T>>() {
                @Override
                public boolean hasNext() {
                    return entries.hasNext();
                }
                
                @Override
                public OffsetList next() {
                    Map.Entry<String,List<T>> entry = entries.next();
                    offsets.offsets = entry.getValue();
                    offsets.termAndZone = new TermAndZone(entry.getKey());
                    return offsets;
                }
                
                @Override
                public void remove() {
                    entries.remove();
                }
            };
        };
    }
    
    public class OffsetListComparator implements Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
            return getSize(o1) - getSize(o2);
        }
        
        private int getSize(String o) {
            List<T> offsetList = offsetsMap.get(o);
            if (offsetList == null) {
                throw new IllegalArgumentException("Cannot compare a key that has no offsets to be found");
            }
            return offsetList.size();
        }
        
    }
}
