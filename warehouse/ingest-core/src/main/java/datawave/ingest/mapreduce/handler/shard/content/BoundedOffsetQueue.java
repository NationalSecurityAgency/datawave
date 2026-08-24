package datawave.ingest.mapreduce.handler.shard.content;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * An offset queue bounded by a total number of offsets, evicting the entry with the fewest offsets once that bound is passed. Ties are broken in insertion
 * order.
 *
 * @param <T>
 *            the offset type
 */
public class BoundedOffsetQueue<T> implements OffsetQueue<T> {
    private static final long serialVersionUID = 452499360525244451L;

    public static class OffsetList<T> {
        public TermAndZone termAndZone;
        public List<T> offsets;

        public int size() {
            return offsets.size();
        }
    }

    // The offsets, keyed by the term and zone itself rather than by a concatenated token, which would have to be built on every
    // lookup and taken apart again on the way out
    HashMap<TermAndZone,List<T>> offsetsMap;

    // The keys of offsetsMap grouped by the size of their offset list, ordered so that the smallest group is the first entry
    TreeMap<Integer,LinkedHashSet<TermAndZone>> keysBySize;

    // The max size
    private int maxNumOffsets;

    // The current size of the offset map in terms of offsets (i.e. not in terms of keys)
    private int numOffsets = 0;

    /**
     * Create a bounded offset queue
     *
     * @param maxNumOffsets
     *            the max size of offsets
     */
    public BoundedOffsetQueue(int maxNumOffsets) {
        this.maxNumOffsets = maxNumOffsets;
        this.keysBySize = new TreeMap<>();
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
    public OffsetList<T> addOffset(TermAndZone termAndZone, T offset) {
        List<T> offsets = offsetsMap.get(termAndZone);
        if (null == offsets) {
            offsets = new ArrayList<>();
            offsetsMap.put(termAndZone, offsets);
        } else {
            unindex(termAndZone, offsets.size());
        }
        offsets.add(offset);
        index(termAndZone, offsets.size());

        numOffsets++;
        if (numOffsets > maxNumOffsets) {
            return removeSmallest();
        } else {
            return null;
        }
    }

    private void index(TermAndZone termAndZone, int size) {
        keysBySize.computeIfAbsent(size, s -> new LinkedHashSet<>()).add(termAndZone);
    }

    private void unindex(TermAndZone termAndZone, int size) {
        LinkedHashSet<TermAndZone> keys = keysBySize.get(size);
        if (keys != null && keys.remove(termAndZone) && keys.isEmpty()) {
            keysBySize.remove(size);
        }
    }

    /**
     * Remove and return the entry with the fewest offsets, breaking ties in insertion order.
     *
     * @return the removed entry
     */
    private OffsetList<T> removeSmallest() {
        Map.Entry<Integer,LinkedHashSet<TermAndZone>> smallest = keysBySize.firstEntry();
        Iterator<TermAndZone> keys = smallest.getValue().iterator();
        TermAndZone termAndZone = keys.next();
        keys.remove();
        if (smallest.getValue().isEmpty()) {
            keysBySize.remove(smallest.getKey());
        }

        OffsetList<T> list = new OffsetList<>();
        list.termAndZone = termAndZone;
        list.offsets = offsetsMap.remove(termAndZone);
        numOffsets -= list.offsets.size();
        return list;
    }

    @Override
    public void clear() {
        keysBySize.clear();
        offsetsMap.clear();
        numOffsets = 0;
    }

    @Override
    public List<T> getOffsets(TermAndZone termAndZone) {
        return offsetsMap.get(termAndZone);
    }

    @Override
    public boolean containsKey(TermAndZone termAndZone) {
        return offsetsMap.containsKey(termAndZone);
    }

    @Override
    public Iterable<OffsetList<T>> offsets() {
        return () -> {
            final Iterator<Map.Entry<TermAndZone,List<T>>> entries = offsetsMap.entrySet().iterator();
            final OffsetList<T> offsets = new OffsetList<>();
            return new Iterator<OffsetList<T>>() {
                @Override
                public boolean hasNext() {
                    return entries.hasNext();
                }

                @Override
                public OffsetList<T> next() {
                    Map.Entry<TermAndZone,List<T>> entry = entries.next();
                    offsets.offsets = entry.getValue();
                    offsets.termAndZone = entry.getKey();
                    return offsets;
                }

                @Override
                public void remove() {
                    entries.remove();
                    unindex(offsets.termAndZone, offsets.offsets.size());
                    numOffsets -= offsets.offsets.size();
                }
            };
        };
    }
}
