package datawave.iterators.filter;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.metadata.protobuf.EdgeMetadata.MetadataValue;
import datawave.metadata.protobuf.EdgeMetadata.MetadataValue.Metadata;

public class EdgeMetadataCQStripperCombiner extends WrappingIterator {
    private static final Logger log = Logger.getLogger(EdgeMetadataCQStripperCombiner.class);
    
    Key topKey;
    Value topValue;
    
    @Override
    public Key getTopKey() {
        if (topKey == null)
            return super.getTopKey();
        return topKey;
    }
    
    @Override
    public Value getTopValue() {
        if (topKey == null)
            return super.getTopValue();
        return topValue;
    }
    
    @Override
    public boolean hasTop() {
        return topKey != null || super.hasTop();
    }
    
    @Override
    public void next() throws IOException {
        if (topKey != null) {
            topKey = null;
            topValue = null;
        } else {
            super.next();
        }
        
        findTop();
    }
    
    private Key workKey = new Key();
    
    private void findTop() throws IOException {
        // check if aggregation is needed
        if (super.hasTop()) {
            workKey.set(super.getTopKey());
            if (workKey.getColumnFamily().equals(datawave.data.ColumnFamilyConstants.COLF_EDGE)) {
                if (workKey.isDeleted()) {
                    return;
                }
                topKey = workKey;
                ValueIterator viter = new ValueIterator(getSource());
                topValue = reduce(topKey, viter);
                while (viter.hasNext()) {
                    viter.next();
                }
                topKey.setTimestamp(viter.getMaxTimeStamp());
                
            }
        }
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // do not want to seek to the middle of a value that should be combined...
        
        Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
        
        super.seek(seekRange, columnFamilies, inclusive);
        findTop();
        
        if (range.getStartKey() != null) {
            while (hasTop() && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
                            && getTopKey().getTimestamp() > range.getStartKey().getTimestamp()) {
                // the value has a more recent time stamp, so pass it up
                next();
            }
            
            while (hasTop() && range.beforeStartKey(getTopKey())) {
                next();
            }
        }
    }
    
    public Value reduce(Key key, Iterator<Value> iter) {
        
        // Since similar metadata values can have different date strings we only want to keep one
        // version of each piece of metadata along with its earliest start date.
        Map<Metadata,String> metadataFields = new TreeMap<>(new MetadataComparator());
        
        // possibly check that the key has "edge" as the cf?
        String earliestDate = null;
        
        while (iter.hasNext()) {
            
            Value value = iter.next();
            MetadataValue metadataVal;
            try {
                metadataVal = MetadataValue.parseFrom(value.get());
            } catch (InvalidProtocolBufferException e) {
                log.error("Found invalid Edge Metadata Value bytes.");
                continue;
            }
            
            List<Metadata> metadata = metadataVal.getMetadataList();
            
            // Go through and store the metadata with its earliest start date
            for (Metadata meta : metadata) {
                
                String tempDate = meta.getDate();
                
                if (!metadataFields.containsKey(meta)) {
                    metadataFields.put(meta, meta.getDate());
                } else if (metadataFields.get(meta).compareTo(meta.getDate()) > 0) {
                    metadataFields.put(meta, meta.getDate());
                }
                
            }
            
        }
        
        // Afterwards insert the earliest start date into its respective metadata object and add it to
        // the MetadataValueBuilder.
        Metadata.Builder metaDatabuilder = Metadata.newBuilder();
        MetadataValue.Builder builder = MetadataValue.newBuilder();
        for (Map.Entry<Metadata,String> metaEntry : metadataFields.entrySet()) {
            metaDatabuilder.clear();
            Metadata metadata = metaEntry.getKey();
            String date = metaEntry.getValue();
            
            metaDatabuilder.mergeFrom(metadata);
            metaDatabuilder.setDate(date);
            
            builder.addMetadata(metaDatabuilder);
        }
        
        return new Value(builder.build().toByteArray());
    }
    
    /*
     * Only compare the first 4 fields in the metedata Want to use this to match other metadata objects with the same fields but different dates
     */
    private class MetadataComparator implements Comparator<Metadata> {
        @Override
        public int compare(Metadata m1, Metadata m2) {
            if (m1.getSource().equals(m2.getSource()) && m1.getSink().equals(m2.getSink()) && m1.getEnrichment().equals(m2.getEnrichment())
                            && m1.getEnrichmentIndex().equals(m2.getEnrichmentIndex())) {
                return 0;
            } else {
                return 1;
            }
        }
    }
    
    /*
     * Used to iterator over values belonging to similar keys, but the keys are not guaranteed to be ordered correctly by their timestamp
     */
    public static class ValueIterator implements Iterator<Value> {
        Key topKey;
        SortedKeyValueIterator<Key,Value> source;
        boolean hasNext;
        long maxTimeStamp;
        
        /**
         * Constructs an iterator over Values whose Keys are versions of the current topKey of the source SortedKeyValueIterator.
         *
         * @param source
         *            The {@code SortedKeyValueIterator<Key,Value>} from which to read data.
         */
        public ValueIterator(SortedKeyValueIterator<Key,Value> source) {
            this.source = source;
            topKey = new Key(source.getTopKey());
            hasNext = _hasNext();
            maxTimeStamp = topKey.getTimestamp();
        }
        
        private boolean _hasNext() {
            return source.hasTop() && !source.getTopKey().isDeleted() && topKey.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
        }
        
        @Override
        public boolean hasNext() {
            return hasNext;
        }
        
        @Override
        public Value next() {
            if (!hasNext)
                throw new NoSuchElementException();
            Value topValue = new Value(source.getTopValue());
            
            // Keep a running total of the most recent timestamp as that is the one that should be used in the combined key/value entry
            if (source.getTopKey().getTimestamp() > maxTimeStamp) {
                maxTimeStamp = source.getTopKey().getTimestamp();
            }
            
            try {
                source.next();
                hasNext = _hasNext();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return topValue;
        }
        
        /**
         * This method is unsupported in this iterator.
         *
         * @throws UnsupportedOperationException
         *             when called
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        public long getMaxTimeStamp() {
            return maxTimeStamp;
        }
    }
    
}
