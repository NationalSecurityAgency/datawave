package datawave.core.iterators;

import datawave.query.Constants;
import datawave.query.jexl.DatawaveArithmetic;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.NoOutputs;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

/**
 * 
 * An iterator for the Datawave shard table, it searches FieldIndex keys and returns Event keys (its topKey must be an Event key).
 * 
 * FieldIndex keys: fi\0{fieldName}:{fieldValue}\0datatype\0uid
 * 
 * Event key: CF, {datatype}\0{UID}
 * 
 */
public class DatawaveFieldIndexListIteratorJexl extends DatawaveFieldIndexCachingIteratorJexl {
    
    public static class Builder<B extends Builder<B>> extends DatawaveFieldIndexCachingIteratorJexl.Builder<B> {
        private FST<?> fst = null;
        // we need the values sorted for buildBoundingRanges to return sorted ranges
        private List<String> values = null;
        
        public B withFST(FST<?> fst) {
            this.fst = fst;
            return self();
        }
        
        public B withValues(Collection<String> values) {
            this.values = new ArrayList<>(values);
            return self();
        }
        
        public DatawaveFieldIndexListIteratorJexl build() {
            return new DatawaveFieldIndexListIteratorJexl(this);
        }
    }
    
    public static Builder<?> builder() {
        return new Builder();
    }
    
    protected DatawaveFieldIndexListIteratorJexl(Builder builder) {
        super(builder);
        if (builder.values != null) {
            this.values = new ArrayList<>(builder.values);
            Collections.sort(this.values);
        }
        this.fst = builder.fst;
    }
    
    private FST<?> fst = null;
    // we need the values sorted for buildBoundingRanges to return sorted ranges
    private List<String> values = null;
    
    // -------------------------------------------------------------------------
    // ------------- Constructors
    public DatawaveFieldIndexListIteratorJexl() {
        super();
    }
    
    public DatawaveFieldIndexListIteratorJexl(DatawaveFieldIndexListIteratorJexl other, IteratorEnvironment env) {
        super(other, env);
        this.values = other.values;
        this.fst = other.fst;
    }
    
    // -------------------------------------------------------------------------
    // ------------- Overrides
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new DatawaveFieldIndexListIteratorJexl(this, env);
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DatawaveFieldIndexFSTIteratorJexl{fName=").append(getFieldName()).append(", negated=").append(isNegated()).append("}");
        return builder.toString();
    }
    
    @Override
    protected List<Range> buildBoundingFiRanges(Text rowId, Text fiName, Text fieldValue) {
        if (ANY_FINAME.equals(fiName)) {
            Key startKey = new Key(rowId, FI_START);
            Key endKey = new Key(rowId, FI_END);
            return new RangeSplitter(new Range(startKey, true, endKey, false), getMaxRangeSplit());
        }
        
        if (fst != null || isNegated()) {
            Key startKey = null;
            Key endKey = null;
            startKey = new Key(rowId, fiName);
            endKey = new Key(rowId, new Text(fiName + Constants.NULL_BYTE_STRING));
            return new RangeSplitter(new Range(startKey, true, endKey, true), getMaxRangeSplit());
        } else {
            List<Range> ranges = new ArrayList<>();
            for (String value : values) {
                ranges.add(buildBoundingRange(rowId, fiName, new Text(value)));
            }
            // TODO: reduce (or expand) ranges by maxRangeSplit
            return ranges;
        }
    }
    
    /**
     * Build a single bounding range for a field value
     * 
     * @param rowId
     * @return
     */
    protected Range buildBoundingRange(Text rowId, Text fiName, Text fieldValue) {
        // construct new range
        this.boundingFiRangeStringBuilder.setLength(0);
        this.boundingFiRangeStringBuilder.append(fieldValue);
        int len = this.boundingFiRangeStringBuilder.length();
        this.boundingFiRangeStringBuilder.append(NULL_BYTE);
        Key startKey = new Key(rowId, fiName, new Text(this.boundingFiRangeStringBuilder.toString()));
        
        this.boundingFiRangeStringBuilder.setLength(len);
        this.boundingFiRangeStringBuilder.append(ONE_BYTE);
        Key endKey = new Key(rowId, fiName, new Text(this.boundingFiRangeStringBuilder.toString()));
        return new Range(startKey, true, endKey, false);
    }
    
    // -------------------------------------------------------------------------
    // ------------- Other stuff
    
    /**
     * Does this key match our FST. Note we are not overriding the super.isMatchingKey() as we need that to work as is NOTE: This method must be thread safe
     * 
     * @param k
     * @return
     * @throws IOException
     */
    @Override
    protected boolean matches(Key k) throws IOException {
        String colq = k.getColumnQualifier().toString();
        
        // search backwards for the null bytes to expose the value in value\0datatype\0UID
        int index = colq.lastIndexOf('\0');
        index = colq.lastIndexOf('\0', index - 1);
        String value = colq.substring(0, index);
        
        return (this.fst != null) ? DatawaveArithmetic.matchesFst(value, fst) : values.contains(value);
    }
    
    public static FST<?> getFST(SortedSet<String> values) throws IOException {
        final IntsRefBuilder irBuilder = new IntsRefBuilder();
        // The builder options with defaults
        FST.INPUT_TYPE inputType = FST.INPUT_TYPE.BYTE1;
        int minSuffixCount1 = 0;
        int minSuffixCount2 = 0;
        boolean doShareSuffix = true;
        boolean doShareNonSingletonNodes = true;
        int shareMaxTailLength = Integer.MAX_VALUE;
        
        boolean allowArrayArcs = true;
        int bytesPageBits = 15;
        final Outputs<Object> outputs = NoOutputs.getSingleton();
        
        // create the FST from the values
        org.apache.lucene.util.fst.Builder<Object> fstBuilder = new org.apache.lucene.util.fst.Builder<>(inputType, minSuffixCount1, minSuffixCount2,
                        doShareSuffix, doShareNonSingletonNodes, shareMaxTailLength, outputs, allowArrayArcs, bytesPageBits);
        
        for (String value : values) {
            Util.toUTF16(value, irBuilder);
            final IntsRef scratchInt = irBuilder.get();
            fstBuilder.add(scratchInt, outputs.getNoOutput());
        }
        return fstBuilder.finish();
    }
    
    /** Utility class to load one instance of any FST per classloader */
    public static class FSTManager {
        static final Map<Path,FST<Object>> fstCache = new HashMap<>();
        
        public static synchronized FST<Object> get(Path fstfile, String compressedCodec, FileSystem fs) throws IOException {
            if (fstfile == null)
                throw new NullPointerException("input fst key was null");
            FST<Object> fst = fstCache.get(fstfile);
            if (fst != null) {
                return fst;
            }
            
            // Attempt to load fst from HDFS
            fst = loadFSTFromFile(fstfile, compressedCodec, fs);
            fstCache.put(fstfile, fst);
            return fst;
        }
        
        public static FST<Object> loadFSTFromFile(Path filename, String compressionCodec, FileSystem fs) throws IOException {
            
            CompressionCodec codec = null;
            if (compressionCodec != null) {
                Class<? extends CompressionCodec> codecClass = null;
                try {
                    codecClass = Class.forName(compressionCodec).asSubclass(CompressionCodec.class);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Compression codec " + compressionCodec + " was not found.", e);
                } catch (ClassCastException e) {
                    throw new IllegalArgumentException("Compression codec " + compressionCodec + " in not a subclass of CompressionCodec.", e);
                }
                try {
                    codec = codecClass.newInstance();
                } catch (InstantiationException e) {
                    throw new IllegalArgumentException("Compression codec " + compressionCodec + " could not be instantiated.", e);
                } catch (IllegalAccessException e) {
                    throw new IllegalArgumentException("Compression codec " + compressionCodec + " could not be accessed.", e);
                }
            }
            
            InputStream fis = fs.open(filename);
            if (codec != null) {
                fis = codec.createInputStream(fis);
            }
            NoOutputs outputs = NoOutputs.getSingleton();
            DataInput di = new InputStreamDataInput(fis);
            return new FST<>(di, outputs);
        }
        
        public static synchronized void clear(String file) {
            fstCache.remove(file);
        }
        
        public static synchronized void clear() {
            fstCache.clear();
        }
    }
}
