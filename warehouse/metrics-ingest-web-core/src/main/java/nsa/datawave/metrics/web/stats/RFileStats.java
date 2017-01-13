package nsa.datawave.metrics.web.stats;

import org.apache.accumulo.core.file.rfile.bcfile.Utils;
import org.apache.accumulo.core.file.rfile.bcfile.Utils.Version;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public final class RFileStats {
    // the current version of BCFile impl, increment them (major or minor) made
    // enough changes
    static final Version API_VERSION = new Version((short) 1, (short) 0);
    static final Log LOG = LogFactory.getLog(RFileStats.class);
    
    /**
     * Prevent the instantiation of BCFile objects.
     */
    private RFileStats() {
        // nothing
    }
    
    /**
     * BCFile Reader, interface to read the file's data and meta blocks.
     */
    static public class StatsBuilder implements Closeable {
        private static final String META_NAME = "BCFile.metaindex";
        private final FSDataInputStream in;
        final DataIndex dataIndex = null;
        // Index for meta blocks
        final MetaIndex metaIndex;
        final Version version;
        private BlockRegion bcFileIndexRegion;
        
        /**
         * Constructor
         * 
         * @param fin
         *            FS input stream.
         * @param fileLength
         *            Length of the corresponding file
         * @throws Exception
         */
        public StatsBuilder(FSDataInputStream fin, long fileLength) throws Exception {
            this.in = fin;
            
            // move the cursor to the beginning of the tail, containing: offset to the
            // meta block index, version and magic
            fin.seek(fileLength - Magic.size() - Version.size() - Long.SIZE / Byte.SIZE);
            long offsetIndexMeta = fin.readLong();
            version = new Version(fin);
            Magic.readAndVerify(fin);
            
            if (!version.compatibleWith(RFileStats.API_VERSION)) {
                throw new RuntimeException("Incompatible BCFile fileBCFileVersion.");
            }
            
            // read meta index
            fin.seek(offsetIndexMeta);
            metaIndex = new MetaIndex(fin);
            
            // read data:BCFile.index, the data block index
            bcFileIndexRegion = getMetaBlockRegion("RFile.index");
            
        }
        
        public long getRFileIndexCompressedSize() {
            return bcFileIndexRegion.getCompressedSize();
        }
        
        public long getRFileIndexRawSize() {
            return bcFileIndexRegion.getRawSize();
        }
        
        /**
         * Stream access to a Meta Block.
         * 
         * @param name
         *            meta block name
         * @return BlockReader input stream for reading the meta block.
         * @throws Exception
         */
        public BlockRegion getMetaBlockRegion(String name) throws Exception {
            MetaIndexEntry imeBCIndex = metaIndex.getMetaByName(name);
            if (imeBCIndex == null) {
                throw new Exception("MetaBlockDoesNotExist: " + name);
            }
            
            return imeBCIndex.getRegion();
        }
        
        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub
            
        }
        
    }
    
    /**
     * Index for all Meta blocks.
     */
    static class MetaIndex {
        // use a tree map, for getting a meta block entry by name
        final Map<String,MetaIndexEntry> index;
        
        // for write
        public MetaIndex() {
            index = new TreeMap<>();
        }
        
        // for read, construct the map from the file
        public MetaIndex(DataInput in) throws IOException {
            int count = Utils.readVInt(in);
            index = new TreeMap<>();
            
            for (int nx = 0; nx < count; nx++) {
                MetaIndexEntry indexEntry = new MetaIndexEntry(in);
                index.put(indexEntry.getMetaName(), indexEntry);
            }
        }
        
        public void addEntry(MetaIndexEntry indexEntry) {
            index.put(indexEntry.getMetaName(), indexEntry);
        }
        
        public MetaIndexEntry getMetaByName(String name) {
            return index.get(name);
        }
        
        public void write(DataOutput out) throws IOException {
            Utils.writeVInt(out, index.size());
            
            for (MetaIndexEntry indexEntry : index.values()) {
                indexEntry.write(out);
            }
        }
    }
    
    /**
     * An entry describes a meta block in the MetaIndex.
     */
    static final class MetaIndexEntry {
        private final String metaName;
        private final String compressionAlgorithm;
        private final static String defaultPrefix = "data:";
        
        private final BlockRegion region;
        
        public MetaIndexEntry(DataInput in) throws IOException {
            String fullMetaName = Utils.readString(in);
            if (fullMetaName.startsWith(defaultPrefix)) {
                metaName = fullMetaName.substring(defaultPrefix.length(), fullMetaName.length());
            } else {
                throw new IOException("Corrupted Meta region Index");
            }
            
            compressionAlgorithm = Utils.readString(in);
            region = new BlockRegion(in);
        }
        
        public String getMetaName() {
            return metaName;
        }
        
        public String getCompressionAlgorithm() {
            return compressionAlgorithm;
        }
        
        public BlockRegion getRegion() {
            return region;
        }
        
        public void write(DataOutput out) throws IOException {
            throw new IOException("Not implemented");
        }
    }
    
    /**
     * Index of all compressed data blocks.
     */
    static class DataIndex {
        final static String BLOCK_NAME = "BCFile.index";
        
        private final String defaultCompressionAlgorithm;
        
        // for data blocks, each entry specifies a block's offset, compressed size
        // and raw size
        private final ArrayList<BlockRegion> listRegions;
        
        private boolean trackBlocks;
        
        // for read, deserialized from a file
        public DataIndex(DataInput in) throws IOException {
            defaultCompressionAlgorithm = Utils.readString(in);
            
            int n = Utils.readVInt(in);
            listRegions = new ArrayList<>(n);
            
            for (int i = 0; i < n; i++) {
                BlockRegion region = new BlockRegion(in);
                listRegions.add(region);
            }
        }
        
        // for write
        public DataIndex(String defaultCompressionAlgorithmName, boolean trackBlocks) {
            this.trackBlocks = trackBlocks;
            this.defaultCompressionAlgorithm = defaultCompressionAlgorithmName;
            listRegions = new ArrayList<>();
        }
        
        public String getDefaultCompressionAlgorithm() {
            return defaultCompressionAlgorithm;
        }
        
        public ArrayList<BlockRegion> getBlockRegionList() {
            return listRegions;
        }
        
        public void addBlockRegion(BlockRegion region) {
            if (trackBlocks)
                listRegions.add(region);
        }
        
        public void write(DataOutput out) throws IOException {
            throw new IOException("not implemented");
        }
    }
    
    /**
     * Magic number uniquely identifying a BCFile in the header/footer.
     */
    static final class Magic {
        private final static byte[] AB_MAGIC_BCFILE = {
                // ... total of 16 bytes
                (byte) 0xd1, (byte) 0x11, (byte) 0xd3, (byte) 0x68, (byte) 0x91, (byte) 0xb5, (byte) 0xd7, (byte) 0xb6, (byte) 0x39, (byte) 0xdf, (byte) 0x41,
                (byte) 0x40, (byte) 0x92, (byte) 0xba, (byte) 0xe1, (byte) 0x50};
        
        public static void readAndVerify(DataInput in) throws IOException {
            byte[] abMagic = new byte[size()];
            in.readFully(abMagic);
            
            // check against AB_MAGIC_BCFILE, if not matching, throw an
            // Exception
            if (!Arrays.equals(abMagic, AB_MAGIC_BCFILE)) {
                throw new IOException("Not a valid BCFile.");
            }
        }
        
        public static void write(DataOutput out) throws IOException {
            out.write(AB_MAGIC_BCFILE);
        }
        
        public static int size() {
            return AB_MAGIC_BCFILE.length;
        }
    }
    
    /**
     * Block region.
     */
    static final class BlockRegion {
        private final long offset;
        private final long compressedSize;
        private final long rawSize;
        
        public BlockRegion(DataInput in) throws IOException {
            offset = Utils.readVLong(in);
            compressedSize = Utils.readVLong(in);
            rawSize = Utils.readVLong(in);
        }
        
        public BlockRegion(long offset, long compressedSize, long rawSize) {
            this.offset = offset;
            this.compressedSize = compressedSize;
            this.rawSize = rawSize;
        }
        
        public void write(DataOutput out) throws IOException {
            Utils.writeVLong(out, offset);
            Utils.writeVLong(out, compressedSize);
            Utils.writeVLong(out, rawSize);
        }
        
        public long getOffset() {
            return offset;
        }
        
        public long getCompressedSize() {
            return compressedSize;
        }
        
        public long getRawSize() {
            return rawSize;
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        Path p = new Path("hdfs://localhost:9000/accumulo/tables/2/b-000008x/I0000093.rf");
        Configuration conf = new Configuration();
        FileSystem fs = p.getFileSystem(conf);
        FSDataInputStream s = fs.open(p);
        StatsBuilder rd = new StatsBuilder(s, fs.getFileStatus(p).getLen());
        System.out.println(rd.getRFileIndexCompressedSize());
        System.out.println(rd.getRFileIndexRawSize());
    }
}
