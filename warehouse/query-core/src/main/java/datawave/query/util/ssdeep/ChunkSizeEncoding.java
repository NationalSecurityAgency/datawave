package datawave.query.util.ssdeep;

import java.io.Serializable;

// @formatter:off

/** The encoder exploits the fact that there are a small number of legal chunk sizes based on the minimum chunk size.
 *  It introduces the concept of a chunkIndex, a number that is considerably smaller than the chunk size itself, and
 *  represents the magnitude of the chunkSize such that:
 * <p>
 * &lt;pre&gt;
 *  chunkSize = MIN_CHUNK_SIZE * 2^chunkIndex
 * <p>
 *  thus:
 * <p>
 *  chunkIndex = log2(chunkSize/MIN_CHUNK_SIZE)
 * &lt;/pre&gt;
 * <p>
 *  For further compression, we encode the chunkIndex as a base64 encoded string, represented using a single character
 *  from the Base64 alphabet.
 * <p>
 *  The encode/decode methods can handle chunkIndexes larger than 64 because the logic is there to handle up to three
 *  digit base64 encoded strings, but in practice due to the max possible value of the long type, the largest
 *  chunkIndex we see is 55, which maps to a chunk size of 108,086,391,056,891,904 bytes and a total file size of
 *  6,917,529,027,641,081,856 bytes
 */
//@formatter:on
public class ChunkSizeEncoding implements Serializable {
    
    private static final int MIN_CHUNK_SIZE = 3;
    private static final int DEFAULT_ENCODING_ALPHABET_LENGTH = HashReverse.LEXICAL_B64_TABLE.length;
    
    private static final int DEFAULT_ENCODING_LENGTH = 1;
    
    static final double L2 = Math.log(2);
    
    private final IntegerEncoding chunkIndexEncoding;
    
    final int minChunkSize;
    
    /**
     * Create a ChunkSizeEncoding with the default parameters of a 64 character encoding alphabet and a length of 1. This allows us to encode 64 distinct chunk
     * index values. Chunk index 0 represents the MIN_CHUNK_SIZE. See class javadocs for more info.
     */
    public ChunkSizeEncoding() {
        this(MIN_CHUNK_SIZE, DEFAULT_ENCODING_ALPHABET_LENGTH, DEFAULT_ENCODING_LENGTH);
    }
    
    public ChunkSizeEncoding(int minChunkSize, int encodingAlphabetLength, int encodingLength) {
        this.minChunkSize = minChunkSize;
        this.chunkIndexEncoding = new IntegerEncoding(encodingAlphabetLength, encodingLength);
    }
    
    public long getLimit() {
        return findChunkSizeIndex(chunkIndexEncoding.getLimit());
    }
    
    public int getLength() {
        return chunkIndexEncoding.getLength();
    }
    
    public long findNthChunkSize(int index) {
        return minChunkSize * ((long) Math.pow(2, index));
    }
    
    public int findChunkSizeIndex(long chunkSize) {
        return (int) (Math.log(chunkSize / (float) minChunkSize) / L2);
    }
    
    public String encode(int chunkSize) {
        int index = findChunkSizeIndex(chunkSize);
        return chunkIndexEncoding.encode(index);
    }
    
    public byte[] encodeToBytes(int chunkSize, byte[] buffer, int offset) {
        int index = findChunkSizeIndex(chunkSize);
        return chunkIndexEncoding.encodeToBytes(index, buffer, offset);
    }
    
    public int decode(String encoded) {
        int index = chunkIndexEncoding.decode(encoded);
        return (int) findNthChunkSize(index);
    }
    
    public int decode(byte[] encoded, int offset) {
        int index = chunkIndexEncoding.decode(encoded, offset);
        return (int) findNthChunkSize(index);
    }
}
