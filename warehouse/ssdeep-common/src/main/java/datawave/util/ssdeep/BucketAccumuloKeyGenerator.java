package datawave.util.ssdeep;

import java.util.Arrays;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Transforms NGram/SSDeep Pairs to Accumulo Key/Values. The approach toward generating rowIds produces prefixes for each indexed ngram that include a 'bucket'
 * and an encoded version of the chunkSize, thus allowing independent portions of the index to be scanned in parallel. A bucket is an abstract concept that
 * serves as a partitioning mechanism for the indexed ngram data.
 * <p>
 * The bucket is chosen based on the hashCode of the original SSDeep bytes. As a result all ngrams from the same ssdeep hash will appear in the same bucket, but
 * identical ngrams may be scattered across multiple buckets. As a result, it is important that a query strategy considers all buckets.
 * <p>
 * In addition to encoding the bucket at the start of the rowId for the keys generated by this class, the rowId also encodes the chunk size of the ngram in the
 * rowId immediately after the bucket prefix appears, within any given bucket, a query strategy can limit its ranges based on the desired buckets.
 * <p>
 * After the encoded bucket and chunk size, the actual ngram from the ssdeep hash (as obtained from the input NGramTuple), is appended to the rowId in the keys
 * generated. As such, the keys generated by this class are in the form: (bucket) + (encoded chunk size) + (ssdeep ngram). A bucket is typically 2 characters,
 * An encoded chunk size is 1 character and a ssdeep ngram is based on the chosen ngram length (7 by default).
 */
public class BucketAccumuloKeyGenerator {

    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final Value EMPTY_VALUE = new Value();

    public static final int DEFAULT_BUCKET_COUNT = 32;

    /** The number of characters in the bucket encoding alphabet */
    public static final int DEFAULT_BUCKET_ENCODING_BASE = 32;
    /** The length of the bucket encoding we will perform */
    public static final int DEFAULT_BUCKET_ENCODING_LENGTH = 2;

    /** The maximum number of buckets we will partition data into */
    final int bucketCount;
    /** Used to encode the bucket id into a string of characters in a constrained alphabet that will go ito the rowId */
    final IntegerEncoding bucketEncoding;
    /** Used to encode a chunk size into a string of characters in a constrained alphabet that will go into the rowId */
    final ChunkSizeEncoding chunkEncoding;
    /** Used to encode the ngram bytes into a string of characters in a constrained alphabet */
    final SSDeepEncoding ngramEncoding;
    /** The timestamp to use for the generated key */
    final long timestamp = 0;

    /**
     * Creates a BucketAccumuloKeyGenerator with the specified bucket count and encoding properties
     *
     * @param bucketCount
     *            the number of index buckets (partitions) that will be used.
     * @param bucketEncodingBase
     *            the size of the alphabet that will be used to encode the index bucket number in the key.
     * @param bucketEncodingLength
     *            the number of characters that will be used to encode the index bucket number.
     */
    public BucketAccumuloKeyGenerator(int bucketCount, int bucketEncodingBase, int bucketEncodingLength) {
        this.bucketCount = bucketCount;
        this.bucketEncoding = new IntegerEncoding(bucketEncodingBase, bucketEncodingLength);
        this.chunkEncoding = new ChunkSizeEncoding();
        this.ngramEncoding = new SSDeepEncoding();

        if (bucketCount > bucketEncoding.getLimit()) {
            throw new IllegalArgumentException("Integer encoding limit is  " + bucketEncoding.getLimit() + " but bucket count was larger: " + bucketCount);
        }
    }

    /**
     * Given a (ngram / ssdeep byte) tuple produce an Accumulo Key/Value pair. The rowId is formed based on the structure discussed in this class' javadoc, the
     * column family is the integer chunk size and the column qualifier is the original ssdeep hash bytes. The generated value is always empty.
     *
     * @param t
     * @return
     */
    public ImmutablePair<Key,Value> call(ImmutablePair<NGramTuple,byte[]> t) {
        int rowSize = t.getKey().getChunk().length() + bucketEncoding.getLength() + chunkEncoding.getLength();
        final byte[] row = new byte[rowSize];
        int pos = 0;

        // encode and write the bucket
        final int bucket = Math.abs(Arrays.hashCode(t.getValue()) % bucketCount);
        bucketEncoding.encodeToBytes(bucket, row, pos);
        pos += bucketEncoding.getLength();

        // encode and write the chunk size
        chunkEncoding.encodeToBytes(t.getKey().getChunkSize(), row, pos);
        pos += chunkEncoding.getLength();

        // encode and write the ngram
        ngramEncoding.encodeToBytes(t.getKey().getChunk(), row, pos);

        final byte[] cf = IntegerEncoding.encodeBaseTenDigitBytes(t.getKey().getChunkSize());
        final byte[] cq = t.getValue();
        return new ImmutablePair<>(new Key(row, cf, cq, EMPTY_BYTES, timestamp, false, false), EMPTY_VALUE);
    }
}
