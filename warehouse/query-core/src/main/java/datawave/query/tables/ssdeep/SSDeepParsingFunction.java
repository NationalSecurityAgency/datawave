package datawave.query.tables.ssdeep;

import java.util.AbstractMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.util.ssdeep.ChunkSizeEncoding;
import datawave.util.ssdeep.IntegerEncoding;
import datawave.util.ssdeep.NGramTuple;
import datawave.util.ssdeep.SSDeepHash;

public class SSDeepParsingFunction implements Function<Map.Entry<Key,Value>,Map.Entry<NGramTuple,SSDeepHash>> {
    /** Used to encode the chunk size as a character which is included in the ranges used to retrieve ngram tuples */
    private final ChunkSizeEncoding chunkSizeEncoding;

    /** Used to encode buckets as characters which are prepended to the ranges used to retrieve ngram tuples */
    private final IntegerEncoding bucketEncoder;

    /**
     * the position where the ngram will start in the generated ranges, determined at construction time based on the bucketEncoder parameters
     */
    private final int chunkStart;

    /**
     * the position where the query ngram will end in the generate ranges, determined based on the bucketEncoder and chunkSizeEncoding parameters
     */

    private final int chunkEnd;

    public SSDeepParsingFunction(SSDeepSimilarityQueryConfiguration config) {
        this.chunkSizeEncoding = new ChunkSizeEncoding();
        this.bucketEncoder = new IntegerEncoding(config.getBucketEncodingBase(), config.getBucketEncodingLength());

        this.chunkStart = bucketEncoder.getLength();
        this.chunkEnd = chunkStart + chunkSizeEncoding.getLength();
    }

    @Override
    public Map.Entry<NGramTuple,SSDeepHash> apply(Map.Entry<Key,Value> entry) {
        // We will receive entries like the following that follow the SSDeep bucket index format:
        // +++//thPkK 3:3:yionv//thPkKlDtn/rXScG2/uDlhl2UE9FQEul/lldDpZflsup:6v/lhPkKlDtt/6TIPFQEqRDpZ+up []
        // see: https://github.com/NationalSecurityAgency/datawave/wiki/SSDeep-In-Datawave#ssdeep-table-structure-bucketed
        // generally, the rowid consists of a:
        // - bucket; first two characters based on a hash of the original ssdeep
        // - chunk; third character,
        // - ngram; the rest of the rowid.
        // the column family holds the chunk size.
        // the column qualifier holds the original ssdeep hash from which the ngram was extracted.
        final Key k = entry.getKey();
        final String row = k.getRow().toString();

        // strip off the bucketing to extract the matching ngram and chunk size from the rowId.
        int chunkSize = chunkSizeEncoding.decode(row.substring(chunkStart, chunkEnd));
        String ngram = row.substring(chunkEnd);

        // extract the matching ssdeep hash from the column qualifier
        final String matchingHashString = k.getColumnQualifier().toString();
        final SSDeepHash matchingHash = SSDeepHash.parse(matchingHashString);

        final NGramTuple matchingNgram = new NGramTuple(chunkSize, ngram);
        return new AbstractMap.SimpleEntry<>(matchingNgram, matchingHash);
    }
}
