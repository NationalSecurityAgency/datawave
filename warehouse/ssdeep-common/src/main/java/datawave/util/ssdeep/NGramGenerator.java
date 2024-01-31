package datawave.util.ssdeep;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

/** Generates NGrams of SSDeep Hashes for indexing or query */
public class NGramGenerator implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(NGramGenerator.class);

    final int ngramSize;
    final int maxRepeatedChars;
    final int minHashSize;

    /**
     * Generate NGrams of the specified size
     *
     * @param ngramSize
     *            the size of ngrams to generate.
     */
    public NGramGenerator(int ngramSize) {
        this(ngramSize, 0, 64);
    }

    /**
     * Generate NGrams of the specified size after normalizing to collapse repeated characters
     *
     * @param ngramSize
     *            the size of the ngrams to generate
     * @param maxRepeatedChars
     *            the max number of repeated characters - uses normalization to replace any run of repeated characters longer than this with this many
     *            characters. If zero, no normalization will be performed
     * @param minHashSize
     *            do not generate ngrams for hashes smaller than this size
     */
    public NGramGenerator(int ngramSize, int maxRepeatedChars, int minHashSize) {
        this.ngramSize = ngramSize;
        this.maxRepeatedChars = maxRepeatedChars;
        this.minHashSize = minHashSize;
    }

    public int getNgramSize() {
        return ngramSize;
    }

    public int getMaxRepeatedChars() {
        return maxRepeatedChars;
    }

    public int getMinHashSize() {
        return minHashSize;
    }

    /**
     *
     * @param queries
     *            expected to be a collection of SSDeep hashes in chunkSize:chunk:doubleChunk format
     * @return a multimap of NGramTuples mapped to the SSDeepHash from which they originated.
     */
    public Multimap<NGramTuple,SSDeepHash> preprocessQueries(Collection<SSDeepHash> queries) {
        Multimap<NGramTuple,SSDeepHash> queryMap = TreeMultimap.create();

        for (SSDeepHash queryHash : queries) {
            generateNgrams(queryHash).forEach(t -> queryMap.put(t, queryHash));
        }

        return queryMap;
    }

    /**
     * @param ssDeepHashString
     *            expected to be an SSDeep hash in chunkSize:chunk:doubleChunk format. This will normalize the hash by removing repeated characters if
     *            maxRepeatedChars is greater than zero.
     * @return a collection of NGramTuples that includes ngrams generated from both the chunk and doubleChunk portions of the input ssdeep hash. If the ssdeep
     *         can't be parsed this method will catch and log the parse exception.
     */
    public Set<NGramTuple> generateNgrams(String ssDeepHashString) {
        try {
            return generateNgrams(SSDeepHash.parseAndNormalize(ssDeepHashString, maxRepeatedChars), minHashSize);
        } catch (SSDeepParseException ex) {
            log.debug(ex.getMessage());
        }
        return Collections.emptySet();
    }

    public Set<NGramTuple> generateNgrams(SSDeepHash ssDeepHash) {
        return this.generateNgrams(ssDeepHash, 0);
    }

    /**
     *
     * @param ssDeepHash
     *            expected to be an SSDeep hash in chunkSize:chunk:doubleChunk format. Assumes that no normalization will be performed on the SSDeepHash (or it
     *            has already been performed, e.g., repeated characters have been collapsed already).
     * @param minHashSize
     *            the minimum size (chunkSize * chunkLength) required for input hashes. We will not generate ngrams for hashes smaller than this. If set to
     *            zero, we will generate ngrams for all hashes regardless of length.
     * @return a collection of NGramTuples that includes ngrams generated from both the chunk and doubleChunk portions of the ssdeep hash.
     */
    public Set<NGramTuple> generateNgrams(SSDeepHash ssDeepHash, int minHashSize) {
        final Set<NGramTuple> queryNgrams = new HashSet<>();

        final int hashSize = ssDeepHash.getChunkSize() * ssDeepHash.getChunk().length();
        if (minHashSize > 0 && hashSize < minHashSize) {
            log.debug("Skipping {}, SSDeep Hash Size {} is less than minimum {}", ssDeepHash, hashSize, minHashSize);
        } else {
            generateNgrams(ssDeepHash.getChunkSize(), ssDeepHash.getChunk(), queryNgrams);

            if (ssDeepHash.hasDoubleChunk()) {
                generateNgrams(ssDeepHash.getDoubleChunkSize(), ssDeepHash.getDoubleChunk(), queryNgrams);
            }
        }
        return queryNgrams;
    }

    /**
     * Generate SSDeep ngrams of size <code>ngramSize</code> and store them in the provided <code>output</code> collection.
     *
     * @param chunkSize
     *            the chunkSize that corresponds to the chunk for which we are generating ngrams. This will be encoded into the output ngram tuple.
     * @param chunk
     *            the chunk of the ssdeep hash for which we are generating ngrams
     * @param output
     *            a collection that is used to collect NGramTuples. These NGramTuples capture the chunk size and ngrams of the input chunk.
     */
    public void generateNgrams(int chunkSize, String chunk, Set<NGramTuple> output) {
        final int ngramCount = chunk.length() - ngramSize;
        for (int i = 0; i < ngramCount; i++) {
            final String ngram = chunk.substring(i, i + ngramSize);
            output.add(new NGramTuple(chunkSize, ngram));
        }
    }
}
