package datawave.util.ssdeep;

import java.util.HashSet;
import java.util.Set;

/**
 * Implements scoring between a pair of hashes based on the number of ngrams they have in common. Returns a unique set of the overlapping ngrams as a result,
 * the overlap score is calculated based on the size of this set.
 */
public class SSDeepNGramOverlapScorer implements SSDeepHashScorer<Set<NGramTuple>> {

    private final int ngramSize;

    public SSDeepNGramOverlapScorer(int ngramSize) {
        this.ngramSize = ngramSize;
    }

    /**
     * Calculate the number of ngrams shared between a part of ssdeep hashes. Considers chunk sizes along with the single chunk / double chunk of each has as
     * appropriate.
     *
     * @param signature1
     *            the first ssdeep hash for comparison
     * @param signature2
     *            the second ssdeep hash for comparison
     * @return the set of ngrams shared between each ssdeep hash, labeled with chunk size.
     */
    public Set<NGramTuple> apply(SSDeepHash signature1, SSDeepHash signature2) {
        if (signature1.getChunkSize() == signature2.getChunkSize()) {
            Set<NGramTuple> ngrams = new HashSet<>();

            ngrams.addAll(calculateOverlappingNGrams(ngramSize, signature1.getChunkSize(), signature1.getChunk(), signature2.getChunk()));

            if (signature1.hasDoubleChunk() && signature2.hasDoubleChunk()) {
                ngrams.addAll(calculateOverlappingNGrams(ngramSize, signature1.getDoubleChunkSize(), signature1.getDoubleChunk(), signature2.getDoubleChunk()));
            }

            return ngrams;
        } else if (signature1.hasDoubleChunk() && (signature1.getDoubleChunkSize() == signature2.getChunkSize())) {
            return calculateOverlappingNGrams(ngramSize, signature1.getDoubleChunkSize(), signature1.getDoubleChunk(), signature2.getChunk());
        } else if (signature2.hasDoubleChunk() && (signature1.getChunkSize() == signature2.getDoubleChunkSize())) {
            return calculateOverlappingNGrams(ngramSize, signature1.getChunkSize(), signature1.getChunk(), signature2.getDoubleChunk());
        } else {
            return new HashSet<NGramTuple>();
        }
    }

    /**
     * Calculate the number of ngrams shared amongst both chunks. Each chunk must have the same ngram size or the comparison will be invalid. For efficiency,
     * the shorter chunk will drive the comparison.
     *
     * @param ngramSize
     *            the size of ngrams to compare
     * @param chunkSize
     *            the chunk size of the ngrams we are comparing, both chunks must have the same size.
     * @param chunk1
     *            the first chunk to compare
     * @param chunk2
     *            the second chunk to compare
     * @return a set of ngrams shared between each chunk, labeled with chunk size.
     */
    private static Set<NGramTuple> calculateOverlappingNGrams(int ngramSize, int chunkSize, String chunk1, String chunk2) {
        Set<NGramTuple> overlappingNGrams = new HashSet<>();
        final String smallChunk = chunk1.length() <= chunk2.length() ? chunk1 : chunk2;
        final String largeChunk = chunk2.equals(smallChunk) ? chunk1 : chunk2;
        final int ngramCount = smallChunk.length() - ngramSize;

        for (int i = 0; i <= ngramCount; i++) {
            String ngram = smallChunk.substring(i, i + ngramSize);
            if (largeChunk.contains(ngram)) {
                overlappingNGrams.add(new NGramTuple(chunkSize, ngram));
            }
        }
        return overlappingNGrams;
    }
}
