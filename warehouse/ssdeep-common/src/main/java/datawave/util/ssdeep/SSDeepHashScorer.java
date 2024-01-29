package datawave.util.ssdeep;

import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.log4j.Logger;

/** Implements functions to calculate a similarity score for a pair of SSDeepHashes */
public class SSDeepHashScorer {
    private static final Logger log = Logger.getLogger(SSDeepHash.class);

    private final int maxRepeatedCharacters;

    public SSDeepHashScorer() {
        this(SSDeepHash.DEFAULT_MAX_REPEATED_CHARACTERS);
    }

    public SSDeepHashScorer(int maxRepeatedCharacters) {
        this.maxRepeatedCharacters = maxRepeatedCharacters;
    }

    /**
     * Compare two ssdeep hashes, returning a score between 0 to 100 that indicates similarity. A score of 0 means that the items are not similar at all whereas
     * a score of 100 indicates a high degree of similarity.
     *
     * @param signature1
     *            the first object to be compared.
     * @param signature2
     *            the second object to be compared.
     * @return an integer between 0 and 100
     */
    public int apply(SSDeepHash signature1, SSDeepHash signature2) {
        if ((null == signature1) || (null == signature2)) {
            return -1;
        }
        final long chunkSize1 = signature1.getChunkSize();
        final long chunkSize2 = signature2.getChunkSize();

        // We require the chunk size to either be equal, or for one to be twice the other. If the chunk sizes don't
        // match then we are comparing apples to oranges. This isn't an 'error' per se. We could have two valid
        // ssdeep hashes, but with chunk sizes so different they can't be compared.
        if ((chunkSize1 != chunkSize2) && (chunkSize1 != (chunkSize2 * 2)) && (chunkSize2 != (chunkSize1 * 2))) {
            if (log.isDebugEnabled()) {
                log.debug("block sizes too different: " + chunkSize1 + " " + chunkSize2);
            }
            return 0;
        }

        // There is very little information content in sequences of the same character like 'LLLLL'. Eliminate any
        // sequences longer than MAX_REPEATED_CHARACTERS (3).
        final String s1chunk = SSDeepHash.normalizeSSDeepChunk(signature1.getChunk(), maxRepeatedCharacters);
        final String s1doubleChunk = SSDeepHash.normalizeSSDeepChunk(signature1.getDoubleChunk(), maxRepeatedCharacters);
        final String s2chunk = SSDeepHash.normalizeSSDeepChunk(signature2.getChunk(), maxRepeatedCharacters);
        final String s2doubleChunk = SSDeepHash.normalizeSSDeepChunk(signature2.getDoubleChunk(), maxRepeatedCharacters);

        // Each ssdeep has two chunks with different chunk sizes. Choose which ones to use from each hash for scoring.
        final long score;
        if (chunkSize1 == chunkSize2) {
            // The ssdeep chunk sizes are equal.
            final long score1 = scoreChunks(s1chunk, s2chunk, chunkSize1);
            final long score2 = scoreChunks(s1doubleChunk, s2doubleChunk, chunkSize2);
            score = Math.max(score1, score2);
        } else if (chunkSize1 == (chunkSize2 * 2)) {
            // The first ssdeep has twice the chunk size of the second.
            score = scoreChunks(s1chunk, s2doubleChunk, chunkSize1);
        } else {
            // The second ssdeep has twice the chunk size of the first.
            score = scoreChunks(s1doubleChunk, s2chunk, chunkSize2);
        }

        return (int) score;
    }

    /**
     * This is the low level chunk scoring algorithm. It takes two chunks and scores them on a scale of 0-100 where 0 is a terrible match and 100 is a great
     * match. The chunkSize is used to cope with very small messages.
     */
    private static int scoreChunks(final String s1, final String s2, final long chunkSize) {
        final int len1 = s1.length();
        final int len2 = s2.length();

        if ((len1 > SSDeepHash.CHUNK_LENGTH) || (len2 > SSDeepHash.CHUNK_LENGTH)) {
            // one of the chunk lengths exceeds the max chunk length, perhaps it is not a real ssdeep?
            return 0;
        }

        // Compute the edit distance between the two chunk strings. The edit distance gives us a pretty good idea of
        // how closely related the two chunks are.
        int editDistance = LevenshteinDistance.getDefaultInstance().apply(s1, s2);
        if (log.isDebugEnabled()) {
            log.debug("edit_dist: " + editDistance);
        }

        // Scale the edit distance by the lengths of the two chunks. This changes the baseScore to be a measure of the
        // proportion of the message that has changed rather than an absolute quantity. It also copes with the
        // variability of the chunk string lengths.
        int score = (editDistance * SSDeepHash.CHUNK_LENGTH) / (len1 + len2);

        // At this stage the baseScore occurs roughly on a 0-64 scale,
        // with 0 being a good match and 64 being a complete mismatch.

        // Rescale to a 0-100 scale (friendlier to humans).
        score = (100 * score) / SSDeepHash.CHUNK_LENGTH;

        // It is possible to get a baseScore above 100 here, but it is a really terrible match.
        if (score >= 100) {
            return 0;
        }

        // Invert the score with 0 being a poor match and 100 being a excellent match.
        score = 100 - score;

        // When the chunk size is small we don't want to exaggerate the match.
        final int threshold = (int) (chunkSize / SSDeepHash.MIN_CHUNK_SIZE * Math.min(len1, len2));
        if (score > threshold) {
            score = threshold;
        }

        return score;
    }
}
