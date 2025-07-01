package datawave.util.ssdeep;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Multimap;

/**
 * Implements scoring between a pair of hashes based on the number of ngrams they have in common. Returns a unique set of the overlapping ngrams as a result,
 * the overlap score is calculated based on the size of this set.
 */
public class SSDeepNGramOverlapScorer implements SSDeepHashScorer<Set<NGramTuple>> {

    NGramGenerator generator;

    public SSDeepNGramOverlapScorer(int ngramSize, int maxRepeatedChars, int minHashSize) {
        generator = new NGramGenerator(ngramSize, maxRepeatedChars, minHashSize);
    }

    public Set<NGramTuple> apply(SSDeepHash signature1, SSDeepHash signature2) {
        if (signature1.getChunkSize() == signature2.getChunkSize()) {
            Set<NGramTuple> ngrams = new HashSet<>();

            ngrams.addAll(generator.calculateOverlappingNGrams(signature1.getChunkSize(), signature1.getChunk(), signature2.getChunk()));

            if (signature1.hasDoubleChunk() && signature2.hasDoubleChunk()) {
                ngrams.addAll(generator.calculateOverlappingNGrams(signature1.getDoubleChunkSize(), signature1.getDoubleChunk(), signature2.getDoubleChunk()));
            }

            return ngrams;
        } else if (signature1.hasDoubleChunk() && (signature1.getDoubleChunkSize() == signature2.getChunkSize())) {
            return generator.calculateOverlappingNGrams(signature1.getDoubleChunkSize(), signature1.getDoubleChunk(), signature2.getChunk());
        } else if (signature2.hasDoubleChunk() && (signature1.getChunkSize() == signature2.getDoubleChunkSize())) {
            return generator.calculateOverlappingNGrams(signature1.getChunkSize(), signature1.getChunk(), signature2.getDoubleChunk());
        } else {
            return new HashSet<NGramTuple>();
        }
    }
}
