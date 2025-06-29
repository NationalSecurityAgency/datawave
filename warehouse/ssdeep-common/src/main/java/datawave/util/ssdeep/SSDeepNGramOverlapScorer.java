package datawave.util.ssdeep;

import java.util.Set;

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
        Set<NGramTuple> ngrams1 = generator.generateNgrams(signature1);
        Set<NGramTuple> ngrams2 = generator.generateNgrams(signature2);

        ngrams1.retainAll(ngrams2);
        return ngrams1;
    }
}
