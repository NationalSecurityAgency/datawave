package datawave.util.ssdeep;

import com.google.common.collect.Multimap;

import java.util.Set;

/**
 * Implements scoring between a pair of hashes based on the number of ngrams they have in common. Returns a unique set of the overlapping ngrams as a result,
 * the overlap score is calculated based on the size of this set.
 */
public class SSDeepNGramOverlapScorer implements SSDeepHashScorer<Set<NGramTuple>> {

    NGramGenerator generator;

    Multimap<SSDeepHash,NGramTuple> queryHashNGramMap;

    public SSDeepNGramOverlapScorer(int ngramSize, int maxRepeatedChars, int minHashSize, Multimap<SSDeepHash,NGramTuple> queryHashNGramMap) {
        generator = new NGramGenerator(ngramSize, maxRepeatedChars, minHashSize);

        this.queryHashNGramMap = queryHashNGramMap;
    }

    public Set<NGramTuple> apply(SSDeepHash signature1, SSDeepHash signature2) {
        Set<NGramTuple> ngrams1;

        if (queryHashNGramMap != null) {
            ngrams1 = (Set<NGramTuple>)queryHashNGramMap.get(signature1);
        }
        else {
            ngrams1 = generator.generateNgrams(signature1);
        }

        // evaluation code goes here

        //ngrams1.retainAll(ngrams2);
        return ngrams1;
    }
}
