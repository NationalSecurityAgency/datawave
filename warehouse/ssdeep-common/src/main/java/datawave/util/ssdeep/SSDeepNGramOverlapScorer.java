package datawave.util.ssdeep;

import java.util.Set;

public class SSDeepNGramOverlapScorer implements SSDeepHashScorer {

    NGramGenerator generator;

    public SSDeepNGramOverlapScorer(int ngramSize, int maxRepeatedChars, int minHashSize) {
        generator = new NGramGenerator(ngramSize, maxRepeatedChars, minHashSize);
    }

    public int apply(SSDeepHash signature1, SSDeepHash signature2) {
        Set<NGramTuple> ngrams1 = generator.generateNgrams(signature1);
        Set<NGramTuple> ngrams2 = generator.generateNgrams(signature2);

        ngrams1.retainAll(ngrams2);
        return ngrams1.size();
    }
}
