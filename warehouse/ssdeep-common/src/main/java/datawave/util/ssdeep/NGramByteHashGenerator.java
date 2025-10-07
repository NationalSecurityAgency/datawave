package datawave.util.ssdeep;

import java.util.Iterator;

import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Generates NGrams for the specified hash using the NGram Generator. Note: hashes may be normalized prior to n-gram-ing, but the non-normalized version of the
 * hash is emitted in the second field of the Tuple emitted by this class.
 */
public class NGramByteHashGenerator {
    final NGramGenerator nGramEngine;
    final SSDeepEncoding ssDeepEncoder;

    public NGramByteHashGenerator(int size, int maxRepeatedCharacters, int minHashSize) {
        nGramEngine = new NGramGenerator(size, maxRepeatedCharacters, minHashSize);
        ssDeepEncoder = new SSDeepEncoding();
    }

    public Iterator<ImmutablePair<NGramTuple,byte[]>> call(final String hash) {
        return nGramEngine.generateNgrams(hash).stream().map(g -> new ImmutablePair<>(g, ssDeepEncoder.encode(hash))).iterator();
    }
}
