package datawave.query.util.ssdeep;

import java.io.Serializable;
import java.util.Objects;

/**
 * Embodies a Base64 encoded SSDeep hash chunk, and an accompanying chunk size for that hash. Per the SSDeep specification, each character in the hash chunk
 * corresponds to a set of bytes of <code>chunkSize</code> in the original binary object.
 *
 * Practically, this can be used to store either an entire SSDEEP hash chunk or a substring/ngram of that chunk.
 */
public class NGramTuple implements Serializable, Comparable<NGramTuple> {

    public static final String CHUNK_DELIMITER = ":";

    final int chunkSize;
    final String chunk;

    public NGramTuple(int chunkSize, String chunk) {
        this.chunk = chunk;
        this.chunkSize = chunkSize;
    }

    public static NGramTuple parse(String tuple) {
        int pos = tuple.indexOf(CHUNK_DELIMITER);
        String chunkSizeString = tuple.substring(0, pos);
        String chunk = tuple.substring(pos + 1);
        int chunkSize = Integer.parseInt(chunkSizeString);
        return new NGramTuple(chunkSize, chunk);
    }

    public String getChunk() {
        return chunk;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public String toString() {
        return String.join(":", String.valueOf(chunkSize), chunk);
    }

    @Override
    public int compareTo(NGramTuple o) {
        int cmp = Integer.compare(this.chunkSize, o.chunkSize);
        if (cmp == 0) {
            return this.chunk.compareTo(o.chunk);
        } else {
            return cmp;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof NGramTuple))
            return false;
        NGramTuple that = (NGramTuple) o;
        return chunkSize == that.chunkSize && chunk.equals(that.chunk);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunk, chunkSize);
    }
}
