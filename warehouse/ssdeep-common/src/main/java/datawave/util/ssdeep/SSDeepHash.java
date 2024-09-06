package datawave.util.ssdeep;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/** An Immutable SSDeepHash object */
public final class SSDeepHash implements Serializable, Comparable<SSDeepHash> {

    /**
     * The paper on Optimizing ssDeep for use at Scale" suggested that any hash with more than this many repeated characters should have that run of characters
     * reduced to this number of characters and suggested that 3 was a good number. This works well for our purposes as well.
     */
    public static final int DEFAULT_MAX_REPEATED_CHARACTERS = 3;

    public static final int MIN_CHUNK_SIZE = 3;
    public static final int CHUNK_MULTIPLE = 3;
    public static final String CHUNK_DELIMITER = ":";
    public static final int CHUNK_LENGTH = 64;
    public static final int DOUBLE_CHUNK_LENGTH = 32;

    final int chunkSize;
    final String chunk;
    final boolean hasDoubleChunk;
    final String doubleChunk;

    public SSDeepHash(int chunkSize, String chunk, String doubleChunk) {
        if (chunkSize < MIN_CHUNK_SIZE) {
            throw new IllegalArgumentException("chunkSize was " + chunkSize + " but must no less than " + MIN_CHUNK_SIZE);
        } else if (chunkSize % CHUNK_MULTIPLE != 0) {
            throw new IllegalArgumentException("chunkSize was " + chunkSize + " but must be a multiple of three that is a power of 2");
        } else if (Integer.bitCount(chunkSize / CHUNK_MULTIPLE) != 1) {
            throw new IllegalArgumentException("chunkSize (" + chunkSize + ") / " + CHUNK_MULTIPLE + " must be a power of 2");
        } else if (chunk.length() > CHUNK_LENGTH) {
            throw new IllegalArgumentException("chunk length must be less than " + CHUNK_LENGTH);
        }

        if (doubleChunk.isEmpty()) {
            this.hasDoubleChunk = false;
        } else if (doubleChunk.length() > DOUBLE_CHUNK_LENGTH) {
            throw new IllegalArgumentException("double chunk length must be less than " + DOUBLE_CHUNK_LENGTH);
        } else {
            this.hasDoubleChunk = true;
        }

        // TODO: We can make additional assertions, e.g.: that the chunk and doubleChunk are base64 encoded.

        this.chunkSize = chunkSize;
        this.chunk = chunk;
        this.doubleChunk = doubleChunk;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public String getChunk() {
        return chunk;
    }

    public boolean hasDoubleChunk() {
        return hasDoubleChunk;
    }

    public int getDoubleChunkSize() {
        return hasDoubleChunk ? chunkSize * 2 : 0;
    }

    public String getDoubleChunk() {
        return doubleChunk;
    }

    @Override
    public int compareTo(SSDeepHash o) {
        int cmp = chunkSize - o.chunkSize;
        if (cmp == 0) {
            cmp = chunk.compareTo(o.chunk);
        }
        if (cmp == 0) {
            cmp = doubleChunk.compareTo(o.doubleChunk);
        }
        return cmp;
    }

    public void serialize(DataOutput oos) throws IOException {
        oos.writeUTF(toString());
    }

    public static SSDeepHash deserialize(DataInput ois) throws IOException {
        return SSDeepHash.parse(ois.readUTF());
    }

    /*
     * TODO: remove unused methods public static byte[] serialize(SSDeepHash hash) { final ByteArrayOutputStream bos = new ByteArrayOutputStream(); try
     * (ObjectOutputStream oos = new ObjectOutputStream(bos)) { hash.serialize(oos);
     *
     * } catch (IOException ioe) { log.error("Exception serializing postings", ioe); } return bos.toByteArray(); }
     *
     * public static SSDeepHash deserialize(byte[] ssDeepHashBytes) { final ByteArrayInputStream bis = new ByteArrayInputStream(ssDeepHashBytes); try
     * (ObjectInputStream ois = new ObjectInputStream(bis)) { return SSDeepHash.deserialize(ois); } catch (IOException ioe) {
     * log.error("Exception deserializing ssdeep hash", ioe); } return null; }
     */

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof SSDeepHash))
            return false;
        SSDeepHash that = (SSDeepHash) o;
        return chunkSize == that.chunkSize && chunk.equals(that.chunk) && doubleChunk.equals(that.doubleChunk);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunkSize, chunk, doubleChunk);
    }

    @Override
    public String toString() {
        return String.join(CHUNK_DELIMITER, String.valueOf(chunkSize), chunk, doubleChunk);
    }

    /**
     * Parse a string of chunkSize:chunk:doubleChunk into a SSDeepHash. doubleChunk is optional and if it does not exist, we will use an empty String in its
     * place.
     *
     * @param ssdeepHash
     *            the string to parse
     * @return the constructed ssdeep hash object.
     * @throws SSDeepParseException
     *             if the ssdeepHash string is not in the expected format.
     */
    public static SSDeepHash parse(String ssdeepHash) throws SSDeepParseException {
        final String[] parts = ssdeepHash.split(CHUNK_DELIMITER); // possible NPE.
        if (parts.length < 2 || parts.length > 3) {
            throw new SSDeepParseException("Could not parse SSDeepHash, expected 2 or 3 '" + CHUNK_DELIMITER + "'-delimited segments observed " + parts.length,
                            ssdeepHash);
        }

        try {
            final String doubleChunk = (parts.length == 3) ? parts[2] : "";
            return new SSDeepHash(Integer.parseInt(parts[0]), parts[1], doubleChunk);
        } catch (NumberFormatException nfe) {
            throw new SSDeepParseException("Could not parse SSDeepHash, expected first segment to be an integer", ssdeepHash);
        }
    }

    public static SSDeepHash normalize(final SSDeepHash input) {
        return SSDeepHash.normalize(input, DEFAULT_MAX_REPEATED_CHARACTERS);
    }

    /**
     * Normalize each chunk in an SSDeepHash by removing strings of repeated characters and replacing them with a string of the same characters that is
     * maxRepeatedCharacters in length. This reduces useless variation that consumes space in the SSDeepHashes. If the string contains no runs of repeated
     * characters longer than maxRepeatedCharacters, the original SSDeepHash is returned.
     *
     * @param input
     *            the SSDeepHash to normalize.
     * @param maxRepeatedCharacters
     *            the maximum number of repeated characters
     * @return a new SSDeepHash with normalized chunks or the original SSDeepHash if no changes were made.
     */
    public static SSDeepHash normalize(final SSDeepHash input, int maxRepeatedCharacters) {
        final String n1 = normalizeSSDeepChunk(input.getChunk(), maxRepeatedCharacters);
        final String n2 = normalizeSSDeepChunk(input.getDoubleChunk(), maxRepeatedCharacters);
        // we really do want '==' here, not equals. neither chunk is changed, so just return the input.
        if (n1 == input.getChunk() && (n2 == input.getDoubleChunk())) {
            return input;
        }
        return new SSDeepHash(input.getChunkSize(), n1 == null ? input.getChunk() : n1, n2 == null ? input.getDoubleChunk() : n2);
    }

    public SSDeepHash normalize(int maxRepeatedCharacters) {
        return normalize(this, maxRepeatedCharacters);
    }

    /**
     * Given a string that potentially contains long runs of repeating characters, replace such runs with at most maxRepeated characters. If the string is not
     * modified, return the input string.
     *
     * @param input
     *            the string to analyze and possibly modify.
     * @param maxRepeatedCharacters
     *            the number of maxRepeatedCharacters to allow. Any String that has a run of more than this many of the same character will have that run
     *            collapsed to be this many characters in length. Zero indicates that no normalization should be performed.
     * @return the modified string or the original string if the string is not modified.
     */
    public static String normalizeSSDeepChunk(final String input, final int maxRepeatedCharacters) {
        if (maxRepeatedCharacters <= 0) {
            return input; // do nothing.
        }
        final char[] data = input.toCharArray();
        final int length = data.length;

        int repeatedCharacters = 1; // number of consecutive characters observed
        int sourceIndex = 0;
        int destIndex = 0;

        // visit each position of the source string tracking runs of consecutive characters in
        // 'consecutiveChars'.
        for (; sourceIndex < length; sourceIndex++) {
            if (sourceIndex < (length - 1) && data[sourceIndex] == data[sourceIndex + 1]) {
                repeatedCharacters++;
            } else {
                repeatedCharacters = 1; // reset consecutive character counter.
            }

            // if we see more than maxConsecutiveChars consecutive characters, we will
            // skip them. Otherwise, we leave the data alone. If we have skipped characters
            // we need to copy them subsequent characters to a new position.
            if (repeatedCharacters <= maxRepeatedCharacters) {
                if (destIndex < sourceIndex) {
                    data[destIndex] = data[sourceIndex];
                }
                destIndex++;
            }
        }

        // if we have modified the data, create and return a string otherwise, return the input unchanged
        if (destIndex < length) {
            return new String(data, 0, destIndex);
        } else {
            return input;
        }
    }

    public static SSDeepHash parseAndNormalize(String ssdeepHash, int maxRepeatedChars) throws SSDeepParseException {
        final String[] parts = ssdeepHash.split(CHUNK_DELIMITER); // possible NPE.
        if (parts.length < 2 || parts.length > 3) {
            throw new SSDeepParseException("Could not parse SSDeepHash, expected 2 or 3 '" + CHUNK_DELIMITER + "'-delimited segments observed " + parts.length,
                            ssdeepHash);
        }

        try {
            final int chunkSize = Integer.parseInt(parts[0]);
            final String chunk = parts[1];
            final String doubleChunk = (parts.length == 3) ? parts[2] : "";

            final String chunkNorm = normalizeSSDeepChunk(chunk, maxRepeatedChars);
            final String doubleChunkNorm = normalizeSSDeepChunk(doubleChunk, maxRepeatedChars);

            // @formatter: off
            return new SSDeepHash(chunkSize, chunkNorm == null ? chunk : chunkNorm, doubleChunkNorm == null ? doubleChunk : doubleChunkNorm);
            // @formatter: on

        } catch (NumberFormatException nfe) {
            throw new SSDeepParseException("Could not parse SSDeepHash, expected first segment to be an integer", ssdeepHash);
        }
    }
}
