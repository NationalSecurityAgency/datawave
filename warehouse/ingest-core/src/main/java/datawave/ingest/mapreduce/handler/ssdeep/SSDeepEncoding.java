package datawave.ingest.mapreduce.handler.ssdeep;

import java.io.Serializable;

/**
 * Simple converter from an SSDeep string that includes only characters from the Base64 alphabet to a byte array. We can cast the Base64 letters directly into
 * bytes without needing to do the complex operations that Java must usually do when converting strings to bytes because we do not have to handle multibyte
 * characters here. As a result this implementation is more performant than alternatives built into java like 'String.getBytes()'.
 */
public class SSDeepEncoding implements Serializable {
    public byte[] encode(String ngram) {
        return encodeToBytes(ngram, new byte[ngram.length()], 0);
    }
    
    public byte[] encodeToBytes(String ngram, byte[] buffer, int offset) {
        for (int i = 0; i < ngram.length(); i++) {
            buffer[i + offset] = (byte) ngram.charAt(i);
        }
        return buffer;
    }
}
