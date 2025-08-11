package datawave.util;

import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPOutputStream;

public class Compression {

    /*
    Compression and Decompression order of ops:

    1. Get your data as a String
    2. Compress it however you want (Lets say GZIP)
    3. Convert that compressed chunk into BASE64, which helps with transferring the data

    4. Receive the compressed chunk
    5. Revert it from BASE64 to normal (Lets say GZIP)
    6. Un-GZIP it to get your info.

     */

    public enum CompressionAlgorithm {
        GZIP,
    }

    public enum Codec {
        BASE64,
    }

    public static String compressGZIP(final String data, final Charset characterSet) throws IOException {
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        final GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
        final DataOutputStream dataOut = new DataOutputStream(gzipStream);

        byte[] arr = data.getBytes(characterSet);
        final int length = arr.length;

        dataOut.writeInt(length);
        dataOut.write(arr);

        dataOut.close();
        byteStream.close();

        return new String(Base64.encodeBase64(byteStream.toByteArray()));
    }

    public static String decompressGZIP(){
        return "";
    }

    public static String encodeBASE64(final String data, final Charset characterSet, ) throws IOException {
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        final GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
        final DataOutputStream dataOut = new DataOutputStream(gzipStream);

        byte[] arr = data.getBytes(characterSet);
        final int length = arr.length;

        dataOut.writeInt(length);
        dataOut.write(arr);

        dataOut.close();
        byteStream.close();

        return new String(Base64.encodeBase64(byteStream.toByteArray()));
    }

    public static String decodeBASE64(){
        return "";
    }
}
