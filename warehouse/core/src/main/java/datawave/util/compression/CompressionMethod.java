package datawave.util.compression;

import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public abstract class CompressionMethod {

    /*
    Compression and Decompression order of ops:

    1. Get your data as a String
    2. Compress it however you want (Lets say GZIP)
    3. Convert that compressed chunk into BASE64, which helps with transferring the data

    4. Receive the compressed chunk
    5. Revert it from BASE64 to normal (Lets say GZIP)
    6. Un-GZIP it to get your info.

     */

    public abstract String compress(final String data, final Charset charset) throws IOException;
    public abstract String decompress(final String base64, final Charset charset) throws IOException;
}
