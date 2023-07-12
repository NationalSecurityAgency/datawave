package datawave.query.tables.async.event;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Joiner;

/**
 * Utility for reducing, serializing, and compressing field sets
 */
public class FieldSets {

    private FieldSets() {
        // static utility
    }

    public static String serializeFieldSet(Set<String> fieldSet) {
        return Joiner.on(',').join(fieldSet);
    }

    public static Set<String> deserializeFieldSet(String fieldSetString) {
        if (fieldSetString == null) {
            return Collections.emptySet();
        }
        return Arrays.stream(StringUtils.split(fieldSetString, ',')).collect(Collectors.toSet());
    }

    public static String compressFieldSet(String serializedFieldSet) throws IOException {
        return compressFieldSet(serializedFieldSet, StandardCharsets.UTF_8);
    }

    public static String compressFieldSet(final String serializedFieldSet, final Charset characterSet) throws IOException {
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        final GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
        final DataOutputStream dataOut = new DataOutputStream(gzipStream);

        byte[] arr = serializedFieldSet.getBytes(characterSet);
        final int length = arr.length;

        dataOut.writeInt(length);
        dataOut.write(arr);

        dataOut.close();
        byteStream.close();

        return new String(Base64.encodeBase64(byteStream.toByteArray()));
    }

    public static String decompressFieldSet(String serializedFieldSet) throws IOException {
        return decompressFieldSet(serializedFieldSet, StandardCharsets.UTF_8);
    }

    public static String decompressFieldSet(final String serializedFieldSet, final Charset characterSet) throws IOException {
        final byte[] inBase64 = Base64.decodeBase64(serializedFieldSet.getBytes());

        final ByteArrayInputStream byteInputStream = new ByteArrayInputStream(inBase64);
        final GZIPInputStream gzipInputStream = new GZIPInputStream(byteInputStream);
        final DataInputStream dataInputStream = new DataInputStream(gzipInputStream);

        final int length = dataInputStream.readInt();
        final byte[] dataBytes = new byte[length];
        dataInputStream.readFully(dataBytes, 0, length);

        dataInputStream.close();
        gzipInputStream.close();

        return new String(dataBytes, characterSet);
    }
}
