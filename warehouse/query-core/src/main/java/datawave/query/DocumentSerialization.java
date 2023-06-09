package datawave.query;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.exceptions.InvalidDocumentHeader;
import datawave.query.exceptions.NoSuchDeserializerException;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.deserializer.WritableDocumentDeserializer;
import datawave.query.function.serializer.DocumentSerializer;
import datawave.query.function.serializer.WritableDocumentSerializer;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.QueryException;

/**
 *
 */
public class DocumentSerialization {

    public enum ReturnType {
        writable, kryo, tostring, noop
    }

    public static final ReturnType DEFAULT_RETURN_TYPE = ReturnType.kryo;

    private static final int DOC_MAGIC = 0x8b2f;

    public static final byte NONE = 0;
    public static final byte GZIP = 1;

    public static final int ZLIB_NUMBER = 2;

    /**
     * If a user-supplied ReturnType is specified, use it; otherwise, use the default ReturnType of {@link #DEFAULT_RETURN_TYPE}
     *
     * @param settings
     *            query settings
     * @return a return type
     */
    public static ReturnType getReturnType(Query settings) {
        Parameter returnType = settings.findParameter(Constants.RETURN_TYPE);
        if (null != returnType && !org.apache.commons.lang.StringUtils.isBlank(returnType.getParameterValue())) {
            return ReturnType.valueOf(returnType.getParameterValue());
        }

        return DEFAULT_RETURN_TYPE;
    }

    public static DocumentDeserializer getDocumentDeserializer(Query settings) throws NoSuchDeserializerException {
        return getDocumentDeserializer(getReturnType(settings));
    }

    public static DocumentDeserializer getDocumentDeserializer(ReturnType rt) throws NoSuchDeserializerException {
        if (ReturnType.kryo.equals(rt)) {
            return new KryoDocumentDeserializer();
        } else if (ReturnType.writable.equals(rt)) {
            return new WritableDocumentDeserializer();
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.DESERIALIZER_CREATE_ERROR);
            throw new NoSuchDeserializerException(qe);
        }
    }

    public static DocumentSerializer getDocumentSerializer(Query settings) throws NoSuchDeserializerException {
        return getDocumentSerializer(getReturnType(settings));
    }

    public static DocumentSerializer getDocumentSerializer(ReturnType rt) throws NoSuchDeserializerException {
        if (ReturnType.kryo.equals(rt)) {
            return new KryoDocumentSerializer();
        } else if (ReturnType.writable.equals(rt)) {
            return new WritableDocumentSerializer(false);
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.DESERIALIZER_CREATE_ERROR);
            throw new NoSuchDeserializerException(qe);
        }
    }

    public static byte[] getHeader() {
        return getHeader(NONE);
    }

    public static byte[] getHeader(int compression) {

        return new byte[] {(byte) DOC_MAGIC, // Magic number (short)
                (byte) (DOC_MAGIC >> 8), // Magic number (short)
                (byte) compression};
    }

    public static byte[] writeBody(byte[] data, int compression) throws InvalidDocumentHeader {
        if (NONE == compression) {
            return data;
        } else if (GZIP == compression) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(data.length);

            try {
                Deflater deflater = new Deflater(ZLIB_NUMBER);
                DeflaterOutputStream deflate = new DeflaterOutputStream(bytes, deflater, 1024);
                deflate.write(data);
                deflate.close();
                return bytes.toByteArray();
            } catch (IOException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.GZIP_STREAM_WRITE_ERROR, e);
                throw new InvalidDocumentHeader(qe);
            }
        } else {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNKNOWN_COMPRESSION_SCHEME, MessageFormat.format("{0}", compression));
            throw new InvalidDocumentHeader(qe);
        }
    }

    public static InputStream consumeHeader(byte[] data) throws InvalidDocumentHeader {
        if (null == data || 3 > data.length) {
            QueryException qe = new QueryException(DatawaveErrorCode.DATA_INVALID_ERROR,
                            MessageFormat.format("Length: {0}", (null != data ? data.length : null)));
            throw new InvalidDocumentHeader(qe);
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        int magic = readUShort(bais);

        if (DOC_MAGIC != magic) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.EXPECTED_HEADER_NOT_FOUND);
            throw new InvalidDocumentHeader(qe);
        }

        int compression = readUByte(bais);

        if (NONE == compression) {
            return new ByteArrayInputStream(data, 3, data.length - 3);
        } else if (GZIP == compression) {
            ByteArrayInputStream bytes = new ByteArrayInputStream(data, 3, data.length - 3);
            return new InflaterInputStream(bytes, new Inflater(), 1024);
        } else {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNKNOWN_COMPRESSION_SCHEME, MessageFormat.format("{0}", compression));
            throw new InvalidDocumentHeader(qe);
        }
    }

    /*
     * Reads unsigned short in Intel byte order.
     */
    private static int readUShort(InputStream in) throws InvalidDocumentHeader {
        int b = readUByte(in);
        return (readUByte(in) << 8) | b;
    }

    /*
     * Reads unsigned byte.
     */
    private static int readUByte(InputStream in) throws InvalidDocumentHeader {
        int b;

        try {
            b = in.read();
        } catch (IOException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.BUFFER_READ_ERROR, e);
            throw new InvalidDocumentHeader(qe);
        }

        if (b == -1) {
            QueryException qe = new QueryException(DatawaveErrorCode.INVALID_BYTE);
            throw new InvalidDocumentHeader(qe);
        }

        return b;
    }

}
