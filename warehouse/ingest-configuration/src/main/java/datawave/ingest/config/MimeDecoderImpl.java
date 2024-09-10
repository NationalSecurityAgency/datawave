package datawave.ingest.config;

import java.io.UnsupportedEncodingException;

import javax.mail.internet.MimeUtility;

public class MimeDecoderImpl implements MimeDecoder {

    @Override
    public byte[] decode(byte[] b) throws UnsupportedEncodingException {
        return MimeUtility.decodeText(new String(b, "iso-8859-1")).getBytes();
    }

}
