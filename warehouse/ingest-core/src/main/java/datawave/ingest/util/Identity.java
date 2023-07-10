package datawave.ingest.util;

import java.security.MessageDigest;

import org.apache.log4j.Logger;

public class Identity {

    private String hasher = "MD5";
    private MessageDigest md;
    private static final String HEXITS = "0123456789abcdef";
    private StringBuffer sb = new StringBuffer();
    private static Logger log = Logger.getLogger(Identity.class);

    public Identity() {
        setHasher(hasher);
    }

    public Identity(String hf) {
        setHasher(hf);
    }

    public String getHasher() {
        return hasher;
    }

    public String setHasher(String hf) {
        try {
            md = MessageDigest.getInstance(hf.toUpperCase());
        } catch (Exception e1) {
            log.warn("WARN no hasher found to match " + hf.toUpperCase());
            log.warn("WARN setting hasher to default SHA1");
            try {
                md = MessageDigest.getInstance(hasher);
            } catch (Exception e2) {
                log.warn(e2);
                return "Can not set hasher, something is wrong";
            }

            return "Hasher set to default " + hasher;
        }
        hasher = hf.toUpperCase();
        return "Hasher set to " + hasher;
    }

    public String toHex(byte[] me) {
        StringBuffer buf = new StringBuffer(me.length * 2);

        for (int i = 0; i < me.length; i++) {
            buf.append(HEXITS.charAt((me[i] >>> 4) & 0xf));
            buf.append(HEXITS.charAt(me[i] & 0xf));
        }

        return buf.toString();

    }

    public void updateId(byte[] value) {
        md.update(value);
    }

    public void updateId(byte[] value, int offset, int len) {
        md.update(value, offset, len);
    }

    public void updateId(String value) {
        md.update(value.getBytes());
    }

    public void updateId(String[] value) {
        sb.setLength(0);
        for (int i = 0; i < value.length; i++)
            sb.append(value[i]);
        md.update(sb.toString().getBytes());
    }

    public byte[] getDigest(byte esc) {
        int escnum = 0;
        byte[] b = md.digest();

        for (int i = 0; i < b.length; i++) {
            if (b[i] == esc) {
                escnum++;
            }
        }
        if (escnum == 0)
            return b;

        byte[] nb = new byte[b.length + escnum];
        escnum = 0;
        for (int i = 0; i < b.length; i++) {
            if (b[i] == esc) {
                nb[i + escnum] = '\\';
                escnum++;
            }
            nb[i + escnum] = b[i];
        }
        return nb;
    }

    public String getId() {
        return toHex(md.digest());
    }

    public String getId(String message) {
        clearId();
        updateId(message.getBytes());
        return getId();
    }

    public byte[] getDigest() {
        return md.digest();
    }

    public void clearId() {
        md.reset();
    }

}
