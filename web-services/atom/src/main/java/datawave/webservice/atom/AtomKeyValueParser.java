package datawave.webservice.atom;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.abdera.Abdera;
import org.apache.abdera.i18n.iri.IRI;
import org.apache.abdera.model.Entry;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.jboss.resteasy.util.Base64;

public class AtomKeyValueParser {

    private static final String DELIMITER = "\0";
    private static final String ENTRY_ID_FORMAT = "https://{0}:{1}/DataWave/Atom/{2}/{3}";
    private static final String LINK_FORMAT = "https://{0}:{1}/DataWave/Query/lookupUUID/UUID?uuid={2}&parameters=model.name:DATAWAVE;model.table.name:DatawaveMetadata";
    private static final String TITLE_FORMAT = "({0}) {1} with {2} @ {3,date,long} {3,time,full}";

    private String collectionName = null;
    private String id = null;
    private Date updated = null;
    private String columnVisibility = null;
    private String uuid = null;
    private String value = null;

    public String getCollectionName() {
        return collectionName;
    }

    public String getId() {
        return id;
    }

    public Date getUpdated() {
        return updated;
    }

    public String getColumnVisibility() {
        return columnVisibility;
    }

    public String getUuid() {
        return uuid;
    }

    private void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    private void setId(String id) {
        this.id = id;
    }

    private void setUpdated(Date updated) {
        this.updated = updated;
    }

    private void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    private void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Entry toEntry(Abdera abdera, String host, String port) {

        String id = MessageFormat.format(ENTRY_ID_FORMAT, host, port, this.getCollectionName(), this.getId());
        String link = MessageFormat.format(LINK_FORMAT, host, port, this.getUuid());
        String title = MessageFormat.format(TITLE_FORMAT, this.getColumnVisibility(), this.getCollectionName(), this.getValue(), this.getUpdated());

        Entry entry = abdera.newEntry();
        IRI atomId = new IRI(id);
        entry.setId(atomId.toString());
        entry.addLink(link, "alternate");
        entry.setTitle(title);
        entry.setUpdated(this.getUpdated());
        return entry;
    }

    public static AtomKeyValueParser parse(Key key, Value value) throws IOException {
        AtomKeyValueParser atom = new AtomKeyValueParser();

        String row = key.getRow().toString();
        int splitPoint = row.indexOf(DELIMITER);
        if (splitPoint != -1) {
            atom.setCollectionName(row.substring(0, splitPoint));
            int delim = key.getRow().find(DELIMITER);
            byte[] b = key.getRow().getBytes();
            long diff = LongCombiner.FIXED_LEN_ENCODER.decode(Arrays.copyOfRange(b, delim + 1, key.getRow().getLength()));
            long time = (Long.MAX_VALUE - diff);
            atom.setUpdated(new Date(time));
        } else {
            throw new IllegalArgumentException("Atom entry is missing row parts: " + key);
        }
        atom.setId(AtomKeyValueParser.encodeId(key.getColumnFamily().toString()));
        String colf = key.getColumnFamily().toString();
        splitPoint = colf.indexOf(DELIMITER);
        if (splitPoint != -1 && splitPoint + 1 != colf.length()) {
            atom.setValue(colf.substring(0, splitPoint));
            atom.setUuid(colf.substring(splitPoint + 1));
        } else {
            throw new IllegalArgumentException("Atom entry is missing column qualifier parts: " + key);
        }
        atom.setColumnVisibility(key.getColumnQualifier().toString());

        return atom;
    }

    public static String encodeId(String id) throws UnsupportedEncodingException {
        String key64 = Base64.encodeBytes(id.getBytes());
        return URLEncoder.encode(key64, "UTF-8");
    }

    public static String decodeId(String encodedId) throws IOException {
        String key64 = URLDecoder.decode(encodedId, "UTF-8");
        byte[] bKey = Base64.decode(key64);
        return new String(bKey);
    }
}
