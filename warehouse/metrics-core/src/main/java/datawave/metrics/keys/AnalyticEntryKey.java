package datawave.metrics.keys;

import org.apache.accumulo.core.data.Key;

public class AnalyticEntryKey implements XKey {

    private long timestamp;
    private String dataType;
    private String ingestType;
    private boolean live;

    public AnalyticEntryKey() {}

    public AnalyticEntryKey(Key k) throws InvalidKeyException {
        parse(k);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getDataType() {
        return dataType;
    }

    public String getIngestType() {
        return ingestType;
    }

    public boolean isLive() {
        return live;
    }

    public boolean isBulk() {
        return !live;
    }

    @Override
    public final void parse(Key k) throws InvalidKeyException {
        try {
            timestamp = Long.parseLong(k.getRow().toString());
            dataType = k.getColumnFamily().toString();
            ingestType = k.getColumnQualifier().toString();
            live = "live".equalsIgnoreCase(ingestType);
        } catch (NullPointerException | StringIndexOutOfBoundsException npe) {
            throw new InvalidKeyException(npe);
        }
    }

    @Override
    public Key toKey() {
        throw new UnsupportedOperationException();
    }
}
