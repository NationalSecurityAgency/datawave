package datawave.metrics.keys;

import datawave.util.StringUtils;
import datawave.metrics.util.WritableUtil;
import org.apache.accumulo.core.data.Key;

public class IngestEntryKey implements XKey {
    protected long timestamp;
    protected String type;
    protected long count;
    protected long duration;
    protected String jobId;
    protected String outputDirectory;
    protected Key meAsAKey;
    protected boolean live;
    protected String _str;
    
    public IngestEntryKey() {
        
    }
    
    public IngestEntryKey(Key k) throws InvalidKeyException {
        parse(k);
    }
    
    /**
     * @param timestamp
     *            a timestamp
     * @param type
     *            the type
     * @param count
     *            the count
     * @param duration
     *            the duration
     * @param jobId
     *            a job id
     * @param outputDirectory
     *            the output directory
     */
    public IngestEntryKey(long timestamp, String type, long count, long duration, String jobId, String outputDirectory) {
        this.timestamp = timestamp;
        this.type = type;
        this.count = count;
        this.duration = duration;
        this.jobId = jobId;
        this.outputDirectory = outputDirectory;
    }
    
    @Override
    public void parse(Key k) throws InvalidKeyException {
        try {
            timestamp = WritableUtil.parseLong(k.getRow());
            
            String[] fam = StringUtils.split(k.getColumnFamily().toString(), (char) 0x0);
            type = fam[0];
            count = Long.parseLong(fam[1]);
            duration = Long.parseLong(fam[2]);
            
            String[] qual = StringUtils.split(k.getColumnQualifier().toString(), (char) 0x0);
            jobId = qual[0];
            if (qual.length == 2) {
                outputDirectory = qual[1];
                live = false;
            } else {
                live = true;
            }
        } catch (NullPointerException | StringIndexOutOfBoundsException npe) {
            throw new InvalidKeyException(npe);
        }
    }
    
    @Override
    public Key toKey() {
        if (meAsAKey == null) {
            meAsAKey = new Key(Long.toString(timestamp), type + 0x0 + count + 0x0 + duration, outputDirectory == null ? jobId : jobId + 0x0 + outputDirectory);
        }
        return meAsAKey;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public String getType() {
        return type;
    }
    
    public long getCount() {
        return count;
    }
    
    public long getDuration() {
        return duration;
    }
    
    public String getJobId() {
        return jobId;
    }
    
    public String getOutputDirectory() {
        return outputDirectory;
    }
    
    public boolean isLive() {
        return live;
    }
    
    public boolean isBulk() {
        return !live;
    }
    
    public String toString() {
        if (_str == null) {
            _str = createString();
        }
        return _str;
    }
    
    private String createString() {
        StringBuilder sb = new StringBuilder(100);
        sb.append(timestamp).append(" ").append(type).append('\u0000').append(count).append('\u0000').append(duration).append(" ").append(jobId);
        if (outputDirectory != null) {
            sb.append('\u0000').append(outputDirectory);
        }
        return sb.toString();
    }
}
