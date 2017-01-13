package nsa.datawave.metrics.web.stats.rfile;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

public class RFileSize implements Comparable<RFileSize> {
    
    private static final Logger log = Logger.getLogger(RFileSize.class);
    protected Key key;
    
    protected long aggregateSize;
    
    protected long aggregateRowCount;
    
    protected long fileCount;
    
    protected String tserver;
    
    protected String extent;
    
    protected String tableId;
    
    protected long compressedSize;
    
    protected long rawSize;
    
    protected Map<String,Long> files;
    
    public RFileSize() {
        key = null;
        
        aggregateSize = 0;
        
        aggregateRowCount = 0;
        
        fileCount = 0;
        
        files = new HashMap<>();
        
    }
    
    public void addFile(String file) {
        files.put(file, 0L);
    }
    
    public Collection<String> getFiles() {
        return files.keySet();
    }
    
    public void removeFile(String file) {
        if (files.containsKey(file))
            files.remove(file);
    }
    
    public void setAge(String file, Long age) {
        files.put(file, age);
    }
    
    public Collection<Long> getAges() {
        return files.values();
    }
    
    public Long getAge(String file) {
        return files.get(file);
    }
    
    public void setRawSize(long sz) {
        rawSize = sz;
    }
    
    public void setCompressedSize(long sz) {
        compressedSize = sz;
    }
    
    public long getRawSize() {
        return rawSize;
    }
    
    public long getCompressedSize() {
        return compressedSize;
    }
    
    public void setKey(Key key) {
        this.key = key;
    }
    
    public void setTableId(String id) {
        tableId = id;
    }
    
    public void incrementSize(long size) {
        aggregateSize += size;
    }
    
    public void incrementCount(long count) {
        aggregateRowCount += count;
    }
    
    public void incrementFileCount() {
        fileCount++;
    }
    
    @Override
    public int compareTo(RFileSize o) {
        int compare = Long.valueOf(aggregateSize).compareTo(o.aggregateSize);
        // log.info(message)
        if (compare == 0) {
            compare = Long.valueOf(aggregateRowCount).compareTo(o.aggregateRowCount);
        } else
            return compare;
        
        if (compare == 0) {
            compare = Long.valueOf(fileCount).compareTo(o.fileCount);
        }
        
        return compare;
    }
    
    @Override
    public boolean equals(Object o) {
        return o instanceof RFileSize && 0 == this.compareTo((RFileSize) o);
    }
    
    public String getTableId() {
        return tableId;
    }
    
    public Key getKey() {
        return key;
    }
    
    public long getSize() {
        return aggregateSize;
    }
    
    public String getTabletServer() {
        return tserver;
    }
    
    public void setTserver(String tabletServer) {
        tserver = tabletServer;
        
    }
    
    public long getAggregateRowCount() {
        return aggregateRowCount;
    }
    
    public long getFileCount() {
        return fileCount;
    }
    
}
