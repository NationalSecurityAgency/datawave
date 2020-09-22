package datawave.query.index.lookup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class CondensedIndexInfo implements Writable {
    
    private static final Logger log = Logger.getLogger(CondensedIndexInfo.class);
    
    protected String day;
    protected Map<String,IndexInfo> indexInfos;
    protected Set<String> ignored;
    
    protected SortedSet<String> allShards;
    protected long lastCount = 0;
    
    public CondensedIndexInfo() {
        indexInfos = Maps.newHashMap();
        ignored = Sets.newHashSet();
        allShards = Sets.newTreeSet();
        lastCount = 0;
    }
    
    public CondensedIndexInfo(String day, long count) {
        this();
        this.day = day;
        lastCount = count;
    }
    
    public CondensedIndexInfo(String day, Multimap<String,String> mapList, Set<String> ignored) {
        this();
        this.day = day;
        indexInfos = Maps.newHashMap();
        for (String key : mapList.keySet()) {
            indexInfos.put(key, new IndexInfo(mapList.get(key)));
        }
        this.ignored.addAll(ignored);
        allShards.addAll(ignored);
        allShards.addAll(indexInfos.keySet());
    }
    
    public boolean isDay() {
        return indexInfos.isEmpty();
    }
    
    public String getDay() {
        return day;
    }
    
    public Set<String> getShards() {
        return allShards;
    }
    
    public IndexInfo getShard(final String shard) {
        return indexInfos.get(shard);
    }
    
    public static String compressOption(final String data, final Charset characterSet) throws IOException {
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
    
    public byte[] toByteArray() throws IOException {
        
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        final GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
        final DataOutputStream dataOut = new DataOutputStream(gzipStream);
        
        this.write(dataOut);
        dataOut.close();
        gzipStream.close();
        
        return byteStream.toByteArray();
    }
    
    public void fromByteArray(byte[] array) throws IOException {
        ByteArrayInputStream byteInputStream = new ByteArrayInputStream(array);
        
        GZIPInputStream gzipInputStream = new GZIPInputStream(byteInputStream);
        
        DataInputStream dataInputStream = new DataInputStream(gzipInputStream);
        
        readFields(dataInputStream);
        
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(day);
        out.writeInt(ignored.size());
        for (String shard : ignored) {
            out.writeUTF(shard);
        }
        if (log.isTraceEnabled()) {
            log.trace("indexInfos.size() " + indexInfos.size());
        }
        out.writeInt(indexInfos.size());
        
        for (Entry<String,IndexInfo> indexInfo : indexInfos.entrySet()) {
            if (ignored.contains(indexInfo.getKey()))
                continue;
            if (log.isTraceEnabled()) {
                log.trace("Writing " + indexInfo.getKey());
            }
            out.writeUTF(indexInfo.getKey());
            indexInfo.getValue().write(out);
        }
        out.writeLong(lastCount);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        day = in.readUTF();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            ignored.add(in.readUTF());
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            String shard = in.readUTF();
            IndexInfo newInfo = new IndexInfo();
            newInfo.readFields(in);
            indexInfos.put(shard, newInfo);
        }
        lastCount = in.readLong();
        allShards.addAll(ignored);
        allShards.addAll(indexInfos.keySet());
    }
    
    public boolean isIgnored(String shard) {
        return ignored.contains(shard);
    }
}
