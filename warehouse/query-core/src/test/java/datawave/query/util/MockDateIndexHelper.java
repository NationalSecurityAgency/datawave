package datawave.query.util;

import com.google.common.collect.TreeMultimap;
import org.apache.accumulo.core.client.TableNotFoundException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;

public class MockDateIndexHelper extends DateIndexHelper {
    
    private final TreeMultimap<String,Entry> entries = TreeMultimap.create();
    
    private static class Entry implements Comparable<Entry> {
        public final String type;
        public final String dataType;
        public final String field;
        public final String shard;
        
        public Entry(String type, String dataType, String field, String shard) {
            this.type = type;
            this.dataType = dataType;
            this.field = field;
            this.shard = shard;
        }
        
        public String getShardDate() {
            int index = shard.indexOf('_');
            return shard.substring(0, index);
        }
        
        @Override
        public int compareTo(Entry o) {
            int cmp = type.compareTo(o.type);
            if (cmp == 0) {
                cmp = dataType.compareTo(o.dataType);
            }
            if (cmp == 0) {
                cmp = field.compareTo(o.field);
            }
            if (cmp == 0) {
                cmp = shard.compareTo(o.shard);
            }
            return cmp;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            
            Entry entry = (Entry) o;
            
            if (type != null ? !type.equals(entry.type) : entry.type != null)
                return false;
            if (dataType != null ? !dataType.equals(entry.dataType) : entry.dataType != null)
                return false;
            if (field != null ? !field.equals(entry.field) : entry.field != null)
                return false;
            return shard != null ? shard.equals(entry.shard) : entry.shard == null;
        }
        
        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (dataType != null ? dataType.hashCode() : 0);
            result = 31 * result + (field != null ? field.hashCode() : 0);
            result = 31 * result + (shard != null ? shard.hashCode() : 0);
            return result;
        }
        
        @Override
        public String toString() {
            return "Entry{" + "type='" + type + '\'' + ", dataType='" + dataType + '\'' + ", field='" + field + '\'' + ", shard='" + shard + '\'' + '}';
        }
    }
    
    public void addEntry(String date, String type, String dataType, String field, String shard) {
        entries.put(date, new Entry(type, dataType, field, shard));
    }
    
    @Override
    public DateTypeDescription getTypeDescription(String dateType, Date begin, Date end, Set<String> datatypeFilter) throws TableNotFoundException {
        final DateTypeDescription desc = new DateTypeDescription();
        for (Entry value : entries.values()) {
            if (value.type.equals(dateType) && (datatypeFilter == null || datatypeFilter.isEmpty() || datatypeFilter.contains(value.dataType))) {
                desc.fields.add(value.field);
            }
        }
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        
        for (Map.Entry<String,Entry> entry : entries.entries()) {
            if (entry.getValue().type.equals(dateType)
                            && (datatypeFilter == null || datatypeFilter.isEmpty() || datatypeFilter.contains(entry.getValue().dataType))) {
                
                String date = entry.getValue().getShardDate();
                if (desc.dateRange[0] == null) {
                    desc.dateRange[0] = desc.dateRange[1] = date;
                } else {
                    if (date.compareTo(desc.dateRange[0]) < 0) {
                        desc.dateRange[0] = date;
                    }
                    if (date.compareTo(desc.dateRange[1]) > 0) {
                        desc.dateRange[1] = date;
                    }
                }
            }
        }
        if (desc.dateRange[0] == null) {
            desc.dateRange[0] = formatter.format(begin);
            desc.dateRange[1] = formatter.format(end);
        }
        return desc;
    }
    
    @Override
    public String getShardsAndDaysHint(String field, Date begin, Date end, Date rangeBegin, Date rangeEnd, Set<String> datatypeFilter)
                    throws TableNotFoundException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String beginStr = formatter.format(begin);
        String endStr = formatter.format(end);
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String,Entry> entry : entries.entries()) {
            if (endStr.compareTo(entry.getKey()) < 0) {
                return builder.toString();
            } else if (beginStr.compareTo(entry.getKey()) <= 0) {
                if (entry.getValue().field.equals(field)
                                && (datatypeFilter == null || datatypeFilter.isEmpty() || datatypeFilter.contains(entry.getValue().dataType))) {
                    if (builder.length() > 0) {
                        builder.append(',');
                    }
                    builder.append(entry.getValue().shard);
                }
            }
        }
        return builder.toString();
    }
}
