package datawave.services.query.configuration;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Class to encapsulate all required information to run a query.
 *
 */
public class QueryData implements ResultContext, Externalizable {
    String tableName;
    List<IteratorSetting> settings = Lists.newArrayList();
    String query;
    Collection<Range> ranges = Sets.newHashSet();
    Collection<String> columnFamilies = Sets.newHashSet();
    Key lastResult;
    boolean finished = false;
    
    public QueryData() {}
    
    public QueryData(String tableName, String query, Collection<Range> ranges, List<IteratorSetting> settings) {
        setTableName(tableName);
        setQuery(query);
        setRanges(ranges);
        setSettings(settings);
    }
    
    public QueryData(QueryData other) {
        this(other.getTableName(), other.getQuery(), other.getRanges(), other.getSettings());
        this.lastResult = other.lastResult;
        this.finished = other.finished;
    }
    
    public QueryData(QueryData other, Collection<Range> ranges) {
        setTableName(other.getTableName());
        setQuery(other.getQuery());
        setSettings(other.getSettings());
        setRanges(ranges);
    }
    
    public QueryData(String tableName, String queryString, Collection<Range> ranges, List<IteratorSetting> settings, Collection<String> columnFamilies) {
        this(tableName, queryString, ranges, settings);
        this.columnFamilies.addAll(columnFamilies);
    }
    
    public List<IteratorSetting> getSettings() {
        return settings;
    }
    
    public void setSettings(List<IteratorSetting> settings) {
        this.settings = Lists.newArrayList(settings);
    }
    
    public String getQuery() {
        return query;
    }
    
    public void setQuery(String query) {
        this.query = query;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public Collection<Range> getRanges() {
        if (isFinished()) {
            return Collections.emptySet();
        } else if (lastResult != null) {
            List<Range> newRanges = new ArrayList<>();
            for (Range range : ranges) {
                if (range.contains(lastResult)) {
                    newRanges.add(new Range(lastResult, false, range.getEndKey(), range.isEndKeyInclusive()));
                } else {
                    newRanges.add(range);
                }
            }
            return newRanges;
        }
        return ranges;
    }
    
    public Collection<String> getColumnFamilies() {
        return columnFamilies;
    }
    
    public void setRanges(Collection<Range> ranges) {
        if (null != ranges)
            this.ranges.addAll(ranges);
    }
    
    public void addIterator(IteratorSetting cfg) {
        this.settings.add(cfg);
    }
    
    public void setLastResult(Key result) {
        this.lastResult = result;
        if (this.lastResult == null) {
            this.finished = true;
        }
    }
    
    public boolean isFinished() {
        return this.finished;
    }
    
    public Key getLastResult() {
        return lastResult;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(256);
        sb.append("Query: '").append(this.query).append("', Ranges: ").append(this.ranges).append(", lastResult: ").append(this.lastResult)
                        .append(", Settings: ").append(this.settings);
        return sb.toString();
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(settings.size());
        for (IteratorSetting setting : settings) {
            setting.write(out);
        }
        if (query != null) {
            out.writeBoolean(true);
            out.writeUTF(query);
        } else {
            out.writeBoolean(false);
        }
        out.writeInt(ranges.size());
        for (Range range : ranges) {
            range.write(out);
        }
        out.writeInt(columnFamilies.size());
        for (String cf : columnFamilies) {
            out.writeUTF(cf);
        }
        if (lastResult != null) {
            out.writeBoolean(true);
            lastResult.write(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(finished);
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException {
        settings.clear();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            settings.add(new IteratorSetting(in));
        }
        boolean exists = in.readBoolean();
        if (exists) {
            query = in.readUTF();
        }
        ranges.clear();
        count = in.readInt();
        for (int i = 0; i < count; i++) {
            Range range = new Range();
            range.readFields(in);
            ranges.add(range);
        }
        count = in.readInt();
        for (int i = 0; i < count; i++) {
            columnFamilies.add(in.readUTF());
        }
        exists = in.readBoolean();
        if (exists) {
            lastResult = new Key();
            lastResult.readFields(in);
        }
        finished = in.readBoolean();
    }
    
    public QueryData(ObjectInput in) throws IOException {
        readExternal(in);
    }
}
