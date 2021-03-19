package datawave.microservice.query.configuration;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Class to encapsulate all required information to run a query.
 *
 */
public class QueryData {
    List<IteratorSetting> settings = Lists.newArrayList();
    String query;
    Collection<Range> ranges = Sets.newHashSet();
    Collection<String> columnFamilies = Sets.newHashSet();
    
    public QueryData() {}
    
    public QueryData(String query, Collection<Range> ranges, List<IteratorSetting> settings) {
        setQuery(query);
        setRanges(ranges);
        setSettings(settings);
    }
    
    public QueryData(QueryData other) {
        this(other.getQuery(), other.getRanges(), other.getSettings());
    }
    
    public QueryData(QueryData other, Collection<Range> ranges) {
        setQuery(other.getQuery());
        setSettings(other.getSettings());
        setRanges(ranges);
    }
    
    public QueryData(String queryString, ArrayList<Range> ranges, List<IteratorSetting> settings, Collection<String> columnFamilies) {
        this(queryString, ranges, settings);
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
    
    public Collection<Range> getRanges() {
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
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(256);
        sb.append("Query: '").append(this.query).append("', Ranges: ").append(this.ranges).append(", Settings: ").append(this.settings);
        return sb.toString();
    }
}
