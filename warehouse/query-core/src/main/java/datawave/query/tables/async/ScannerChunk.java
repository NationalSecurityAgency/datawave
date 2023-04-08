package datawave.query.tables.async;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import datawave.core.query.configuration.ResultContext;
import datawave.query.tables.SessionOptions;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 
 */
public class ScannerChunk {
    
    protected ResultContext context;
    protected SessionOptions options;
    protected ConcurrentLinkedQueue<Range> ranges;
    protected Range lastRange;
    protected String lastKnownLocation;
    protected int hashCode = 31;
    protected String queryId = "";
    
    /*
     * Constructor used for testing
     *
     * @param options
     * 
     * @param ranges
     * 
     * @param context
     */
    public ScannerChunk(SessionOptions options, Collection<Range> ranges, ResultContext context) {
        this(options, ranges, context, "localhost");
    }
    
    public ScannerChunk(SessionOptions options, Collection<Range> ranges, ResultContext context, String server) {
        Preconditions.checkNotNull(ranges);
        this.context = context;
        this.options = options;
        this.ranges = new ConcurrentLinkedQueue<>();
        this.lastKnownLocation = server;
        setRanges(ranges);
        
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(options);
        builder.append(ranges);
        builder.append(lastKnownLocation);
        hashCode += builder.hashCode();
    }
    
    /**
     * Deepcopy for the scanner chunk
     * 
     * @param chunk
     *            a chunk
     */
    public ScannerChunk(ScannerChunk chunk) {
        Preconditions.checkNotNull(chunk);
        this.options = chunk.options;
        this.context = chunk.context;
        this.ranges = new ConcurrentLinkedQueue<>();
        setRanges(chunk.ranges);
        this.lastKnownLocation = chunk.lastKnownLocation;
        
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(options);
        builder.append(ranges);
        hashCode += builder.hashCode();
        this.queryId = chunk.queryId;
    }
    
    protected void setRanges(Collection<Range> ranges) {
        if (!ranges.isEmpty()) {
            List<Range> rangeList = Lists.newArrayList(ranges);
            Collections.sort(rangeList);
            this.ranges.addAll(ranges);
            lastRange = Iterables.getLast(this.ranges);
        } else
            lastRange = null;
    }
    
    public void addRange(Range range) {
        setRanges(Collections.singleton(range));
    }
    
    @Override
    public int hashCode() {
        return hashCode;
        
    }
    
    public ResultContext getContext() {
        return context;
    }
    
    public Range getLastRange() {
        return lastRange;
    }
    
    public Range getNextRange() {
        if (ranges.isEmpty())
            return null;
        else
            return ranges.poll();
    }
    
    public String getLastKnownLocation() {
        return lastKnownLocation;
    }
    
    @Override
    public String toString() {
        return new StringBuilder().append(options).append(ranges).append(lastKnownLocation).toString();
    }
    
    public SessionOptions getOptions() {
        return options;
    }
    
    public Collection<Range> getRanges() {
        return ranges;
    }
    
    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    public void setOptions(SessionOptions newOptions) {
        this.options = newOptions;
    }
    
}
