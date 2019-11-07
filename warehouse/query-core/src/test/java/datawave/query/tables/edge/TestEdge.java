package datawave.query.tables.edge;

import java.util.ArrayList;
import java.util.List;

import datawave.data.normalizer.AbstractNormalizer;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestEdge {
    
    // Temp class, should go away once final edge stuff is determined and migrated to
    
    public byte[] NULL_BYTE = new byte[0];
    
    public char valSep = '/';
    
    public char relSep = '-';
    
    public boolean bidirectional = true;
    
    public String source;
    
    public String sink;
    
    public String dataType;
    
    public String statsType;
    
    public String STATS_STR = "STATS";
    
    public String fromRel;
    
    public String toRel;
    
    public String fromSource;
    
    public String toSource;
    
    public String dateStr;
    
    public String visibility;
    
    public long timestamp;
    
    public Value value = null;
    
    public boolean statsEdge = false;
    
    protected AbstractNormalizer<String> normalizer;
    
    public static TestEdge createEdge(String source, String sink, String dateStr, String dataType, String fromRel, String toRel, String fromSource,
                    String toSource, String visibility, long timestamp) {
        return createEdge(source, sink, dateStr, dataType, fromRel, toRel, fromSource, toSource, visibility, timestamp, null);
    }
    
    public static TestEdge createEdge(String source, String sink, String dateStr, String dataType, String fromRel, String toRel, String fromSource,
                    String toSource, String visibility, long timestamp, AbstractNormalizer<String> normalizer) {
        
        TestEdge retVal = new TestEdge();
        retVal.source = source;
        retVal.sink = sink;
        retVal.dataType = dataType;
        retVal.fromRel = fromRel;
        retVal.toRel = toRel;
        retVal.fromSource = fromSource;
        retVal.toSource = toSource;
        retVal.dateStr = dateStr;
        retVal.visibility = visibility;
        retVal.timestamp = timestamp;
        retVal.normalizer = normalizer;
        return retVal;
    }
    
    // Construct a stats edge
    public static TestEdge createEdge(String source, String dateStr, String statsType, String dataType, String toRel, String toSource, String visibility,
                    long timestamp) {
        return createEdge(source, dateStr, statsType, dataType, toRel, toSource, visibility, timestamp, null);
    }
    
    // Construct a stats edge
    public static TestEdge createEdge(String source, String dateStr, String statsType, String dataType, String toRel, String toSource, String visibility,
                    long timestamp, AbstractNormalizer<String> normalizer) {
        
        TestEdge retVal = new TestEdge();
        retVal.source = source;
        retVal.statsType = statsType;
        retVal.dataType = dataType;
        retVal.toRel = toRel;
        retVal.toSource = toSource;
        retVal.dateStr = dateStr;
        retVal.visibility = visibility;
        retVal.timestamp = timestamp;
        retVal.statsEdge = true;
        retVal.normalizer = normalizer;
        return retVal;
    }
    
    @Test
    public void placeholder() {}
    
    public Text getColumnFamily(boolean reversed, boolean protobufEdgeFormat) {
        StringBuilder colfsb = new StringBuilder();
        colfsb.append(getDataType()).append(valSep);
        if (reversed) {
            colfsb.append(getToRel()).append(relSep).append(getFromRel());
            if (!protobufEdgeFormat) {
                colfsb.append(valSep).append(getToSource()).append(relSep).append(getFromSource());
            }
        } else {
            colfsb.append(getFromRel()).append(relSep).append(getToRel());
            if (!protobufEdgeFormat) {
                colfsb.append(valSep).append(getFromSource()).append(relSep).append(getToSource());
            }
        }
        return new Text(colfsb.toString());
    }
    
    public Text getStatsColumnFamily(boolean protobufEdgeFormat) {
        StringBuilder colfsb = new StringBuilder();
        colfsb.append(STATS_STR).append(valSep).append(statsType).append(valSep);
        colfsb.append(getDataType()).append(valSep);
        colfsb.append(getToRel());
        
        if (!protobufEdgeFormat) {
            colfsb.append(valSep).append(getToSource());
        }
        
        return new Text(colfsb.toString());
    }
    
    public Text getColumnQualifier(boolean reversed, boolean protobufEdgeFormat) {
        StringBuilder colqsb = new StringBuilder();
        colqsb.append(getDateStr());
        if (protobufEdgeFormat) {
            colqsb.append(valSep).append(getFromSource()).append(relSep).append(getToSource());
        }
        return new Text(colqsb.toString());
    }
    
    public Text getStatsColumnQualifier(boolean protobufEdgeFormat) {
        StringBuilder colqsb = new StringBuilder();
        colqsb.append(getDateStr());
        if (protobufEdgeFormat) {
            colqsb.append(valSep).append(getToSource());
        }
        return new Text(colqsb.toString());
    }
    
    protected String formatRow(String source) {
        String tempSource = source;
        if (normalizer != null) {
            tempSource = normalizer.normalize(source);
        }
        tempSource = StringEscapeUtils.escapeJava(tempSource);
        return tempSource;
    }
    
    protected String formatRow(String source, String sink) {
        String tempSource = source, tempSink = sink;
        if (normalizer != null) {
            tempSource = normalizer.normalize(tempSource);
            tempSink = normalizer.normalize(tempSink);
        }
        tempSource = StringEscapeUtils.escapeJava(tempSource);
        tempSink = StringEscapeUtils.escapeJava(tempSink);
        return tempSource + "\0" + tempSink;
    }
    
    public List<Mutation> getMutations(boolean protobufEdgeFormat) {
        ArrayList<Mutation> retVal = new ArrayList<>();
        
        if (statsEdge) {
            Mutation mut = new Mutation(formatRow(getSource()));
            mut.put(getStatsColumnFamily(protobufEdgeFormat), getStatsColumnQualifier(protobufEdgeFormat), new ColumnVisibility(getVisibility()),
                            getTimestamp(), getValue());
            retVal.add(mut);
            
        } else {
            Mutation mut = new Mutation(formatRow(getSource(), getSink()));
            mut.put(getColumnFamily(false, protobufEdgeFormat), getColumnQualifier(false, protobufEdgeFormat), new ColumnVisibility(getVisibility()),
                            getTimestamp(), getValue());
            retVal.add(mut);
            if (isBidirectional()) {
                Mutation mut2 = new Mutation(formatRow(getSink(), getSource()));
                mut2.put(getColumnFamily(true, protobufEdgeFormat), getColumnQualifier(true, protobufEdgeFormat), new ColumnVisibility(getVisibility()),
                                getTimestamp(), getValue());
                retVal.add(mut2);
            }
        }
        
        return retVal;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    public Value getValue() {
        if (value == null)
            value = new Value(NULL_BYTE);
        return value;
    }
    
    public void setValue(Value value) {
        this.value = value;
    }
    
    public boolean isBidirectional() {
        return bidirectional;
    }
    
    public void setBidirectional(boolean bidirectional) {
        this.bidirectional = bidirectional;
    }
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
    
    public String getSink() {
        return sink;
    }
    
    public void setSink(String sink) {
        this.sink = sink;
    }
    
    public String getDataType() {
        return dataType;
    }
    
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
    
    public String getFromRel() {
        return fromRel;
    }
    
    public void setFromRel(String fromRel) {
        this.fromRel = fromRel;
    }
    
    public String getToRel() {
        return toRel;
    }
    
    public void setToRel(String toRel) {
        this.toRel = toRel;
    }
    
    public String getStatsType() {
        return statsType;
    }
    
    public void setStatsType(String statsType) {
        this.statsType = statsType;
    }
    
    public String getFromSource() {
        return fromSource;
    }
    
    public void setFromSource(String fromSource) {
        this.fromSource = fromSource;
    }
    
    public String getToSource() {
        return toSource;
    }
    
    public void setToSource(String toSource) {
        this.toSource = toSource;
    }
    
    public String getDateStr() {
        return dateStr;
    }
    
    public void setDateStr(String dateStr) {
        this.dateStr = dateStr;
    }
    
    public String getVisibility() {
        return visibility;
    }
    
    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }
    
}
