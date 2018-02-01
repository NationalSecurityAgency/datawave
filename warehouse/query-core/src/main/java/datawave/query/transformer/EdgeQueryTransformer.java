package datawave.query.transformer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import datawave.edge.model.EdgeModelAware;
import datawave.edge.util.EdgeKey;
import datawave.edge.util.EdgeValue;
import datawave.edge.util.EdgeValueHelper;
import datawave.marking.MarkingFunctions;
import datawave.util.time.DateHelper;
import datawave.webservice.query.Query;
import datawave.webservice.query.cachedresults.CacheableLogic;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.cachedresults.CacheableQueryRowImpl;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

public class EdgeQueryTransformer extends BaseQueryLogicTransformer implements CacheableLogic, EdgeModelAware {
    private Logger log = Logger.getLogger(EdgeQueryTransformer.class);
    protected Authorizations auths;
    protected ResponseObjectFactory responseObjectFactory;
    
    public EdgeQueryTransformer(Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
        super(markingFunctions);
        this.responseObjectFactory = responseObjectFactory;
        auths = new Authorizations(settings.getQueryAuthorizations().split(","));
    }
    
    private static final String ERROR_INCORRECT_BYTE_ARRAY_SIZE = "The bitmask byte array is invalid. The array should have four bytes, but has %d bytes";
    private static final String ERROR_INVALID_HOUR = "The supplied hour must be a value of 0-23. Supplied hour: %d";
    
    /**
     * Returns the bit for a given hour from within the bytes of an int32 Protobuf bitmask.
     * 
     * @param bytes
     *            The bitmask bytes
     * @param hour
     *            The hour
     * @return The bit for the hour.
     */
    private static int getBitInBitmask(byte[] bytes, int hour) {
        if (bytes.length != 4) {
            throw new IllegalArgumentException(String.format(ERROR_INCORRECT_BYTE_ARRAY_SIZE, bytes.length));
        }
        if (hour < -1 || hour > 24) {
            throw new IllegalArgumentException(String.format(ERROR_INVALID_HOUR, hour));
        }
        
        int tmpInt = 23 - hour;
        Double tmpDbl = Math.floor(((double) tmpInt) / 8);
        int idx = tmpDbl.intValue() + 1;
        
        byte b = bytes[idx];
        tmpDbl = ((double) hour) / 8;
        int pos = hour - (tmpDbl.intValue() * 8);
        return (b >> pos & 1);
    }
    
    /**
     * Returns a boolean array with the decoded hourly activity from the Value's bitmask.
     * 
     * @param value
     * @return the hourly activity
     * @throws InvalidProtocolBufferException
     */
    public static boolean[] decodeHourlyActivityToBooleanArray(Value value) throws InvalidProtocolBufferException {
        boolean[] hourlyActivity = new boolean[24];
        EdgeValue edgeValue = EdgeValue.decode(value);
        if (null != edgeValue) {
            int bitmask = edgeValue.getBitmask();
            if (bitmask != 0) {// if the protobuff did not include a value no point parsing nulls out.
                byte[] bytes = ByteBuffer.allocate(4).putInt(bitmask).array();
                for (int i = 0; i < 24; i++) {
                    hourlyActivity[i] = (getBitInBitmask(bytes, i)) == 1;
                }
            }
        }
        return hourlyActivity;
    }
    
    @Override
    public Object transform(Object input) {
        
        if (!(input instanceof Entry)) {
            throw new IllegalArgumentException("Invalid input is not an Entry<Key, Value>: " + (input == null ? null : input.getClass()));
        }
        
        @SuppressWarnings("unchecked")
        Entry<Key,Value> entry = (Entry<Key,Value>) input;
        
        EdgeKey edgeKey = EdgeKey.decode(entry.getKey());
        Value value = entry.getValue();
        
        EdgeBase edge = (EdgeBase) this.responseObjectFactory.getEdge();
        
        boolean statsEdge = edgeKey.isStatsKey();
        try {
            Map<String,String> markings = markingFunctions.translateFromColumnVisibilityForAuths(new ColumnVisibility(edgeKey.getColvis()), auths);
            edge.setMarkings(markings);
            edge.setEdgeType(edgeKey.getType());
            edge.setEdgeRelationship(edgeKey.getRelationship());
            edge.setEdgeAttribute1Source(edgeKey.getAttribute1());
            
            if (edgeKey.getDateType() == EdgeKey.DATE_TYPE.ACTIVITY_ONLY || edgeKey.getDateType() == EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT) {
                edge.setActivityDate(edgeKey.getYyyymmdd());
            }
            edge.setDate(DateHelper.format(entry.getKey().getTimestamp())); // the aquisition time is always in the key timestamp field
        } catch (Exception ex) {
            log.error("cound not get markings for " + new ColumnVisibility(edgeKey.getColvis()), ex);
        }
        EdgeValue edgeValue = null;
        try {
            edgeValue = EdgeValue.decode(value);
            // the source and sink values in the protobuf are un-normalized, so use them if they are present
            edge.setSource(edgeValue.getSourceValue());
            edge.setSink(edgeValue.getSinkValue());
            if (edgeValue.hasLoadDate()) {
                edge.setLoadDate(edgeValue.getLoadDate());
            } else {
                edge.setLoadDate(edgeKey.getYyyymmdd());
            }
            
        } catch (InvalidProtocolBufferException e) {
            // bad protobuf, get source and sink from key
            log.error("Invalid protobuff edge encountered!");
        }
        
        // value for source and sink will get set if they were not in the protobuf
        if (edge.getSource() == null) {
            edge.setSource(edgeKey.getSourceData());
        }
        if (edge.getSink() == null) {
            edge.setSink(edgeKey.getSinkData());
        }
        
        if (statsEdge) {
            edge.setStatsType(edgeKey.getStatsType().name());
            switch (edgeKey.getStatsType()) {
                case ACTIVITY:
                    edge.setCounts(EdgeValueHelper.decodeActivityHistogram(value));
                    break;
                case DURATION:
                    edge.setCounts(EdgeValueHelper.decodeDurationHistogram(value));
                    break;
                case LINKS:
                    break;
            }
            edge.setCount(null);
        } else {
            // Added to support displaying the hourly activity bitmask for regular edge queries
            
            boolean[] hourlyActivity;
            try {
                hourlyActivity = decodeHourlyActivityToBooleanArray(value);
                if (hourlyActivity != null) {
                    List<Long> counts = new ArrayList<>();
                    for (boolean hourActive : hourlyActivity) {
                        counts.add(hourActive ? 1L : 0L);
                    }
                    edge.setCounts(counts);
                } else {
                    edge.setCounts(null);
                }
            } catch (InvalidProtocolBufferException ex) {
                log.error("invalid protocol buffer encountered when attempting to parse bitmask.");
            }
            
            if (edgeValue != null) {
                edge.setCount(edgeValue.getCount());
            }
        }
        
        if (edgeKey.hasAttribute2()) {
            edge.setEdgeAttribute2(edgeKey.getAttribute2());
        }
        if (edgeKey.hasAttribute3()) {
            edge.setEdgeAttribute3(edgeKey.getAttribute3());
        }
        return edge;
    }
    
    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        try {
            EdgeQueryResponseBase response = responseObjectFactory.getEdgeQueryResponse();
            
            Set<ColumnVisibility> uniqueColumnVisibilities = Sets.newHashSet();
            for (Object result : resultList) {
                EdgeBase edge = (EdgeBase) result;
                Map<String,String> markings = edge.getMarkings();
                uniqueColumnVisibilities.add(this.markingFunctions.translateToColumnVisibility(markings));
                response.addEdge(edge);
            }
            
            ColumnVisibility combinedVisibility = this.markingFunctions.combine(uniqueColumnVisibilities);
            response.setMarkings(this.markingFunctions.translateFromColumnVisibility(combinedVisibility));
            return response;
        } catch (Exception ex) {
            throw new RuntimeException("could not handle markings in resultList ", ex);
        }
    }
    
    @Override
    public List<CacheableQueryRow> writeToCache(Object o) throws QueryException {
        
        List<CacheableQueryRow> cqoList = new ArrayList<>();
        EdgeBase edge = (EdgeBase) o;
        
        CacheableQueryRow cqo = new CacheableQueryRowImpl();
        cqo.setColFam("");
        cqo.setDataType("");
        cqo.setEventId(generateEventId(edge));
        cqo.setRow("");
        
        if (edge.getSource() != null) {
            cqo.addColumn(EDGE_SOURCE, edge.getSource(), edge.getMarkings(), "", 0l);
        }
        if (edge.getSink() != null) {
            cqo.addColumn(EDGE_SINK, edge.getSink(), edge.getMarkings(), "", 0l);
        }
        if (edge.getEdgeType() != null) {
            cqo.addColumn(EDGE_TYPE, edge.getEdgeType(), edge.getMarkings(), "", 0l);
        }
        if (edge.getEdgeRelationship() != null) {
            cqo.addColumn(EDGE_RELATIONSHIP, edge.getEdgeRelationship(), edge.getMarkings(), "", 0l);
        }
        if (edge.getEdgeAttribute1Source() != null) {
            cqo.addColumn(EDGE_ATTRIBUTE1, edge.getEdgeAttribute1Source(), edge.getMarkings(), "", 0l);
        }
        if (edge.getStatsType() != null) {
            cqo.addColumn(STATS_EDGE, edge.getStatsType(), edge.getMarkings(), "", 0l);
        }
        if (edge.getEdgeAttribute2() != null) {
            cqo.addColumn(EDGE_ATTRIBUTE2, edge.getEdgeAttribute2(), edge.getMarkings(), "", 0l);
        }
        if (edge.getDate() != null) {
            cqo.addColumn(DATE, edge.getDate(), edge.getMarkings(), "", 0l);
        }
        if (edge.getCount() != null) {
            cqo.addColumn(COUNT, edge.getCount().toString(), edge.getMarkings(), "", 0l);
        }
        if (edge.getEdgeAttribute3() != null) {
            cqo.addColumn(EDGE_ATTRIBUTE3, edge.getEdgeAttribute3(), edge.getMarkings(), "", 0l);
        }
        List<Long> counts = edge.getCounts();
        if (counts != null && counts.size() > 0) {
            cqo.addColumn(COUNTS, StringUtils.join(counts, '\0'), edge.getMarkings(), "", 0l);
        }
        if (edge.getLoadDate() != null) {
            cqo.addColumn(LOAD_DATE, edge.getLoadDate(), edge.getMarkings(), "", 0l);
        }
        if (edge.getActivityDate() != null) {
            cqo.addColumn(ACTIVITY_DATE, edge.getActivityDate(), edge.getMarkings(), "", 0l);
        }
        cqoList.add(cqo);
        return cqoList;
    }
    
    @Override
    public List<Object> readFromCache(List<CacheableQueryRow> cacheableQueryRowList) {
        
        List<Object> edgeList = new ArrayList<>();
        
        for (CacheableQueryRow cqr : cacheableQueryRowList) {
            Map<String,String> markings = cqr.getMarkings();
            
            EdgeBase edge = (EdgeBase) responseObjectFactory.getEdge();
            
            edge.setMarkings(markings);
            
            Map<String,String> columnValues = cqr.getColumnValues();
            
            if (columnValues.containsKey(EDGE_SOURCE)) {
                edge.setSource(columnValues.get(EDGE_SOURCE));
            }
            if (columnValues.containsKey(EDGE_SINK)) {
                edge.setSink(columnValues.get(EDGE_SINK));
            }
            if (columnValues.containsKey(EDGE_TYPE)) {
                edge.setEdgeType(columnValues.get(EDGE_TYPE));
            }
            if (columnValues.containsKey(EDGE_RELATIONSHIP)) {
                edge.setEdgeRelationship(columnValues.get(EDGE_RELATIONSHIP));
            }
            if (columnValues.containsKey(EDGE_ATTRIBUTE1)) {
                edge.setEdgeAttribute1Source(columnValues.get(EDGE_ATTRIBUTE1));
            }
            if (columnValues.containsKey(STATS_EDGE)) {
                edge.setStatsType(columnValues.get(STATS_EDGE));
            }
            if (columnValues.containsKey(EDGE_ATTRIBUTE2)) {
                edge.setEdgeAttribute2(columnValues.get(EDGE_ATTRIBUTE2));
            }
            if (columnValues.containsKey(DATE)) {
                edge.setDate(columnValues.get(DATE));
            }
            if (columnValues.containsKey(COUNT)) {
                if (!columnValues.get(COUNT).isEmpty()) {
                    edge.setCount(Long.valueOf(columnValues.get(COUNT)));
                }
            }
            if (columnValues.containsKey(EDGE_ATTRIBUTE3)) {
                edge.setEdgeAttribute3(columnValues.get(EDGE_ATTRIBUTE3));
            }
            if (columnValues.containsKey(COUNTS)) {
                String countStr = columnValues.get(COUNTS);
                String[] countSplit = StringUtils.split(countStr, '\0');
                List<Long> countListAsLongs = new ArrayList<>();
                for (String s : countSplit) {
                    countListAsLongs.add(Long.valueOf(s));
                }
                edge.setCounts(countListAsLongs);
            }
            if (columnValues.containsKey(LOAD_DATE)) {
                edge.setLoadDate(columnValues.get(LOAD_DATE));
            }
            if (columnValues.containsKey(ACTIVITY_DATE)) {
                edge.setActivityDate(columnValues.get(ACTIVITY_DATE));
            }
            edgeList.add(edge);
        }
        
        return edgeList;
    }
    
    public String generateEventId(EdgeBase edge) {
        
        int hashCode = new HashCodeBuilder().append(this.getClass().getCanonicalName()).append(edge.getDate()).append(edge.getSource()).append(edge.getSink())
                        .append(edge.getEdgeRelationship()).toHashCode();
        
        return Integer.valueOf(hashCode).toString();
    }
}
