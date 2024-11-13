package datawave.query.transformer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.edge.model.EdgeModelFields;
import datawave.edge.util.EdgeValue;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;

public abstract class EdgeQueryTransformerSupport<I,O> extends BaseQueryLogicTransformer<I,O> implements CacheableLogic {
    protected Authorizations auths;
    protected ResponseObjectFactory responseObjectFactory;
    protected EdgeModelFields fields;

    public EdgeQueryTransformerSupport(Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory, EdgeModelFields fields) {
        super(markingFunctions);
        this.responseObjectFactory = responseObjectFactory;
        auths = new Authorizations(settings.getQueryAuthorizations().split(","));
        this.fields = fields;
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
     *            a value
     * @return the hourly activity
     * @throws InvalidProtocolBufferException
     *             if the buffer is invalid
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
    public CacheableQueryRow writeToCache(Object o) throws QueryException {
        EdgeBase edge = (EdgeBase) o;

        CacheableQueryRow cqo = responseObjectFactory.getCacheableQueryRow();
        cqo.setMarkingFunctions(this.markingFunctions);
        cqo.setColFam("");
        cqo.setDataType("");
        cqo.setEventId(generateEventId(edge));
        cqo.setRow("");

        if (edge.getSource() != null) {
            cqo.addColumn(fields.getSourceFieldName(), edge.getSource(), edge.getMarkings(), "", 0l);
        }
        if (edge.getSink() != null) {
            cqo.addColumn(fields.getSinkFieldName(), edge.getSink(), edge.getMarkings(), "", 0l);
        }
        if (edge.getEdgeType() != null) {
            cqo.addColumn(fields.getTypeFieldName(), edge.getEdgeType(), edge.getMarkings(), "", 0l);
        }
        if (edge.getEdgeRelationship() != null) {
            cqo.addColumn(fields.getRelationshipFieldName(), edge.getEdgeRelationship(), edge.getMarkings(), "", 0l);
        }
        if (edge.getEdgeAttribute1Source() != null) {
            cqo.addColumn(fields.getAttribute1FieldName(), edge.getEdgeAttribute1Source(), edge.getMarkings(), "", 0l);
        }
        if (edge.getStatsType() != null) {
            cqo.addColumn(fields.getStatsEdgeFieldName(), edge.getStatsType(), edge.getMarkings(), "", 0l);
        }
        if (edge.getEdgeAttribute2() != null) {
            cqo.addColumn(fields.getAttribute2FieldName(), edge.getEdgeAttribute2(), edge.getMarkings(), "", 0l);
        }
        if (edge.getDate() != null) {
            cqo.addColumn(fields.getDateFieldName(), edge.getDate(), edge.getMarkings(), "", 0l);
        }
        if (edge.getCount() != null) {
            cqo.addColumn(fields.getCountFieldName(), edge.getCount().toString(), edge.getMarkings(), "", 0l);
        }
        if (edge.getEdgeAttribute3() != null) {
            cqo.addColumn(fields.getAttribute3FieldName(), edge.getEdgeAttribute3(), edge.getMarkings(), "", 0l);
        }
        List<Long> counts = edge.getCounts();
        if (counts != null && !counts.isEmpty()) {
            cqo.addColumn(fields.getCountsFieldName(), StringUtils.join(counts, '\0'), edge.getMarkings(), "", 0l);
        }
        if (edge.getLoadDate() != null) {
            cqo.addColumn(fields.getLoadDateFieldName(), edge.getLoadDate(), edge.getMarkings(), "", 0l);
        }
        if (edge.getActivityDate() != null) {
            cqo.addColumn(fields.getActivityDateFieldName(), edge.getActivityDate(), edge.getMarkings(), "", 0l);
        }
        return cqo;
    }

    @Override
    public Object readFromCache(CacheableQueryRow cacheableQueryRow) {
        Map<String,String> markings = cacheableQueryRow.getMarkings();

        EdgeBase edge = (EdgeBase) responseObjectFactory.getEdge();

        edge.setMarkings(markings);

        Map<String,String> columnValues = cacheableQueryRow.getColumnValues();

        if (columnValues.containsKey(fields.getSourceFieldName())) {
            edge.setSource(columnValues.get(fields.getSourceFieldName()));
        }
        if (columnValues.containsKey(fields.getSinkFieldName())) {
            edge.setSink(columnValues.get(fields.getSinkFieldName()));
        }
        if (columnValues.containsKey(fields.getTypeFieldName())) {
            edge.setEdgeType(columnValues.get(fields.getTypeFieldName()));
        }
        if (columnValues.containsKey(fields.getRelationshipFieldName())) {
            edge.setEdgeRelationship(columnValues.get(fields.getRelationshipFieldName()));
        }
        if (columnValues.containsKey(fields.getAttribute1FieldName())) {
            edge.setEdgeAttribute1Source(columnValues.get(fields.getAttribute1FieldName()));
        }
        if (columnValues.containsKey(fields.getStatsEdgeFieldName())) {
            edge.setStatsType(columnValues.get(fields.getStatsEdgeFieldName()));
        }
        if (columnValues.containsKey(fields.getAttribute2FieldName())) {
            edge.setEdgeAttribute2(columnValues.get(fields.getAttribute2FieldName()));
        }
        if (columnValues.containsKey(fields.getDateFieldName())) {
            edge.setDate(columnValues.get(fields.getDateFieldName()));
        }
        if (columnValues.containsKey(fields.getCountFieldName())) {
            if (!columnValues.get(fields.getCountFieldName()).isEmpty()) {
                edge.setCount(Long.valueOf(columnValues.get(fields.getCountFieldName())));
            }
        }
        if (columnValues.containsKey(fields.getAttribute3FieldName())) {
            edge.setEdgeAttribute3(columnValues.get(fields.getAttribute3FieldName()));
        }
        if (columnValues.containsKey(fields.getCountsFieldName())) {
            String countStr = columnValues.get(fields.getCountsFieldName());
            String[] countSplit = StringUtils.split(countStr, '\0');
            List<Long> countListAsLongs = new ArrayList<>();
            for (String s : countSplit) {
                countListAsLongs.add(Long.valueOf(s));
            }
            edge.setCounts(countListAsLongs);
        }
        if (columnValues.containsKey(fields.getLoadDateFieldName())) {
            edge.setLoadDate(columnValues.get(fields.getLoadDateFieldName()));
        }
        if (columnValues.containsKey(fields.getActivityDateFieldName())) {
            edge.setActivityDate(columnValues.get(fields.getActivityDateFieldName()));
        }
        return edge;
    }

    public String generateEventId(EdgeBase edge) {

        int hashCode = new HashCodeBuilder().append(this.getClass().getCanonicalName()).append(edge.getDate()).append(edge.getSource()).append(edge.getSink())
                        .append(edge.getEdgeRelationship()).toHashCode();

        return Integer.toString(hashCode);
    }
}
