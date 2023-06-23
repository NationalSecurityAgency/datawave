package datawave.query.transformer;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.core.query.cachedresults.CacheableLogic;
import datawave.edge.model.EdgeModelFields;
import datawave.edge.util.EdgeKey;
import datawave.edge.util.EdgeValue;
import datawave.edge.util.EdgeValueHelper;
import datawave.marking.MarkingFunctions;
import datawave.util.time.DateHelper;
import datawave.webservice.query.Query;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class EdgeQueryTransformer extends EdgeQueryTransformerSupport<Entry<Key,Value>,EdgeBase> implements CacheableLogic {
    private Logger log = Logger.getLogger(EdgeQueryTransformer.class);

    public EdgeQueryTransformer(Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory, EdgeModelFields fields) {
        super(settings, markingFunctions, responseObjectFactory, fields);
    }

    @Override
    public EdgeBase transform(Entry<Key,Value> entry) {

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
}
