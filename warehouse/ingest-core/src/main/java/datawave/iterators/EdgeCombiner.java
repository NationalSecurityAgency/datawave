package datawave.iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner.VarLenEncoder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.edge.protobuf.EdgeData;
import datawave.edge.util.EdgeKey;
import datawave.edge.util.EdgeKey.STATS_TYPE;
import datawave.edge.util.EdgeKeyDecoder;
import datawave.edge.util.EdgeValue;
import datawave.edge.util.EdgeValue.EdgeValueBuilder;
import datawave.edge.util.EdgeValueHelper;

/**
 * Combines edges from different values based on the edge type found in the key.
 *
 * Note: the {@link datawave.edge.util.EdgeValueHelper} class correctly combines old style varint array values with new style protocol buffer values. This will
 * always write protocol buffers as the value
 *
 */
public class EdgeCombiner extends Combiner {

    static final Logger log = Logger.getLogger(EdgeCombiner.class);
    private final Text colFam = new Text();
    private final Text colQual = new Text();

    /**
     * Reduces a list of Values into a single Value.
     *
     * @param key
     *            The most recent version of the Key being reduced.
     *
     * @param iter
     *            An iterator over the Values for different versions of the key.
     *
     * @return The combined Value.
     */
    @Override
    public Value reduce(Key key, Iterator<Value> iter) {
        Value combinedValue = null;
        if (log.isTraceEnabled())
            log.trace("Running Edge Combiner for : " + key);

        key.getColumnFamily(colFam);
        EdgeKey.EDGE_FORMAT edgeFormat = EdgeKeyDecoder.determineEdgeFormat(colFam);

        switch (edgeFormat) {
            case STANDARD:
                combinedValue = combineStandardKey(key, iter);
                break;
            case STATS:
                STATS_TYPE statsType = EdgeKeyDecoder.determineStatsType(colFam);
                combinedValue = combineStatsKey(statsType, key, iter);
                break;
            case UNKNOWN:
                break;
        }
        return combinedValue;
    }

    private Value combineStandardKey(Key key, Iterator<Value> iter) {

        EdgeValueBuilder builder = EdgeValue.newBuilder();
        int combineCount = 0;
        while (iter.hasNext()) {
            Value value = iter.next();
            try {
                EdgeData.EdgeValue protoEdgeValue = EdgeData.EdgeValue.parseFrom(value.get());

                if (protoEdgeValue.hasCount()) {
                    builder.setCount(protoEdgeValue.getCount() + builder.getCount());
                }

                if (protoEdgeValue.hasHourBitmask()) {
                    builder.combineBitmask(protoEdgeValue.getHourBitmask());
                }

                useEarliestLoadDate(key, builder, protoEdgeValue);
                combineSourceAndSink(builder, protoEdgeValue);
                useEarliestUuid(builder, protoEdgeValue);
                combineBadActivityDate(builder, protoEdgeValue);
            } catch (InvalidProtocolBufferException e) {
                // Try to decode an old varint value
                long count = new VarLenEncoder().decode(value.get());
                builder.setCount(builder.getCount() + count);
            }
            combineCount++;
        }
        if (log.isTraceEnabled())
            log.debug("Combined " + combineCount + " values.");
        return builder.build().encode();
    }

    private Value combineStatsKey(STATS_TYPE statsType, Key key, Iterator<Value> iter) {
        // If this is a STATS link count edge, merge all the values into a single one.
        if (STATS_TYPE.LINKS == statsType) {
            return (StatsLinksEdgeCombiner.combineStatsLinksEdgeValues(key, iter));
        }

        EdgeValueBuilder builder = EdgeValue.newBuilder();

        List<Long> combinedList = new ArrayList<>();
        while (iter.hasNext()) {
            Value value = iter.next();
            try {
                EdgeData.EdgeValue protoEdgeValue = EdgeData.EdgeValue.parseFrom(value.get());
                useEarliestLoadDate(key, builder, protoEdgeValue);
                combineSourceAndSink(builder, protoEdgeValue);
                useEarliestUuid(builder, protoEdgeValue);
                combineBadActivityDate(builder, protoEdgeValue);
                combineHistogram(statsType, builder, combinedList, protoEdgeValue); // already decoded the value
            } catch (InvalidProtocolBufferException e) {
                // value wasn't previously a protobuf, so we don't get the source or sink

                // combine the stats hours/duration with the raw value
                combineHistogramFromLegacyValue(statsType, builder, combinedList, value);
            }
        }

        return builder.build().encode();
    }

    /**
     * Determines the oldest load date and updates the builder with it
     *
     * @param key
     *            a key
     * @param builder
     *            will be updated by this method with the oldest load date
     * @param protoEdgeValue
     *            the current value, decoded
     */
    private void useEarliestLoadDate(Key key, EdgeValueBuilder builder, EdgeData.EdgeValue protoEdgeValue) {
        String loadDate = builder.getLoadDate();
        if (protoEdgeValue.hasLoadDate()) {
            if (null == loadDate || loadDate.compareTo(protoEdgeValue.getLoadDate()) > 0) {
                builder.setLoadDate(protoEdgeValue.getLoadDate());
            }
        } else if (null == loadDate) {
            builder.setLoadDate(getDateFromKey(key));
        }
    }

    private String getDateFromKey(Key key) {
        key.getColumnQualifier(colQual);
        return EdgeKeyDecoder.getYYYYMMDD(colQual);
    }

    private void combineHistogram(STATS_TYPE statsType, EdgeValueBuilder builder, List<Long> combinedList, EdgeData.EdgeValue protoEdgeValue) {
        if (STATS_TYPE.ACTIVITY == statsType) {
            List<Long> sourceList = EdgeValueHelper.decodeActivityHistogram(protoEdgeValue.getHoursList());
            EdgeValueHelper.combineHistogram(sourceList, combinedList);
            builder.setHours(combinedList);
        } else if (STATS_TYPE.DURATION == statsType) {
            List<Long> sourceList = EdgeValueHelper.decodeDurationHistogram(protoEdgeValue.getDurationList());
            EdgeValueHelper.combineHistogram(sourceList, combinedList);
            builder.setDuration(combinedList);
        }
    }

    private void combineHistogramFromLegacyValue(STATS_TYPE statsType, EdgeValueBuilder builder, List<Long> combinedList, Value value) {
        if (STATS_TYPE.ACTIVITY == statsType) {
            List<Long> sourceList = EdgeValueHelper.decodeActivityHistogram(EdgeValueHelper.getVarLongList(value.get()));
            EdgeValueHelper.combineHistogram(sourceList, combinedList);
            builder.setHours(combinedList);
        } else if (STATS_TYPE.DURATION == statsType) {
            List<Long> sourceList = EdgeValueHelper.decodeDurationHistogram(EdgeValueHelper.getVarLongList(value.get()));
            EdgeValueHelper.combineHistogram(sourceList, combinedList);
            builder.setDuration(combinedList);
        }
    }

    private void combineSourceAndSink(EdgeValueBuilder builder, EdgeData.EdgeValue protoEdgeValue) {
        if (StringUtils.isBlank(builder.getSourceValue()) && protoEdgeValue.hasSourceValue()) {
            builder.setSourceValue(protoEdgeValue.getSourceValue());
        }
        if (StringUtils.isBlank(builder.getSinkValue()) && protoEdgeValue.hasSinkValue()) {
            builder.setSinkValue(protoEdgeValue.getSinkValue());
        }
    }

    private void useEarliestUuid(EdgeValueBuilder builder, EdgeData.EdgeValue protoEdgeValue) {
        // Keeps overriding value of 'uuid' so the last(earliest) one will always be used
        // the value corresponding to the key with the most recent timestamp will come first
        // the value corresponding to the key with the oldest timestamp will come last
        if (protoEdgeValue.hasUuid()) {
            builder.setUuidObj(EdgeValue.convertUuidObject(protoEdgeValue.getUuid()));
            builder.setOnlyUuidString(false);
        } else if (protoEdgeValue.hasUuidString()) {
            builder.setOnlyUuidString(true);
            builder.setUuid(protoEdgeValue.getUuidString());
        }
    }

    private void combineBadActivityDate(EdgeValueBuilder builder, EdgeData.EdgeValue protoEdgeValue) {
        // Only set the bad activity flag if one of the edges to be combined contains the bad activity flag.
        // This should only happen with the new EVENT_ONLY date type edges
        if (protoEdgeValue.hasBadActivity()) {
            if (builder.badActivityDateSet()) {
                // If one of the activity dates is good then the edge will be treated as good
                // They all must be bad for it to be treated as a bad activity date.
                builder.setBadActivityDate(builder.isBadActivityDate() && protoEdgeValue.getBadActivity());
            } else {
                builder.setBadActivityDate(protoEdgeValue.getBadActivity());
            }
        }
    }
}
