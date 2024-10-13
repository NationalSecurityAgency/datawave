package datawave.ingest.mapreduce.handler.edge.define;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import datawave.edge.util.EdgeKey;
import datawave.edge.util.EdgeValue.EdgeValueBuilder;
import datawave.edge.util.EdgeValueHelper;
import datawave.edge.util.ExtendedHyperLogLogPlus;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.edge.define.VertexValue.ValueType;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.util.time.DateHelper;

/**
 * Combines an EdgeDefinition with values obtained from Event data.
 *
 */
public class EdgeDataBundle {

    private static final Logger log = Logger.getLogger(EdgeDataBundle.class);

    // Input/Setup variables
    // final so you're not tempted to change them
    private final RawRecordContainer event;
    private VertexValue source;
    private VertexValue sink;
    private EdgeDefinition edgeDefinition = null;
    private EdgeDirection edgeDirection = EdgeDirection.UNIDIRECTIONAL;

    private String sourceMaskedValue = null;
    private boolean hasMaskedSource = false;
    private String sinkMaskedValue = null;
    private boolean hasMaskedSink = false;

    // Variables for output
    private boolean requiresMasking = false;
    private boolean isDeleting = false;
    private String edgeType;
    private String enrichedIndex;
    private String enrichedValue;
    private String edgeAttribute3;
    private String edgeAttribute2;

    private long eventDate = 0;
    private IngestHelperInterface helper;
    private Map<String,String> markings = null;
    private ColumnVisibility maskedVisibility = null;
    private boolean forceMaskedVisibility = false; // if this is a masked event, but the
    // the fields defined aren't masked, use the unmasked visibility

    // Duration value if defined
    private DurationValue durationValue = null;

    private String loadDate;
    private MarkingFunctions mf = null;
    private String uuid;
    private long activityDate;
    private boolean validActivityDate;

    private EdgeKey.DATE_TYPE dateType;

    public EdgeDataBundle(RawRecordContainer event, String typeName, String id, IngestHelperInterface helper) {
        this.mf = MarkingFunctions.Factory.createMarkingFunctions();
        this.event = event;
        this.eventDate = event.getDate();
        this.edgeType = typeName;
        this.uuid = id;
        this.helper = helper;
    }

    public EdgeDataBundle(EdgeDefinition edgeDef, NormalizedContentInterface ifaceSource, NormalizedContentInterface ifaceSink, RawRecordContainer event,
                    IngestHelperInterface helper) {
        this(event, edgeDef.getEdgeType().toString(), null, helper);

        this.setSource(new VertexValue(edgeDef.isUseRealm(), edgeDef.getSourceIndexedFieldRealm(), edgeDef.getSourceEventFieldRealm(),
                        edgeDef.getSourceRelationship(), edgeDef.getSourceCollection(), ifaceSource));
        this.setSink(new VertexValue(edgeDef.isUseRealm(), edgeDef.getSinkIndexedFieldRealm(), edgeDef.getSourceEventFieldRealm(),
                        edgeDef.getSinkRelationship(), edgeDef.getSinkCollection(), ifaceSink));
        this.edgeDefinition = edgeDef;

        this.edgeDirection = edgeDef.getDirection();
        if (event.getAltIds() != null && !event.getAltIds().isEmpty()) {
            this.uuid = event.getAltIds().iterator().next();
        }
        // even though event, etc references are saved above, passing in the event
        // prevents future bug
        this.initFieldMasking(helper, event);
        this.initMarkings(getSource().getMarkings(), getSink().getMarkings());
    }

    public EdgeDataBundle(RawRecordContainer event) {
        this.mf = MarkingFunctions.Factory.createMarkingFunctions();
        this.event = event;

    }

    public void clearNonEventFields() {
        this.source = null;
        this.sink = null;
    }

    private int getHour(long time) {
        /* Calculate the Hour for the event datetime */
        Calendar cal = Calendar.getInstance();
        if (0 != time) {
            cal.setTimeInMillis(time);
            return cal.get(Calendar.HOUR_OF_DAY);
        } else {
            return -1;
        }
    }

    public void initFieldMasking(IngestHelperInterface helper, RawRecordContainer event) {
        this.setHelper(helper);
        this.requiresMasking = event.isRequiresMasking();
        if (!this.requiresMasking) {
            return;
        }
        if (helper.isEmbeddedHelperMaskedFieldHelper()) {
            MaskedFieldHelper maskedFieldHelper = helper.getEmbeddedHelperAsMaskedFieldHelper();
            initVertexMasking(this.getSource(), maskedFieldHelper);
            initVertexMasking(this.getSink(), maskedFieldHelper);
            if ((!this.getSource().hasMaskedValue()) && (!this.getSink().hasMaskedValue())) {
                this.requiresMasking = false;
            }
        } else {
            this.requiresMasking = false;
        }
    }

    private void initVertexMasking(final VertexValue vertex, final MaskedFieldHelper maskedFieldHelper) {
        final String fieldName = vertex.getFieldName();
        if (maskedFieldHelper.contains(fieldName)) {
            final String maskedValue = helper.getNormalizedMaskedValue(fieldName);
            vertex.setMaskedValue(maskedValue);
            vertex.setHasMaskedValue(true);
        }
    }

    public void setMaskedVisibility(ColumnVisibility maskedVisibility) {
        this.maskedVisibility = maskedVisibility;
    }

    @SuppressWarnings("unchecked")
    public void initMarkings(Map<String,String> m1, Map<String,String> m2) {
        if (m1 != null) {
            if (m2 != null) {
                try {
                    this.markings = mf.combine(m1, m2);
                } catch (Exception e) {
                    throw new RuntimeException("Unable to combine markings", e);
                }
            } else {
                this.markings = m1;
            }
        } else if (m2 != null) {
            this.markings = m2;
        }
    }

    public int getDuration() {
        return null == this.getDurationValue() ? -1 : this.getDurationValue().getDuration();
    }

    public void setEdgeAttribute3(String edgeAttribute3) {
        this.edgeAttribute3 = edgeAttribute3;
    }

    public String getEdgeAttribute3() {
        return this.edgeAttribute3;
    }

    public void setEdgeAttribute2(String edgeAttribute2) {
        this.edgeAttribute2 = edgeAttribute2;
    }

    public String getEdgeAttribute2() {
        return this.edgeAttribute2;
    }

    public Value getEdgeValue(EdgeKey.DATE_TYPE date_type) {
        return getEdgeValue(true, date_type);
    }

    public Value getEdgeValue(boolean forwardEdge, EdgeKey.DATE_TYPE date_type) {
        EdgeValueBuilder builder = datawave.edge.util.EdgeValue.newBuilder();
        int hour = -1;

        if (date_type == EdgeKey.DATE_TYPE.ACTIVITY_ONLY || date_type == EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT) {
            hour = getHour(activityDate);
        } else {
            hour = getHour(eventDate);
        }

        if (date_type == EdgeKey.DATE_TYPE.EVENT_ONLY) {
            if (validActivityDate) {
                builder.setBadActivityDate(false);
            } else {
                builder.setBadActivityDate(true);
            }

        }
        // Set counts
        if (!this.isDeleting()) {
            builder.setCount(1l);
        } else {
            builder.setCount(-1l);
        }
        // Set Hour Bitmask
        if (hour != -1) {
            builder.setHour(hour);
        }
        if (forwardEdge == true) {
            builder.setSourceValue(source.getValue(ValueType.EVENT));
            builder.setSinkValue(sink.getValue(ValueType.EVENT));
        } else {
            builder.setSourceValue(sink.getValue(ValueType.EVENT));
            builder.setSinkValue(source.getValue(ValueType.EVENT));
        }
        builder.setLoadDate(loadDate);
        builder.setUuid(uuid);
        return builder.build().encode();
    }

    public Value getStatsActivityValue(EdgeKey.DATE_TYPE date_type) {
        return getStatsActivityValue(true, date_type);
    }

    public Value getStatsActivityValue(boolean forwardEdge, EdgeKey.DATE_TYPE date_type) {
        EdgeValueBuilder builder = datawave.edge.util.EdgeValue.newBuilder();
        int hour = -1;

        if (date_type == EdgeKey.DATE_TYPE.ACTIVITY_ONLY || date_type == EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT) {
            hour = getHour(activityDate);
        } else {
            hour = getHour(eventDate);
        }

        if (date_type == EdgeKey.DATE_TYPE.EVENT_ONLY) {
            if (validActivityDate) {
                builder.setBadActivityDate(false);
            } else {
                builder.setBadActivityDate(true);
            }

        }
        List<Long> hours = EdgeValueHelper.getLongListForHour(hour, this.isDeleting());
        builder.setHours(hours);
        if (forwardEdge == true) {
            builder.setSourceValue(source.getValue(ValueType.EVENT));
        } else {
            builder.setSourceValue(sink.getValue(ValueType.EVENT));
        }
        builder.setLoadDate(loadDate);
        builder.setUuid(uuid);
        return builder.build().encode();
    }

    public void initDuration(NormalizedContentInterface uptimeNCI, NormalizedContentInterface downtimeNCI) {
        this.setDurationValue(new DurationValue(uptimeNCI, downtimeNCI));

    }

    public void initDuration(NormalizedContentInterface elapsedTimeNCI) {
        this.setDurationValue(new DurationValue(elapsedTimeNCI));
    }

    public DurationValue getDurationValue() {
        return this.durationValue;
    }

    public Value getDurationAsValue() {
        return getDurationAsValue(true);
    }

    public Value getDurationAsValue(boolean forwardEdge) {
        EdgeValueBuilder builder = datawave.edge.util.EdgeValue.newBuilder();
        List<Long> duration = EdgeValueHelper.getLongListForDuration(this.getDuration(), this.isDeleting());
        builder.setDuration(duration);
        if (forwardEdge == true) {
            builder.setSourceValue(source.getValue(ValueType.EVENT));
        } else {
            builder.setSourceValue(sink.getValue(ValueType.EVENT));
        }
        builder.setLoadDate(loadDate);
        builder.setUuid(uuid);
        return builder.build().encode();
    }

    public boolean hasDuration() {
        return (this.getDurationValue() != null);
    }

    public boolean isValid() {
        if (null == getSource().getIndexedFieldValue() || null == getSink().getIndexedFieldValue() || getSource().getIndexedFieldValue().isEmpty()
                        || getSink().getIndexedFieldValue().isEmpty()) {
            return false;
        }

        // check masked values
        if (getSource().hasMaskedValue()) {
            if (null == getSource().getMaskedValue() || getSource().getMaskedValue().isEmpty()) {
                this.setRequiresMasking(false);
            }
        } else {
            if (null == getSource().getIndexedFieldValue() || getSource().getIndexedFieldValue().isEmpty()) {
                this.setRequiresMasking(false);
            }
        }
        if (getSink().hasMaskedValue()) {
            if (null == getSink().getMaskedValue() || getSink().getMaskedValue().isEmpty()) {
                this.setRequiresMasking(false);
            }
        } else {
            if (null == getSink().getIndexedFieldValue() || getSink().getIndexedFieldValue().isEmpty()) {
                this.setRequiresMasking(false);
            }
        }
        // TODO verify that no edge created for invalid enrichment.
        if (this.edgeDefinition.isEnrichmentEdge()) {
            if (null == this.getEnrichedValue() || this.getEnrichedValue().isEmpty()) {
                return false;
            }
        }

        return true;
    }

    public String getYyyyMMdd(EdgeKey.DATE_TYPE date_type) {
        if (date_type == EdgeKey.DATE_TYPE.ACTIVITY_ONLY) {
            return DateHelper.format(this.activityDate);
        } else {
            return DateHelper.format(this.eventDate);
        }
    }

    public String getUuid() {
        return this.uuid;
    }

    public String getEnrichedValue() {
        return enrichedValue;
    }

    public void setEnrichedValue(String enrichedValue) {
        this.enrichedValue = enrichedValue;
    }

    public boolean requiresMasking() {
        return requiresMasking;
    }

    public void setRequiresMasking(boolean requiresMasking) {
        this.requiresMasking = requiresMasking;
    }

    public boolean isDeleting() {
        return isDeleting;
    }

    public void setIsDeleting(boolean isDeleting) {
        this.isDeleting = isDeleting;
    }

    public String getEdgeType() {
        return edgeType;
    }

    public void setEdgeType(String edgeType) {
        this.edgeType = edgeType;
    }

    public VertexValue getSource() {
        return source;
    }

    public void setSource(VertexValue source) {
        this.source = source;
    }

    public VertexValue getSink() {
        return sink;
    }

    public void setSink(VertexValue sink) {
        this.sink = sink;
    }

    public Map<String,String> getMarkings() {
        return this.markings;
    }

    public void setMarkings(Map<String,String> markings) {
        this.markings = markings;
    }

    public RawRecordContainer getEvent() {
        return event;
    }

    public EdgeDirection getEdgeDirection() {
        return edgeDirection;
    }

    public void setEdgeDirection(EdgeDirection edgeDirection) {
        this.edgeDirection = edgeDirection;
    }

    public void setDurationValue(DurationValue durationValue) {
        this.durationValue = durationValue;
    }

    public ColumnVisibility getMaskedVisibility() {
        return maskedVisibility;
    }

    public IngestHelperInterface getHelper() {
        return helper;
    }

    public void setHelper(IngestHelperInterface helper) {
        this.helper = helper;
    }

    public long getEventDate() {
        return this.eventDate;
    }

    public void setEventDate(long newDate) {
        this.eventDate = newDate;
    }

    public boolean getForceMaskedVisibility() {
        return forceMaskedVisibility;
    }

    public void setForceMaskedVisibility(boolean forceMaskedVisibility) {
        this.forceMaskedVisibility = forceMaskedVisibility;
    }

    public void setEnrichedIndex(String enrichedIndex) {
        this.enrichedIndex = enrichedIndex;
    }

    public String getEnrichedIndex() {
        return this.enrichedIndex;
    }

    public boolean hasMaskedSink() {
        return hasMaskedSink;
    }

    public void setHasMaskedSink(boolean hasMaskedSink) {
        this.hasMaskedSink = hasMaskedSink;
    }

    public boolean hasMaskedSource() {
        return hasMaskedSource;
    }

    public void setHasMaskedSource(boolean hasMaskedSource) {
        this.hasMaskedSource = hasMaskedSource;
    }

    public String getSourceMaskedValue() {
        return sourceMaskedValue;
    }

    public void setSourceMaskedValue(String sourceMaskedValue) {
        this.sourceMaskedValue = sourceMaskedValue;
    }

    public String getSinkMaskedValue() {
        return sinkMaskedValue;
    }

    public void setSinkMaskedValue(String sinkMaskedValue) {
        this.sinkMaskedValue = sinkMaskedValue;
    }

    public String getDataTypeName() {
        return helper.getType().typeName();
    }

    public EdgeDefinition getEdgeDefinition() {
        return edgeDefinition;
    }

    public String getLoadDate() {
        return loadDate;
    }

    public void setLoadDate(String loadDate) {
        this.loadDate = loadDate;
    }

    public long getActivityDate() {
        return activityDate;
    }

    public void setActivityDate(long activityDate) {
        this.activityDate = activityDate;
    }

    public boolean isValidActivityDate() {
        return validActivityDate;
    }

    public void setValidActivityDate(boolean validActivityDate) {
        this.validActivityDate = validActivityDate;
    }

    @Override
    public String toString() {
        return "EdgeValue [source=" + source + ", sink=" + sink + ", edgeDefinition=" + edgeDefinition + ", edgeDirection=" + edgeDirection
                        + ", requiresMasking=" + requiresMasking + ", isDeleting=" + isDeleting + ", edgeType=" + edgeType + ", enrichedValue=" + enrichedValue
                        + ", yyyyMMdd=" + getYyyyMMdd(EdgeKey.DATE_TYPE.EVENT_ONLY) + ", hour=" + getHour(eventDate) + ", eventDate=" + eventDate
                        + ", validActivityDate=" + validActivityDate + ", activityDate=" + activityDate + ", helper=" + helper + ", markings=" + markings
                        + ", maskedVisibility=" + maskedVisibility + ", forceMaskedVisibility=" + forceMaskedVisibility + ", durationValue=" + durationValue
                        + ", loadDate=" + loadDate + "]";
    }

    /**
     * Create a STATS link count edge value initialized with a single identifier.
     *
     * @param realmedIdentifier
     *            The identifier to add to the STATS link count edge value.
     * @return A Value or null if there was an error.
     */
    public static Value getStatsLinkValue(final String realmedIdentifier) {
        try {
            final ExtendedHyperLogLogPlus hll = new ExtendedHyperLogLogPlus();

            hll.offer(realmedIdentifier);

            return (new Value(hll.getBytes()));
        } catch (IOException e) {
            log.warn("Failed to add " + realmedIdentifier + " to HyperLogLog", e);

            return (null);
        }
    }

    public void setDateType(EdgeKey.DATE_TYPE dateType) {
        this.dateType = dateType;
    }

    public void setEdgeDefinition(EdgeDefinition edgeDef) {
        this.edgeDefinition = edgeDef;
        this.edgeDirection = edgeDef.getDirection();
        this.edgeType = edgeDef.getEdgeType().toString();
    }

    public void setUUID(String uuid) {
        this.uuid = uuid;

    }

    public EdgeKey.DATE_TYPE getDateType() {
        return this.dateType;
    }

} /* end EdgeValue */
