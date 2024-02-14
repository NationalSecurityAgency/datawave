package datawave.ingest.mapreduce.handler.edge;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.normalizer.DateNormalizer;
import datawave.edge.util.EdgeKey;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.GroupedNormalizedContentInterface;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.normalizer.SimpleGroupFieldNameParser;
import datawave.ingest.mapreduce.EventMapper;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDataBundle;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinitionConfigurationHelper;
import datawave.ingest.time.Now;
import datawave.util.time.DateHelper;

public class EdgeEventFieldUtil {

    private static final Now now = Now.getInstance();

    private static final Logger log = LoggerFactory.getLogger(EdgeEventFieldUtil.class);

    protected Multimap<String,NormalizedContentInterface> normalizedFields;
    protected Map<String,Multimap<String,NormalizedContentInterface>> depthFirstList;

    SimpleGroupFieldNameParser fieldParser = new SimpleGroupFieldNameParser();
    protected boolean trimFieldGroup;

    public EdgeEventFieldUtil(boolean trimFieldGroup) {
        this.trimFieldGroup = trimFieldGroup;
        normalizedFields = HashMultimap.create();
        depthFirstList = new HashMap<>();
    }

    public void normalizeAndGroupFields(Multimap<String,NormalizedContentInterface> fields) {
        Multimap<String,NormalizedContentInterface> tmp = null;
        for (Map.Entry<String,NormalizedContentInterface> e : fields.entries()) {
            NormalizedContentInterface value = e.getValue();
            String subGroup = null;
            if (value instanceof GroupedNormalizedContentInterface) {
                subGroup = ((GroupedNormalizedContentInterface) value).getSubGroup();
            }
            String fieldName = getGroupedFieldName(value);
            tmp = depthFirstList.get(fieldName);
            if (tmp == null) {
                tmp = HashMultimap.create();
            }
            tmp.put(subGroup, value);
            depthFirstList.put(fieldName, tmp);

            normalizedFields.put(fieldName, value);
        }
    }

    public Multimap<String,NormalizedContentInterface> getNormalizedFields() {
        return this.normalizedFields;
    }

    public Map<String,Multimap<String,NormalizedContentInterface>> getDepthFirstList() {
        return this.depthFirstList;
    }

    protected String getGroupedFieldName(NormalizedContentInterface value) {
        String fieldName = value.getIndexedFieldName();
        if (value instanceof GroupedNormalizedContentInterface) {
            GroupedNormalizedContentInterface grouped = (GroupedNormalizedContentInterface) value;
            if (grouped.isGrouped() && grouped.getGroup() != null) {
                if (!grouped.getGroup().isEmpty()) {
                    String group;
                    if (trimFieldGroup) {
                        group = fieldParser.getTrimmedGroup(grouped.getGroup());
                    } else {
                        group = grouped.getGroup();
                    }
                    fieldName = fieldName + '.' + group;

                }
            }
        }
        return fieldName;
    }

    private long getActivityDate(Multimap<String,NormalizedContentInterface> normalizedFields, EdgeDefinitionConfigurationHelper edgeDefConfigs) {
        if (normalizedFields.containsKey(edgeDefConfigs.getActivityDateField())) {
            String actDate = normalizedFields.get(edgeDefConfigs.getActivityDateField()).iterator().next().getEventFieldValue();
            try {
                return DateNormalizer.parseDate(actDate, DateNormalizer.FORMAT_STRINGS).getTime();
            } catch (ParseException e1) {
                log.error("Parse exception when getting the activity date: " + actDate + " for edge creation " + e1.getMessage());
            }
        }
        return -1L;
    }

    /*
     * validates the activity date using the past and future delta configured variables both past and future deltas are expected to be positive numbers (in
     * milliseconds)
     */
    protected boolean validateActivityDate(long activityTime, long eventTime, long pastDelta, long futureDelta) {

        if (eventTime - activityTime > pastDelta) {
            // if activity > event then number is negative and will be checked in the next else if statement
            return false;
        } else if (activityTime - eventTime > futureDelta) {
            // if activity < event then number is negative and would have been checked by the previous if statement
            return false;
        } else {
            return true;
        }
    }

    /*
     * Compares activity and event time. Returns true if they are both on the same day. Eg. both result in the same yyyyMMdd string
     */
    protected boolean compareActivityAndEvent(long activityDate, long eventDate) {
        // The date toString() returns dates in the format yyyy-mm-dd
        if (DateHelper.format(activityDate).equals(DateHelper.format(eventDate))) {
            return true;
        } else {
            return false;
        }
    }

    private String getLoadDateString(Multimap<String,NormalizedContentInterface> fields) {
        String loadDateStr;
        Collection<NormalizedContentInterface> loadDates = fields.get(EventMapper.LOAD_DATE_FIELDNAME);
        if (!loadDates.isEmpty()) {
            NormalizedContentInterface nci = loadDates.iterator().next();
            Date date = new Date(Long.parseLong(nci.getEventFieldValue()));
            loadDateStr = DateHelper.format(date);
        } else {
            // If fields does not include the load date then use the current system time as load date
            loadDateStr = DateHelper.format(new Date(now.get()));
        }
        return loadDateStr;
    }

    private String getEdgeAttr3(EdgeDefinitionConfigurationHelper edgeDefConfigs) {
        // get the edgeAttribute3 from the event fields map
        if (normalizedFields.containsKey(edgeDefConfigs.getEdgeAttribute3())) {
            return normalizedFields.get(edgeDefConfigs.getEdgeAttribute3()).iterator().next().getIndexedFieldValue();
        }
        return null;
    }

    private String getEdgeAttr2(EdgeDefinitionConfigurationHelper edgeDefConfigs) {
        // get the edgeAttribute2 from the event fields map
        if (normalizedFields.containsKey(edgeDefConfigs.getEdgeAttribute2())) {
            return normalizedFields.get(edgeDefConfigs.getEdgeAttribute2()).iterator().next().getIndexedFieldValue();
        }
        return null;
    }

    private NormalizedContentInterface getNullKeyedNCI(String fieldValue, Multimap<String,NormalizedContentInterface> fields) {
        Iterator<NormalizedContentInterface> nciIter = fields.get(fieldValue).iterator();
        if (nciIter.hasNext()) {
            return nciIter.next();
        }
        return null;
    }

    public void setEdgeDuration(EdgeDefinition edgeDef, EdgeDataBundle edgeDataBundle) {
        if (edgeDef.getUDDuration()) {
            NormalizedContentInterface upnci = getNullKeyedNCI(edgeDef.getUpTime(), normalizedFields);
            NormalizedContentInterface downnci = getNullKeyedNCI(edgeDef.getDownTime(), normalizedFields);
            if (null != upnci && null != downnci) {
                edgeDataBundle.initDuration(upnci, downnci);
            }
        } else {
            NormalizedContentInterface elnci = getNullKeyedNCI(edgeDef.getElapsedTime(), normalizedFields);
            if (null != elnci) {
                edgeDataBundle.initDuration(elnci);
            }
        }
    }

    protected void setEdgeKeyDateType(EdgeDataBundle bundle, boolean validActivtyDate, boolean sameActivityDate, long eventDate, long newFormatStartDate) {
        EdgeKey.DATE_TYPE dateType;
        if (eventDate < newFormatStartDate) {
            dateType = EdgeKey.DATE_TYPE.OLD_EVENT;
        } else if (validActivtyDate) {
            if (sameActivityDate) {
                dateType = EdgeKey.DATE_TYPE.ACTIVITY_AND_EVENT;
            } else {
                dateType = EdgeKey.DATE_TYPE.ACTIVITY_ONLY;
                // also need to write EVENT_ONLY for some reason
            }
        } else {
            dateType = EdgeKey.DATE_TYPE.EVENT_ONLY;
        }
        bundle.setDateType(dateType);
    }

    public EdgeDataBundle setEdgeInfoFromEventFields(EdgeDataBundle bundle, EdgeDefinitionConfigurationHelper edgeDefConfigs, RawRecordContainer event,
                    EdgeIngestConfiguration edgeConfig, long newFormatStartDate, Configuration conf, String typeName) {

        // Get the load date of the event from the fields map
        String loadDateStr = getLoadDateString(normalizedFields);
        long activityDate = getActivityDate(normalizedFields, edgeDefConfigs);

        // get the activity date from the event fields map
        boolean validActivityDate = validateActivityDate(activityDate, event.getDate(), edgeConfig.getPastDelta(), edgeConfig.getFutureDelta());
        boolean activityEqualsEvent = false;

        // If the activity date is valid check to see if it is on the same day as the event date
        if (validActivityDate) {
            activityEqualsEvent = compareActivityAndEvent(activityDate, event.getDate());
        }

        if (event.getAltIds() != null && !event.getAltIds().isEmpty()) {
            bundle.setUUID(event.getAltIds().iterator().next());
        }

        setEdgeKeyDateType(bundle, validActivityDate, activityEqualsEvent, event.getDate(), newFormatStartDate);

        bundle.setLoadDate(loadDateStr);
        bundle.setActivityDate(activityDate);
        bundle.setEventDate(event.getDate());
        bundle.setValidActivityDate(validActivityDate);
        bundle.setEdgeAttribute2(getEdgeAttr2(edgeDefConfigs));
        bundle.setEdgeAttribute3(getEdgeAttr3(edgeDefConfigs));
        bundle.setRequiresMasking(event.isRequiresMasking());
        bundle.setHelper(event.getDataType().getIngestHelper(conf));

        return bundle;
    }
}
