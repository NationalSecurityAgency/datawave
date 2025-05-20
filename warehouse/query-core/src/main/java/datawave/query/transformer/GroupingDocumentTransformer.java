package datawave.query.transformer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

import datawave.core.query.logic.BaseQueryLogic;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.query.model.QueryModel;
import datawave.query.tables.ShardQueryLogic;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

public class GroupingDocumentTransformer extends DocumentTransformer {

    private static final Logger log = Logger.getLogger(GroupingDocumentTransformer.class);

    private List<String> groupFieldsList;
    private Map<String,FieldBase<?>> fieldMap = Maps.newHashMap();

    public GroupingDocumentTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory, Collection<String> groupFieldsSet) {
        super(logic, settings, markingFunctions, responseObjectFactory);
        createGroupFieldsList(groupFieldsSet);
    }

    public GroupingDocumentTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory, Collection<String> groupFieldsSet, Boolean reducedResponse) {
        super(logic, settings, markingFunctions, responseObjectFactory, reducedResponse);
        createGroupFieldsList(groupFieldsSet);
    }

    public GroupingDocumentTransformer(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory,
                    Collection<String> groupFieldsSet, Boolean reducedResponse) {
        super(tableName, settings, markingFunctions, responseObjectFactory, reducedResponse);
        createGroupFieldsList(groupFieldsSet);
    }

    public void createGroupFieldsList(Collection<String> groupFieldsSet) {
        this.groupFieldsList = Lists.newArrayList(groupFieldsSet);
        QueryModel model = ((ShardQueryLogic) logic).getQueryModel();
        for (String groupField : groupFieldsSet) {
            String f = model.getReverseAliasForField(groupField);
            if (f != null && !f.isEmpty()) {
                this.groupFieldsList.add(f);
            }
        }
    }

    @Override
    /**
     * count the desired fields and create a new response with one event.
     */
    public BaseQueryResponse createResponse(List<Object> resultList) {

        Multiset<Collection<FieldBase<?>>> multiset = HashMultiset.create();
        if (log.isTraceEnabled())
            log.trace("groupFieldsList:" + groupFieldsList);
        for (Object o : resultList) {
            if (log.isTraceEnabled())
                log.trace("processing " + o);
            this.getListKeyCounts((EventBase) o, multiset);
        }
        return createGroupedResponse(multiset);
    }

    protected BaseQueryResponse createGroupedResponse(Multiset<Collection<FieldBase<?>>> multiset) {
        Map<String,String> markings = Maps.newHashMap();
        EventQueryResponseBase response = this.responseObjectFactory.getEventQueryResponse();
        List<EventBase> events = new ArrayList<>();
        for (Collection<FieldBase<?>> entry : multiset.elementSet()) {
            EventBase event = this.responseObjectFactory.getEvent();
            event.setMarkings(markings);
            List<FieldBase<?>> fields = new ArrayList(entry);
            FieldBase<?> counter = this.responseObjectFactory.getField();
            counter.setName("COUNT");
            counter.setMarkings(markings);
            counter.setValue(multiset.count(entry));
            counter.setTimestamp(0L);
            fields.add(counter);
            event.setFields(fields);
            events.add(event);
        }
        response.setEvents(events);
        response.setTotalEvents((long) events.size());
        response.setReturnedEvents((long) events.size());
        return response;
    }

    private Multimap<String,String> getFieldToFieldWithGroupingContextMap(Collection<FieldBase<?>> fields, Set<String> expandedGroupFieldsList) {
        Multimap<String,String> fieldToFieldWithContextMap = TreeMultimap.create();
        for (FieldBase<?> field : fields) {
            if (log.isTraceEnabled())
                log.trace("field is " + field);
            String fieldName = field.getName();
            String shortName = fieldName;
            String shorterName = shortName;
            if (shortName.indexOf('.') != -1)
                shortName = shortName.substring(0, shortName.lastIndexOf('.'));
            if (shorterName.indexOf('.') != -1)
                shorterName = shorterName.substring(0, shorterName.indexOf('.'));
            if (log.isTraceEnabled())
                log.trace("fieldName:" + fieldName + ", shortName:" + shortName);
            if (this.groupFieldsList.contains(shorterName)) {
                expandedGroupFieldsList.add(shortName);
                if (log.isTraceEnabled())
                    log.trace(this.groupFieldsList + " contains " + shorterName);
                FieldBase<?> created = null;
                try {
                    created = this.makeField(shortName, this.markingFunctions.translateFromColumnVisibility(new ColumnVisibility(field.getColumnVisibility())),
                                    field.getColumnVisibility(), 0L, field.getValueOfTypedValue());
                } catch (Exception ex) {
                    log.error(ex);
                }
                if (created != null) {
                    if (fieldMap.containsKey(fieldName)) {
                        log.warn("fieldMap already has " + fieldMap.get(fieldName) + " for " + fieldName);
                    }
                    fieldMap.put(fieldName, created);
                    if (log.isTraceEnabled())
                        log.trace("put " + created + " into fieldMap for key:" + fieldName);
                }
                fieldToFieldWithContextMap.put(shortName, fieldName);
            } else {
                if (log.isTraceEnabled())
                    log.trace(this.groupFieldsList + " does not contain " + shorterName);
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("fields:" + fields);
            log.trace("fieldToFieldWithGroupingContextMap:" + fieldToFieldWithContextMap);
            log.trace("expandedGroupFieldsList:" + expandedGroupFieldsList);
        }
        return fieldToFieldWithContextMap;
    }

    private int longestValueList(Multimap<String,String> in) {
        int max = 0;
        for (Collection<String> valueCollection : in.asMap().values()) {
            max = Math.max(max, valueCollection.size());
        }
        return max;
    }

    private void getListKeyCounts(EventBase e, Multiset<Collection<FieldBase<?>>> multiset) {

        Set<String> expandedGroupFieldsList = new LinkedHashSet<>();
        List<FieldBase<?>> fields = e.getFields();
        Multimap<String,String> fieldToFieldWithContextMap = this.getFieldToFieldWithGroupingContextMap(fields, expandedGroupFieldsList);
        if (log.isTraceEnabled())
            log.trace("got a new fieldToFieldWithContextMap:" + fieldToFieldWithContextMap);
        int longest = this.longestValueList(fieldToFieldWithContextMap);
        for (int i = 0; i < longest; i++) {
            Collection<FieldBase<?>> fieldCollection = Sets.newHashSet();
            for (String fieldListItem : expandedGroupFieldsList) {
                if (log.isTraceEnabled())
                    log.trace("fieldListItem:" + fieldListItem);
                Collection<String> gtNames = fieldToFieldWithContextMap.get(fieldListItem);
                if (gtNames == null || gtNames.isEmpty()) {
                    if (log.isTraceEnabled()) {
                        log.trace("gtNames:" + gtNames);
                        log.trace("fieldToFieldWithContextMap:" + fieldToFieldWithContextMap + " did not contain " + fieldListItem);
                    }
                    continue;
                } else {
                    String gtName = gtNames.iterator().next();
                    if (fieldListItem.equals(gtName) == false) {
                        fieldToFieldWithContextMap.remove(fieldListItem, gtName);
                    }
                    if (log.isTraceEnabled()) {
                        log.trace("fieldToFieldWithContextMap now:" + fieldToFieldWithContextMap);
                        log.trace("gtName:" + gtName);
                    }
                    fieldCollection.add(this.fieldMap.get(gtName));
                }
            }
            if (fieldCollection.size() == expandedGroupFieldsList.size()) {
                multiset.add(fieldCollection);
                if (log.isTraceEnabled())
                    log.trace("added fieldList to the map:" + fieldCollection);
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("fieldList.size() != this.expandedGroupFieldsList.size()");
                    log.trace("fieldList:" + fieldCollection);
                    log.trace("expandedGroupFieldsList:" + expandedGroupFieldsList);
                }
            }
        }
        if (log.isTraceEnabled())
            log.trace("map:" + multiset);
    }
}
