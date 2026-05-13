package datawave.query.discovery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import datawave.microservice.query.QueryImpl;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.microservice.query.Query;
import datawave.query.model.QueryModel;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

import static datawave.query.discovery.DiscoveryLogic.VALUES_ONLY;

public class DiscoveryTransformer extends BaseQueryLogicTransformer<DiscoveredThing,EventBase> implements CacheableLogic {
    private List<String> variableFieldList = null;
    private final BaseQueryLogic<DiscoveredThing> logic;
    private QueryModel myQueryModel;
    private MarkingFunctions markingFunctions;
    private final ResponseObjectFactory responseObjectFactory;
    private boolean valuesOnly = false;

    /**
     * Variant of a field list that contains only a value field.
     */
    BiFunction<DiscoveredThing,Map<String,String>,List<FieldBase<?>>> generateValuesOnlyFieldList = (x, y) -> {
        List<FieldBase<?>> fields = new ArrayList<>();

        fields.add(this.makeField("VALUE", y, "", 0L, x.getTerm()));

        return fields;
    };

    /**
     * Variant of field list that contains a standard, default set of fields.
     */
    BiFunction<DiscoveredThing,Map<String,String>,List<FieldBase<?>>> generateFieldList = (x, y) -> {
        List<FieldBase<?>> fields = new ArrayList<>();

        fields.add(this.makeField("VALUE", y, "", 0L, x.getTerm()));
        /*
         * Added query model to alias FIELD, if DiscoveredThing::field both not NULL and not empty.
         */
        Optional<String> fieldOFThing = Optional.ofNullable(x.getField());
        fieldOFThing.filter(i -> !i.isBlank()).ifPresent(i -> fields.add(this.makeField("FIELD", y, "", 0L, myQueryModel.aliasFieldNameReverseModel(i))));

        fields.add(this.makeField("DATE", y, "", 0L, x.getDate()));
        fields.add(this.makeField("DATA TYPE", y, "", 0L, x.getType()));

        // If requested return counts separated by colvis, all counts by colvis could be > total record count
        if (x.getCountsByColumnVisibility() != null && !x.getCountsByColumnVisibility().isEmpty()) {
            for (Map.Entry<Writable,Writable> entry : x.getCountsByColumnVisibility().entrySet()) {
                try {
                    Map<String,String> eMarkings = this.markingFunctions.translateFromColumnVisibility(new ColumnVisibility(entry.getKey().toString()));
                    fields.add(this.makeField("RECORD COUNT", new HashMap<>(), entry.getKey().toString(), 0L, entry.getValue().toString()));
                } catch (Exception e) {
                    throw new RuntimeException("could not parse to markings: " + x.getColumnVisibility());
                }

            }
        } else {
            fields.add(this.makeField("RECORD COUNT", y, "", 0L, Long.toString(x.getCount())));
        }
        return fields;
    };

    final Map<String, BiFunction<DiscoveredThing,Map<String,String>,List<FieldBase<?>>> > mapMapGenerator = new HashMap<>();
    {
        mapMapGenerator.put("VALUES_ONLY",generateValuesOnlyFieldList);
        mapMapGenerator.put("STANDARD",generateFieldList);
    }

    /**
     * Factory to get a  particular  field list generator. Different scenarios may call for different field lists.
     *
     * @param isValuesOnly
     * @return
     */
    BiFunction<DiscoveredThing,Map<String,String>,List<FieldBase<?>>> getMapGenerator(boolean isValuesOnly){
        return mapMapGenerator.get(isValuesOnly? "VALUES_ONLY": "STANDARD");
    }

    public DiscoveryTransformer(BaseQueryLogic<DiscoveredThing> logic, Query settings, QueryModel qm) {
        super(new MarkingFunctions.Default());
        this.markingFunctions = logic.getMarkingFunctions();
        this.responseObjectFactory = logic.getResponseObjectFactory();
        this.logic = logic;
        this.myQueryModel = qm;
        this.valuesOnly = getOrDefaultBoolean(settings, VALUES_ONLY, false);
    }

    @Override
    public EventBase transform(DiscoveredThing thing) {
        Preconditions.checkNotNull(thing, "Received a null object to transform!");

        EventBase event = this.responseObjectFactory.getEvent();

        Map<String,String> markings = markingsFromVisibility.apply(thing.getColumnVisibility());
        event.setMarkings(markings);

        List<FieldBase<?>> fields = getMapGenerator(valuesOnly).apply(thing, markings);
        event.setFields(fields);
        event.setSizeInBytes(fields.size() * 6L);

        Metadata metadata = new Metadata();
        metadata.setInternalId(""); // there is no UUID for a single index pointer
        metadata.setDataType(thing.getType()); // duplicate
        metadata.setRow(thing.getTerm()); // duplicate
        metadata.setTable(logic.getTableName());
        event.setMetadata(metadata);

        return event;
    }

    protected FieldBase<?> makeField(String name, Map<String,String> markings, String columnVisibility, Long timestamp, Object value) {
        FieldBase<?> field = this.responseObjectFactory.getField();
        field.setName(name);
        field.setMarkings(markings);
        field.setColumnVisibility(columnVisibility);
        field.setTimestamp(timestamp);
        field.setValue(value);
        return field;
    }

    @Override
    public BaseQueryResponse createResponse(List<Object> resultList) {
        EventQueryResponseBase response = this.responseObjectFactory.getEventQueryResponse();
        List<EventBase> eventList = new ArrayList<>();
        for (Object o : resultList) {
            EventBase e = (EventBase) o;
            eventList.add(e);
        }
        response.setFields(this.variableFieldList);
        response.setEvents(eventList);
        response.setReturnedEvents((long) eventList.size());
        return response;
    }

    @Override
    public CacheableQueryRow writeToCache(Object o) throws QueryException {
        EventBase event = (EventBase) o;

        CacheableQueryRow cqo = responseObjectFactory.getCacheableQueryRow();
        cqo.setMarkingFunctions(this.markingFunctions);
        Metadata metadata = event.getMetadata();
        cqo.setColFam(metadata.getDataType() + ":" + cqo.getEventId());
        cqo.setDataType(metadata.getDataType());
        cqo.setEventId(metadata.getInternalId());
        cqo.setRow(metadata.getRow());

        List<FieldBase> fields = event.getFields();
        for (FieldBase f : fields) {
            cqo.addColumn(f.getName(), f.getTypedValue(), f.getMarkings(), f.getColumnVisibility(), f.getTimestamp());
        }
        return cqo;
    }

    @Override
    public Object readFromCache(CacheableQueryRow cacheableQueryRow) {
        if (this.variableFieldList == null) {
            this.variableFieldList = cacheableQueryRow.getVariableColumnNames();
        }
        Map<String,String> markings = cacheableQueryRow.getMarkings();
        String dataType = cacheableQueryRow.getDataType();
        String internalId = cacheableQueryRow.getEventId();
        String row = cacheableQueryRow.getRow();

        EventBase event = this.responseObjectFactory.getEvent();

        event.setMarkings(markings);

        Metadata metadata = new Metadata();
        metadata.setDataType(dataType);
        metadata.setInternalId(internalId);
        metadata.setRow(row);
        metadata.setTable(logic.getTableName());
        event.setMetadata(metadata);

        List<FieldBase> fieldList = new ArrayList<>();
        Map<String,String> columnValueMap = cacheableQueryRow.getColumnValues();
        for (Map.Entry<String,String> entry : columnValueMap.entrySet()) {
            String columnName = entry.getKey();
            String columnValue = entry.getValue();
            String columnVisibility = cacheableQueryRow.getColumnVisibility(columnName);
            Long columnTimestamp = cacheableQueryRow.getColumnTimestamp(columnName);
            Map<String,String> columnMarkings = cacheableQueryRow.getColumnMarkings(columnName);
            FieldBase field = this.responseObjectFactory.getField();
            field.setName(columnName);
            field.setMarkings(columnMarkings);
            field.setColumnVisibility(columnVisibility);
            field.setTimestamp(columnTimestamp);
            field.setValue(columnValue);
            fieldList.add(field);
        }
        event.setFields(fieldList);
        return event;
    }

    Function<String,Map<String,String>> markingsFromVisibility = x -> {
        try {
            return this.markingFunctions.translateFromColumnVisibility(new ColumnVisibility(x));
        } catch (Exception e) {
            throw new RuntimeException("could not parse to markings: " + x);
        }
    };

    /**
     * If present, return the value of the given parameter from the given settings as a boolean, or return the default value otherwise.
     */
    private boolean getOrDefaultBoolean(Query settings, String parameterName, boolean defaultValue) {
        String value = getTrimmedParameter(settings, parameterName);
        return StringUtils.isBlank(value) ? defaultValue : Boolean.parseBoolean(value);
    }

    /**
     * Return the trimmed value of the given parameter from the given settings, or null if a value is not present.
     */
    private String getTrimmedParameter(Query settings, String parameterName) {
        QueryImpl.Parameter parameter = settings.findParameter(parameterName);
        return parameter != null ? parameter.getParameterValue().trim() : null;
    }

}
