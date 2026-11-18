package datawave.query.discovery;

import static datawave.query.discovery.DiscoveryLogic.VALUES_ONLY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.model.QueryModel;
import datawave.visibility.AccessExpressionUtil;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;

@SuppressWarnings({"rawtypes"})
public class DiscoveryTransformer extends BaseQueryLogicTransformer<DiscoveredThing,EventBase> implements CacheableLogic {
    private List<String> variableFieldList = null;
    private final BaseQueryLogic<DiscoveredThing> logic;
    private QueryModel myQueryModel;
    private final ResponseObjectFactory responseObjectFactory;
    private boolean valuesOnly = false;

    public DiscoveryTransformer(BaseQueryLogic<DiscoveredThing> logic, Query settings, QueryModel qm) {
        super();
        this.responseObjectFactory = logic.getResponseObjectFactory();
        this.logic = logic;
        this.myQueryModel = qm;
        this.valuesOnly = getOrDefaultBoolean(settings, VALUES_ONLY, false);
    }

    /**
     * Variant of a field list that contains only a value field.
     */
    BiFunction<DiscoveredThing,String,List<FieldBase>> generateValuesOnlyFieldList = (x, y) -> {
        List<FieldBase> fields = new ArrayList<>();

        fields.add(this.makeField("VALUE", y, 0L, x.getTerm()));

        return fields;
    };

    /**
     * Variant of field list that contains a standard, default set of fields.
     */
    BiFunction<DiscoveredThing,String,List<FieldBase>> generateFieldList = (x, y) -> {
        List<FieldBase> fields = new ArrayList<>();

        fields.add(this.makeField("VALUE", y, 0L, x.getTerm()));
        /*
         * Added query model to alias FIELD, if DiscoveredThing::field both not NULL and not empty.
         */
        Optional<String> fieldOFThing = Optional.ofNullable(x.getField());
        fieldOFThing.filter(i -> !i.isBlank()).ifPresent(i -> fields.add(this.makeField("FIELD", y, 0L, myQueryModel.aliasFieldNameReverseModel(i))));

        fields.add(this.makeField("DATE", y, 0L, x.getDate()));
        fields.add(this.makeField("DATA TYPE", y, 0L, x.getType()));

        // If requested return counts separated by colvis, all counts by colvis could be > total record count
        if (x.getCountsByColumnVisibility() != null && !x.getCountsByColumnVisibility().isEmpty()) {
            for (Map.Entry<Writable,Writable> entry : x.getCountsByColumnVisibility().entrySet()) {
                String recordCountVisibility = entry.getKey().toString();
                try {
                    // validate the visibility
                    new ColumnVisibility(recordCountVisibility);
                } catch (RuntimeException e) {
                    throw new RuntimeException("could not parse visibility: " + recordCountVisibility, e);
                }
                fields.add(this.makeField("RECORD COUNT", recordCountVisibility, 0L, entry.getValue().toString()));
            }
        } else {
            fields.add(this.makeField("RECORD COUNT", y, 0L, Long.toString(x.getCount())));
        }
        return fields;
    };

    final Map<String,BiFunction<DiscoveredThing,String,List<FieldBase>>> mapMapGenerator = new HashMap<>();
    {
        mapMapGenerator.put("VALUES_ONLY", generateValuesOnlyFieldList);
        mapMapGenerator.put("STANDARD", generateFieldList);
    }

    /**
     * Factory to get a particular field list generator. Different scenarios may call for different field lists.
     *
     * @param isValuesOnly
     * @return
     */
    BiFunction<DiscoveredThing,String,List<FieldBase>> getMapGenerator(boolean isValuesOnly) {
        return mapMapGenerator.get(isValuesOnly ? "VALUES_ONLY" : "STANDARD");
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public EventBase transform(DiscoveredThing thing) {
        Preconditions.checkNotNull(thing, "Received a null object to transform!");

        EventBase event = this.responseObjectFactory.getEvent();

        String visibility = normalizedVisibility.apply(thing.getColumnVisibility());
        event.setColumnVisibility(visibility);

        List<FieldBase> fields = getMapGenerator(valuesOnly).apply(thing, visibility);
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

    @SuppressWarnings({"rawtypes"})
    protected FieldBase makeField(String name, String columnVisibility, Long timestamp, Object value) {
        FieldBase field = this.responseObjectFactory.getField();
        field.setName(name);
        field.setColumnVisibility(columnVisibility);
        field.setTimestamp(timestamp);
        field.setValue(value);
        return field;
    }

    @SuppressWarnings({"rawtypes"})
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

    @SuppressWarnings({"unchecked"})
    @Override
    public CacheableQueryRow writeToCache(Object o) {
        EventBase event = (EventBase) o;

        CacheableQueryRow cqo = responseObjectFactory.getCacheableQueryRow();
        Metadata metadata = event.getMetadata();
        cqo.setColFam(metadata.getDataType() + ":" + cqo.getEventId());
        cqo.setDataType(metadata.getDataType());
        cqo.setEventId(metadata.getInternalId());
        cqo.setRow(metadata.getRow());

        List<FieldBase> fields = event.getFields();
        for (FieldBase f : fields) {
            // TODO remove-accumulo-utils: CacheableQueryRow visibility addColumn
            cqo.addColumn(f.getName(), f.getTypedValue(), null, f.getColumnVisibility(), f.getTimestamp());
        }
        return cqo;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public Object readFromCache(CacheableQueryRow cacheableQueryRow) {
        if (this.variableFieldList == null) {
            this.variableFieldList = cacheableQueryRow.getVariableColumnNames();
        }
        String dataType = cacheableQueryRow.getDataType();
        String internalId = cacheableQueryRow.getEventId();
        String row = cacheableQueryRow.getRow();

        EventBase event = this.responseObjectFactory.getEvent();

        event.setColumnVisibility(cacheableQueryRow.getColumnVisibility());

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
            FieldBase field = this.responseObjectFactory.getField();
            field.setName(columnName);
            field.setColumnVisibility(columnVisibility);
            field.setTimestamp(columnTimestamp);
            field.setValue(columnValue);
            fieldList.add(field);
        }
        event.setFields(fieldList);
        return event;
    }

    Function<String,String> normalizedVisibility = x -> {
        try {
            return AccessExpressionUtil.normalize(new ColumnVisibility(x)).getExpression();
        } catch (Exception e) {
            throw new RuntimeException("could not parse visibility: " + x, e);
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
