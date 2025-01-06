package datawave.query.transformer;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.esotericsoftware.kryo.io.Input;

import datawave.core.query.cachedresults.CacheableLogic;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctions.Exception;
import datawave.microservice.query.Query;
import datawave.query.Constants;
import datawave.query.parser.EventFields;
import datawave.query.parser.EventFields.FieldValue;
import datawave.util.StringUtils;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;

public class EventQueryTransformer extends EventQueryTransformerSupport<Entry<Key,Value>,EventBase> implements CacheableLogic {

    public EventQueryTransformer(String tableName, Query settings, MarkingFunctions markingFunctions, ResponseObjectFactory responseObjectFactory) {
        super(tableName, settings, markingFunctions, responseObjectFactory);
    }

    public EventQueryTransformer(BaseQueryLogic<Entry<Key,Value>> logic, Query settings, MarkingFunctions markingFunctions,
                    ResponseObjectFactory responseObjectFactory) {
        super(logic, settings, markingFunctions, responseObjectFactory);
    }

    @Override
    public EventBase transform(Entry<Key,Value> entry) {

        Key key = entry.getKey();
        Value val = entry.getValue();
        if (entry.getKey() == null && entry.getValue() == null)
            return null;

        if (null == entry.getKey() || null == entry.getValue()) {
            throw new IllegalArgumentException("Null key or value. Key:" + entry.getKey() + ", Value: " + entry.getValue());
        }
        EventBase event = this.responseObjectFactory.getEvent();

        Map<String,String> markings = null;
        try {
            markings = this.markingFunctions.translateFromColumnVisibilityForAuths(new ColumnVisibility(key.getColumnVisibility()), this.auths);
        } catch (Exception e) {
            log.error("could not translate " + key.getColumnVisibility() + " to markings, skipping entry");
            return null;
        }
        if (null == markings || markings.isEmpty()) {
            // can't process this one because we did not have valid security markings
            log.error("Transformer visibility interpreter was null, skipping entry");
            return null;
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(entry.getValue().get());
        Input i = new Input(bais);
        eventFields = kryo.readObject(i, EventFields.class);

        i.close();

        String row = entry.getKey().getRow().toString();
        String colf = entry.getKey().getColumnFamily().toString();
        String colq = entry.getKey().getColumnQualifier().toString();
        // if the column qualifier is set, then we have returned an alternate event from the one that was
        // evaluated by using the returnUidMapper (@see datawave.core.iterators.EvaluatingIterator: aggregateAltEvent)
        if (!colq.equals("")) {
            colf = colq;
        }
        int sepIndex = colf.indexOf(Constants.NULL_BYTE_STRING);
        String baseUid = colf.substring(sepIndex + 1);

        Set<FieldBase<?>> values = new HashSet<>();

        String origFieldName = null;
        String fieldName = null;

        // Hold unique Column Visibilities and merge them at the end
        // for the overall event ColumnVisibility.
        Set<ColumnVisibility> visibilitiesToMerge = new HashSet<>();

        for (Entry<String,Collection<FieldValue>> e : eventFields.asMap().entrySet()) {
            origFieldName = e.getKey();
            if (this.qm != null) {
                fieldName = this.qm.aliasFieldNameReverseModel(origFieldName);
            } else {
                fieldName = origFieldName;
            }

            for (FieldValue fv : e.getValue()) {
                visibilitiesToMerge.add(fv.getVisibility());
                try {
                    Map<String,String> fieldMarkings = this.markingFunctions.translateFromColumnVisibility(fv.getVisibility());
                    String value = new String(fv.getValue(), Charset.forName("UTF-8"));
                    // if this is a content field name, then replace the value with the uid
                    if (getContentFieldNames().contains(fieldName)) {
                        value = baseUid;
                    }
                    values.add(this.makeField(fieldName, fieldMarkings, new String(fv.getVisibility().getExpression()), entry.getKey().getTimestamp(), value));
                } catch (Exception e1) {
                    throw new RuntimeException("could not make markings from: " + fv.getVisibility());
                }
            }
        }

        ColumnVisibility columnVisibility = null;
        try {
            columnVisibility = this.markingFunctions.combine(visibilitiesToMerge);
            event.setMarkings(this.markingFunctions.translateFromColumnVisibility(columnVisibility));
        } catch (Exception e1) {
            throw new RuntimeException("could not make markings from: " + columnVisibility);
        }
        event.setFields(new ArrayList<>(values));

        Metadata metadata = new Metadata();
        String[] colfParts = StringUtils.split(colf, '\0');
        if (colfParts.length >= 1) {
            metadata.setDataType(colfParts[0]);
        }

        if (colfParts.length >= 2) {
            metadata.setInternalId(colfParts[1]);
        }

        if (this.tableName != null) {
            metadata.setTable(this.tableName);
        }
        metadata.setRow(row);
        event.setMetadata(metadata);

        if (eventQueryDataDecoratorTransformer != null) {
            event = (EventBase<?,?>) eventQueryDataDecoratorTransformer.transform(event);
        }

        // assign an estimate of the event size
        // in practice this is about 6 times the size of the kryo bytes
        event.setSizeInBytes(entry.getValue().getSize() * 6);

        return event;
    }
}
