package datawave.query.tables.shard;

import static datawave.query.transformer.ShardQueryCountTableTransformer.COUNT_CELL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.ResultPostprocessor;
import datawave.marking.MarkingFunctions;
import datawave.query.Constants;
import datawave.query.config.CountingQueryConfiguration;
import datawave.query.language.functions.jexl.Count;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.query.result.event.ResponseObjectFactory;

public class CountResultPostprocessor implements ResultPostprocessor {
    private static final Logger log = Logger.getLogger(CountResultPostprocessor.class);

    private final MarkingFunctions markingFunctions;
    private final ResponseObjectFactory responseObjectFactory;

    private long resultCount = 0;
    private ColumnVisibility vis = new ColumnVisibility();

    public CountResultPostprocessor(ResponseObjectFactory factory, MarkingFunctions markingFunctions) {
        this.responseObjectFactory = factory;
        this.markingFunctions = markingFunctions;
    }

    @Override
    public void apply(List<Object> results, boolean flushed) {
        if (flushed) {
            return;
        }

        Set<ColumnVisibility> visibilities = new HashSet<>();
        visibilities.add(this.vis);
        for (Object result : results) {
            if (result instanceof EventBase) {
                EventBase event = (EventBase) result;

                // aggregate the count, and column visibility
                FieldBase<?> countField = getCountField(event.getFields());
                if (countField != null) {
                    if (countField.getTypedValue().getDataType().isAssignableFrom(Long.class)) {
                        resultCount += ((Number) countField.getValueOfTypedValue()).longValue();
                        visibilities.add(new ColumnVisibility(countField.getColumnVisibility()));
                    }
                }
            }
        }
        try {
            this.vis = markingFunctions.combine(visibilities);
        } catch (MarkingFunctions.Exception e) {
            throw new RuntimeException("Unable to combine column visibilities", e);
        }
        results.clear();
    }

    @Override
    public void saveState(GenericQueryConfiguration config) {
        CountingQueryConfiguration cConfig = (CountingQueryConfiguration) config;
        cConfig.setResultCount(cConfig.getResultCount() + resultCount);
        if (cConfig.getResultVis() == null) {
            cConfig.setResultVis(vis);
        } else {
            Set<ColumnVisibility> visibilities = new HashSet<>();
            visibilities.add(this.vis);
            visibilities.add(cConfig.getResultVis());
            try {
                cConfig.setResultVis(markingFunctions.combine(visibilities));
            } catch (MarkingFunctions.Exception e) {
                throw new RuntimeException("Unable to combine column visibilitis", e);
            }
        }
        vis = new ColumnVisibility();
        resultCount = 0;
    }

    @Override
    public Iterator<Object> flushResults(GenericQueryConfiguration config) {
        Authorizations auths = new Authorizations(config.getQuery().getQueryAuthorizations().split(","));

        Map<String,String> markings;
        try {
            markings = markingFunctions.translateFromColumnVisibilityForAuths(vis, auths);
        } catch (MarkingFunctions.Exception e1) {
            throw new IllegalArgumentException("Unable to translate markings", e1);
        }

        EventBase e = this.responseObjectFactory.getEvent();
        e.setMarkings(markings);

        FieldBase field = this.makeField(COUNT_CELL, markings, vis, System.currentTimeMillis(), resultCount);
        e.setMarkings(markings);

        List<FieldBase> fields = new ArrayList<>();
        fields.add(field);
        e.setFields(fields);

        Metadata metadata = new Metadata();
        metadata.setDataType(Constants.EMPTY_STRING);
        metadata.setInternalId(field.getName()); // There is only one item returned for the entire query logic.
        metadata.setRow(Constants.EMPTY_STRING);
        e.setMetadata(metadata);

        Collection<Object> events = Collections.singleton(e);
        return events.iterator();
    }

    private FieldBase makeField(String name, Map<String,String> markings, ColumnVisibility columnVisibility, Long timestamp, Object value) {
        FieldBase field = this.responseObjectFactory.getField();
        field.setName(name);
        field.setMarkings(markings);
        field.setColumnVisibility(columnVisibility);
        field.setTimestamp(timestamp);
        field.setValue(value);
        return field;
    }

    private FieldBase<?> getCountField(List<FieldBase<?>> fields) {
        FieldBase<?> countField = null;
        for (FieldBase<?> field : fields) {
            if (field.getName().equals(COUNT_CELL)) {
                countField = field;
                break;
            }
        }
        return countField;
    }
}
