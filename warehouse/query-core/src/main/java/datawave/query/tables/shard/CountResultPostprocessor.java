package datawave.query.tables.shard;

import static datawave.query.transformer.ShardQueryCountTableTransformer.COUNT_CELL;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.core.query.logic.ResultPostprocessor;
import datawave.marking.MarkingFunctions;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;

public class CountResultPostprocessor implements ResultPostprocessor {
    private static final Logger log = Logger.getLogger(CountResultPostprocessor.class);

    private final MarkingFunctions markingFunctions;

    public CountResultPostprocessor(MarkingFunctions markingFunctions) {
        this.markingFunctions = markingFunctions;
    }

    @Override
    public void apply(List<Object> results) {
        if (results.size() > 1) {
            EventBase firstResult = null;
            Long count = 0L;
            Set<ColumnVisibility> columnVisibilities = Sets.newHashSet();

            boolean success = true;
            List<Object> resultsToRemove = new ArrayList<>();
            for (Object result : results) {
                if (result instanceof EventBase) {
                    EventBase event = (EventBase) result;

                    // save the first result
                    if (firstResult == null) {
                        firstResult = event;
                    }

                    // aggregate the count, and column visibility
                    FieldBase<?> countField = getCountField(event.getFields());
                    if (countField != null) {
                        columnVisibilities.add(new ColumnVisibility(countField.getColumnVisibility()));
                        if (countField.getTypedValue().getDataType().isAssignableFrom(Long.class)) {
                            count += ((Number) countField.getValueOfTypedValue()).longValue();

                            if (event != firstResult) {
                                resultsToRemove.add(event);
                            }
                        } else {
                            success = false;
                            break;
                        }
                    } else {
                        success = false;
                        break;
                    }
                } else {
                    success = false;
                    break;
                }
            }

            if (success) {
                ColumnVisibility columnVisibility = null;
                try {
                    columnVisibility = markingFunctions.combine(columnVisibilities);
                } catch (Exception e) {
                    log.error("Could not create combined columnVisibilities for the count", e);
                }

                if (columnVisibility != null) {
                    results.removeAll(resultsToRemove);

                    // update the first result
                    FieldBase<?> countField = getCountField(firstResult.getFields());
                    countField.setValue(count);
                    countField.setColumnVisibility(columnVisibility);
                }
            }
        }
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
