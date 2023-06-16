package datawave.query.predicate;

import java.util.Map.Entry;

import datawave.query.attributes.Document;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;

/**
 * Remove Documents which have no entries. This might occur from a user being excluded from seeing a Document due to the ColumnVisibility, or from the filtering
 * of attributes via a projection.
 *
 *
 *
 */
public class EmptyDocumentFilter implements Predicate<Entry<Key,Document>> {
    private static final Logger log = Logger.getLogger(EmptyDocumentFilter.class);

    @Override
    public boolean apply(Entry<Key,Document> input) {
        boolean nonempty = (input.getValue().size() > 0);

        if (log.isTraceEnabled())
            log.trace("Testing exclusion" + input.getValue());
        // If we have data, and trace logging it turned on
        if (!nonempty && log.isDebugEnabled()) {
            log.debug("Excluding empty Document: " + input.getKey());
        }

        // Return false when we have no results
        return nonempty;
    }

}
