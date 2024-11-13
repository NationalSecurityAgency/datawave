package datawave.ingest.data.config.ingest;

import com.google.common.collect.Multimap;

import datawave.ingest.data.config.NormalizedContentInterface;

/**
 * <p>
 * A marker interface that demonstrates an {@link IngestHelperInterface} must apply a filter to the event fields.
 * </p>
 *
 * @see datawave.ingest.data.config.ingest.IngestFieldFilter
 */
public interface FilterIngest {

    /**
     * Filters unnecessary fields from the given fields map.
     *
     * @param fields
     *            map of fields to filter
     */
    void filter(Multimap<String,NormalizedContentInterface> fields);
}
