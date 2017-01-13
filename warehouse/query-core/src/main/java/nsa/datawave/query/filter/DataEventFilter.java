package nsa.datawave.query.filter;

import nsa.datawave.query.parser.EventFields;

/**
 * <p>
 * {@link DataEventFilter} is an abstract implementation to filter out results returned by an iterator.
 * </p>
 */
public abstract class DataEventFilter extends DataFilter {
    /**
     * Given a populated, {@link EventFields} object, {@link #accept(EventFields)} determines if the event should be returned to the caller or be filtered out
     * and not returned.
     * 
     * @param event
     *            The current aggregated {@link EventFields}
     * @return True if the value should be returned to the caller, false otherwise
     */
    public boolean accept(EventFields event) {
        return true;
    }
}
