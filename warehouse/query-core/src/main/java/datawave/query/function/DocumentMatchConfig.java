package datawave.query.function;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import datawave.query.predicate.TimeFilter;

/**
 * Configuration used to build the document-match context lookup function that runs immediately before JEXL evaluation.
 */
public class DocumentMatchConfig {
    private SortedKeyValueIterator<Key,Value> source;
    private TimeFilter timeFilter;
    private DocumentMatchContext.Limits limits;
    private boolean tld;

    public SortedKeyValueIterator<Key,Value> getSource() {
        return source;
    }

    public void setSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }

    public TimeFilter getTimeFilter() {
        return timeFilter;
    }

    public void setTimeFilter(TimeFilter timeFilter) {
        this.timeFilter = timeFilter;
    }

    public DocumentMatchContext.Limits getLimits() {
        return limits;
    }

    public void setLimits(DocumentMatchContext.Limits limits) {
        this.limits = limits;
    }

    public boolean isTld() {
        return tld;
    }

    public void setTld(boolean tld) {
        this.tld = tld;
    }
}
