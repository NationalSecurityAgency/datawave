package datawave.query.iterator;

import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Predicate;

import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.core.iterators.querylock.QueryLock;
import datawave.query.function.DocumentPermutation;
import datawave.query.function.Equality;
import datawave.query.function.serializer.DocumentSerializer;
import datawave.query.iterator.filter.KeyIdentity;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.TimeFilter;
import datawave.query.statsd.QueryStatsDClient;
import datawave.query.util.sortedset.FileSortedSet;

/**
 * QueryOptionsMixin adds the @JsonIgnore decorator to fields in
 * the main class for testing. This is specifically useful for checking if
 * default options are present for any new query options, keeping
 * the original QueryOptions class nice and clean!
 */

public abstract class QueryOptionsMixin {

    @JsonIgnore
    private ASTJexlScript script;

    @JsonIgnore
    private DocumentSerializer documentSerializer;

    @JsonIgnore
    protected Set<String> hitsOnlySet = new HashSet<>();

    @JsonIgnore
    protected FieldIndexAggregator fiAggregator;

    @JsonIgnore
    protected Equality equality;

    @JsonIgnore
    protected EventDataQueryFilter evaluationFilter;

    @JsonIgnore
    protected EventDataQueryFilter eventEvaluationFilter;

    @JsonIgnore
    protected EventDataQueryFilter eventFilter;

    @JsonIgnore
    protected List<DocumentPermutation> documentPermutations = null;

    @JsonIgnore
    protected TimeFilter timeFilter = null;

    @JsonIgnore
    protected FileSystemCache fsCache = null;

    @JsonIgnore
    protected FileSortedSet.PersistOptions ivaratorPersistOptions = new FileSortedSet.PersistOptions();

    @JsonIgnore
    protected Predicate<Key> eventEntryKeyDataTypeFilter = KeyIdentity.Function;

    @JsonIgnore
    protected QueryStatsDClient statsdClient = null;

    @JsonIgnore
    private long cardinality = Long.MAX_VALUE;

    // --- Methods ---

    @JsonIgnore
    public DocumentSerializer getDocumentSerializer() {
        return documentSerializer;
    }

    @JsonIgnore
    public void setDocumentSerializer(DocumentSerializer documentSerializer) {
        this.documentSerializer = documentSerializer;
    }

    @JsonIgnore
    public Predicate<Key> getEventEntryKeyDataTypeFilter() {
        return this.eventEntryKeyDataTypeFilter;
    }

    @JsonIgnore
    public void setEvaluationFilter(EventDataQueryFilter evaluationFilter) {
        this.evaluationFilter = evaluationFilter;
    }

    @JsonIgnore
    public void setFiEvaluationFilter(EventDataQueryFilter fiEvaluationFilter) {}

    @JsonIgnore
    public void setEventEvaluationFilter(EventDataQueryFilter eventEvaluationFilter) {}

    @JsonIgnore
    public TimeFilter getTimeFilter() {
        return timeFilter;
    }

    @JsonIgnore
    public void setTimeFilter(TimeFilter timeFilter) {
        this.timeFilter = timeFilter;
    }

    @JsonIgnore
    public Set<String> getAllIndexOnlyFields() {
        return null;
    }

    @JsonIgnore
    public Set<String> getNonEventFields() {
        return null;
    }

    @JsonIgnore
    public Set<String> getAllFields() {
        return null;
    }

    @JsonIgnore
    public FileSystemCache getFileSystemCache() throws MalformedURLException {
        return fsCache;
    }

    @JsonIgnore
    public QueryLock getQueryLock() throws MalformedURLException, QuorumPeerConfig.ConfigException {
        return null;
    }

    @JsonIgnore
    public List<String> getMatchingFieldList() {
        return null;
    }

    @JsonIgnore
    public Set<String> getHitsOnlySet() {
        return hitsOnlySet;
    }

    @JsonIgnore
    public void setHitsOnlySet(Set<String> hitsOnlySet) {
        this.hitsOnlySet = hitsOnlySet;
    }
}
