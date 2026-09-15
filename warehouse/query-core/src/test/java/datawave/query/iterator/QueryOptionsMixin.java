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
 * QueryOptionsMixin adds the @JsonIgnore decorator to fields in the main class for testing. This is specifically useful for checking if default options are
 * present for any new query options, keeping the original QueryOptions class nice and clean!
 *
 * Fields and methods are ignored here because they are either:
 * <ul>
 * <li>Non-serializable types that Jackson cannot serialize (e.g. ASTJexlScript, Predicate, custom interfaces)</li>
 * <li>Runtime-only state that is not configured via iterator options (e.g. filters, caches, statsd clients)</li>
 * </ul>
 *
 * By using a mixin rather than annotating QueryOptions directly, we avoid coupling the production class to test-only Jackson configuration.
 */

public abstract class QueryOptionsMixin {

    // Non-serializable: ASTJexlScript is a parsed JEXL AST node, not a simple POJO
    @JsonIgnore
    private ASTJexlScript script;

    // Non-serializable: custom serializer interface, set at runtime
    @JsonIgnore
    private DocumentSerializer documentSerializer;

    // Runtime-only: populated during query execution, not configured via iterator options
    @JsonIgnore
    protected Set<String> hitsOnlySet = new HashSet<>();

    // Non-serializable: field index aggregation strategy, set at runtime
    @JsonIgnore
    protected FieldIndexAggregator fiAggregator;

    // Non-serializable: custom equality interface, set at runtime
    @JsonIgnore
    protected Equality equality;

    // Non-serializable: query filter interfaces, set at runtime during iterator initialization
    @JsonIgnore
    protected EventDataQueryFilter evaluationFilter;

    @JsonIgnore
    protected EventDataQueryFilter eventEvaluationFilter;

    @JsonIgnore
    protected EventDataQueryFilter eventFilter;

    // Non-serializable: list of DocumentPermutation interfaces, set at runtime
    @JsonIgnore
    protected List<DocumentPermutation> documentPermutations = null;

    // Non-serializable: TimeFilter is a Predicate, configured at runtime
    @JsonIgnore
    protected TimeFilter timeFilter = null;

    // Non-serializable: HDFS file system cache, initialized at runtime
    @JsonIgnore
    protected FileSystemCache fsCache = null;

    // Non-serializable: ivarator persistence configuration, initialized at runtime
    @JsonIgnore
    protected FileSortedSet.PersistOptions ivaratorPersistOptions = new FileSortedSet.PersistOptions();

    // Non-serializable: Guava Predicate for filtering event entry keys by data type
    @JsonIgnore
    protected Predicate<Key> eventEntryKeyDataTypeFilter = KeyIdentity.Function;

    // Non-serializable: StatsD client for metrics, initialized at runtime
    @JsonIgnore
    protected QueryStatsDClient statsdClient = null;

    // Runtime-only: cardinality counter, not configured via iterator options
    @JsonIgnore
    private long cardinality = Long.MAX_VALUE;

    // --- Methods ---
    // The following methods are ignored because they return or accept non-serializable types
    // that would cause Jackson serialization failures during the default options completeness test.

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
