package datawave.query.postprocessing.tf;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;

import datawave.query.function.Equality;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.TypeMetadata;

/**
 * Configs required to setup various term frequency related objects
 */
public class TermFrequencyConfig {

    private ASTJexlScript script;
    private SortedKeyValueIterator<Key,Value> source;
    private Set<String> contentExpansionFields;
    private Set<String> tfFields;
    private TypeMetadata typeMetadata;
    private Equality equality;
    private EventDataQueryFilter evaluationFilter;
    private boolean isTld;

    private int tfAggregationThreshold;

    public ASTJexlScript getScript() {
        return script;
    }

    public void setScript(ASTJexlScript script) {
        this.script = script;
    }

    public void setSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }

    // Return the source
    public SortedKeyValueIterator<Key,Value> getSource() {
        return this.source;
    }

    public Set<String> getContentExpansionFields() {
        return contentExpansionFields;
    }

    public void setContentExpansionFields(Set<String> contentExpansionFields) {
        this.contentExpansionFields = contentExpansionFields;
    }

    public Set<String> getTfFields() {
        return tfFields;
    }

    public void setTfFields(Set<String> tfFields) {
        this.tfFields = tfFields;
    }

    public TypeMetadata getTypeMetadata() {
        return typeMetadata;
    }

    public void setTypeMetadata(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
    }

    public Equality getEquality() {
        return equality;
    }

    public void setEquality(Equality equality) {
        this.equality = equality;
    }

    public EventDataQueryFilter getEvaluationFilter() {
        return evaluationFilter;
    }

    public void setEvaluationFilter(EventDataQueryFilter evaluationFilter) {
        this.evaluationFilter = evaluationFilter;
    }

    public boolean isTld() {
        return isTld;
    }

    public void setTld(boolean tld) {
        isTld = tld;
    }

    public int getTfAggregationThreshold() {
        return tfAggregationThreshold;
    }

    public void setTfAggregationThreshold(int tfAggregationThreshold) {
        this.tfAggregationThreshold = tfAggregationThreshold;
    }
}
