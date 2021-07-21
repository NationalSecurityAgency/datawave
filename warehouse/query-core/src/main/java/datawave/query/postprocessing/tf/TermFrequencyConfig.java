package datawave.query.postprocessing.tf;

import datawave.query.function.Equality;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import java.util.Set;

/**
 * Configs required to setup various term frequency related objects
 */
public class TermFrequencyConfig {
    
    private ASTJexlScript script;
    private SortedKeyValueIterator<Key,Value> source;
    private IteratorEnvironment iterEnv;
    private Set<String> contentExpansionFields;
    private Set<String> tfFields;
    private TypeMetadata typeMetadata;
    private Equality equality;
    private EventDataQueryFilter evaluationFilter;
    private boolean isTld;
    
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
    
    // Return a copy of the source
    public SortedKeyValueIterator<Key,Value> getSourceDeepCopy() {
        return source.deepCopy(iterEnv);
    }
    
    public IteratorEnvironment getIterEnv() {
        return iterEnv;
    }
    
    public void setIterEnv(IteratorEnvironment iterEnv) {
        this.iterEnv = iterEnv;
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
}
