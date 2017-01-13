package nsa.datawave.query.rewrite.tables;

import java.util.Iterator;
import java.util.Map.Entry;

import nsa.datawave.query.rewrite.iterator.ParentQueryIterator;
import nsa.datawave.query.rewrite.planner.QueryPlanner;
import nsa.datawave.query.rewrite.tld.DedupeColumnFamilies;
import nsa.datawave.query.rewrite.transformer.DocumentTransformer;
import nsa.datawave.query.rewrite.transformer.ParentDocumentTransformer;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.logic.QueryLogicTransformer;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Iterators;

public class ParentQueryLogic extends RefactoredShardQueryLogic {
    public ParentQueryLogic() {}
    
    public ParentQueryLogic(ParentQueryLogic other) {
        super(other);
        setIter();
    }
    
    @Override
    public void setQueryPlanner(QueryPlanner planner) {
        super.setQueryPlanner(planner);
        setIter();
    }
    
    protected void setIter() {
        getQueryPlanner().setQueryIteratorClass(ParentQueryIterator.class);
    }
    
    @Override
    public ParentQueryLogic clone() {
        return new ParentQueryLogic(this);
    }
    
    @Override
    public Iterator<Entry<Key,Value>> iterator() {
        return Iterators.filter(super.iterator(), new DedupeColumnFamilies());
    }
    
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        
        DocumentTransformer transformer = new ParentDocumentTransformer(this, settings, this.getMarkingFunctions(), this.getResponseObjectFactory(),
                        this.isReducedResponse());
        transformer.setEventQueryDataDecoratorTransformer(eventQueryDataDecoratorTransformer);
        
        transformer.setQm(queryModel);
        
        return transformer;
    }
}
