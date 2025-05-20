package datawave.query.tables;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Iterators;

import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.query.Query;
import datawave.query.iterator.ParentQueryIterator;
import datawave.query.planner.QueryPlanner;
import datawave.query.tld.DedupeColumnFamilies;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.transformer.ParentDocumentTransformer;

public class ParentQueryLogic extends ShardQueryLogic {
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
