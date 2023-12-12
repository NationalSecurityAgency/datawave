package datawave.query.tables.chained.strategy;

import datawave.query.tables.chained.iterators.ChainedQueryStreamingIterator;
import datawave.webservice.query.Query;
import datawave.webservice.query.logic.QueryLogic;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Iterator;
import java.util.Set;

public abstract class StreamedChainStrategy<T1,T2> implements ChainStrategy<T1,T2> {
    //TODO: why do we need this?
    private Iterator<T2> finalResults = null;

    public Iterator<T2> runChainedQuery(AccumuloClient client, Query initialQuery, Set<Authorizations> auths, Iterator<T1> initialQueryResults, QueryLogic<T2> latterQueryLogic) {
        finalResults = getStreamingIterator(client, initialQuery, auths, initialQueryResults, latterQueryLogic);
        return finalResults;
    }

    protected abstract ChainedQueryStreamingIterator<T1, T2> getStreamingIterator(AccumuloClient client, Query initialQuery, Set<Authorizations> auths, Iterator<T1> initialQueryResults, QueryLogic<T2> latterQueryLogic);
}
