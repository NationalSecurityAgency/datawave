package datawave.query.tables.chained.strategy;

import datawave.core.query.logic.QueryLogic;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Iterator;
import java.util.Set;

/**
 * The approach to take when converting query results into another query
 * 
 * FullChainStrategy: Parse all results from Q1 to create Q2 StreamedChainStrategy: Buffer results from Q1, and then query Q2
 * 
 * 
 */
public interface ChainStrategy<T1,T2> {
    Iterator<T2> runChainedQuery(AccumuloClient client, Query initialQuery, Set<Authorizations> auths, Iterator<T1> initialQueryResults,
                    QueryLogic<T2> latterQueryLogic) throws Exception;
}
