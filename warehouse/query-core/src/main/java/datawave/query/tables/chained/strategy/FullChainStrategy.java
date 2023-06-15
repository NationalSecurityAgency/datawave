package datawave.query.tables.chained.strategy;

import java.util.Iterator;
import java.util.Set;

import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.QueryLogic;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

/**
 * Defines the logic to collect all of the results from the former query logic and issue one query against the latter query logic
 *
 *
 *
 * @param <T1>
 *            Type of former {@link datawave.webservice.query.logic.QueryLogic}
 * @param <T2>
 *            Type of latter {@link datawave.webservice.query.logic.QueryLogic}
 */
public abstract class FullChainStrategy<T1,T2> implements ChainStrategy<T1,T2> {
    protected final Logger log = Logger.getLogger(FullChainStrategy.class);

    @Override
    public Iterator<T2> runChainedQuery(AccumuloClient client, Query initialQuery, Set<Authorizations> auths, Iterator<T1> initialQueryResults,
                    QueryLogic<T2> latterQueryLogic) throws Exception {
        Query latterQuery = buildLatterQuery(initialQuery, initialQueryResults, latterQueryLogic.getLogicName());

        if (null == latterQuery) {
            log.info("Could not compute a query to run.");

            // Stub out an empty iterator to return if we couldn't generate a query to run
            return new Iterator<T2>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public T2 next() {
                    return null;
                }

                @Override
                public void remove() {}
            };
        }

        GenericQueryConfiguration config = latterQueryLogic.initialize(client, latterQuery, auths);

        latterQueryLogic.setupQuery(config);

        return latterQueryLogic.iterator();
    }

    protected abstract Query buildLatterQuery(Query initialQuery, Iterator<T1> initialQueryResults, String latterLogicName);
}
