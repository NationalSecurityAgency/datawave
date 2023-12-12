package datawave.query.tables.chained.iterators;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.QueryLogic;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public abstract class ChainedQueryStreamingIterator<T1,T2> extends ChainedQueryIterator<T1,T2> {
    protected final Logger log = Logger.getLogger(ChainedQueryStreamingIterator.class);

    protected int maxResultsToBuffer =5;

    protected QueryLogic<T2> runningLatterQueryLogic = null;

    public int getMaxResultsToBuffer() {
        return maxResultsToBuffer;
    }

    public void setMaxResultsToBuffer(int maxResultsToBuffer) {
        this.maxResultsToBuffer = maxResultsToBuffer;
    }

    @Override
    public boolean hasNext() {
        log.trace("Invoking hasNext()...");

        // We may still have results from the last latterQuery
        if (null != this.latterQueryResults && this.latterQueryResults.hasNext()) {
            return true;
        }

        // No results, close the latterQuery
        if (runningLatterQueryLogic != null) {
            NDC.pop();
            try {
                runningLatterQueryLogic.close();
            } catch (Exception e) {
                log.error("Failure while closing query logic", e);
            }
            runningLatterQueryLogic = null;
        }

        // We want to try to exhaust all initialQueryResults
        // This code will break out early if we collected enough results
        // to create a new Query which will return additional results.
        while (null != initialQueryResults && initialQueryResults.hasNext()) {
            log.trace("Initial query Iterator has more results, continuing to fecth...");

            int count = 0;
            Set<String> queryTerms = new HashSet<>();

            // while we have results from the initial query
            while (null != initialQueryResults && initialQueryResults.hasNext()) {
                if (log.isTraceEnabled()) {
                    log.trace("Current count is: " + count);
                    log.trace("maxResultsToBuffer: " + maxResultsToBuffer);
                }
                if (count >= maxResultsToBuffer) {
                    log.info("maxResultsToBuffer <= count = " + count);
                    log.info("queryTerms: " + queryTerms);
                }

                log.trace("calling next() on initialQueryResults...");

                T1 q = initialQueryResults.next();;

                // No more initial query results, see if we can run on what's left
                if (null == q) {
                    log.info("The initial query has no more results, despite returning true for hasNext(). Moving query terms to latter query");
                    initialQueryResults = null;
                    break;
                }

                log.trace("Fetching query terms from the initialQueryResults.");
                Set<String> newTerms = fetchQueryTerms(q);

                if (null == newTerms) {
                    log.info("NewTerms == null, indicating that there are no more initialQueryResults");
                    initialQueryResults = null;
                    break;
                }
                else if (!newTerms.isEmpty()) {
                    log.debug("newTerms: " + newTerms);
                    queryTerms.addAll(newTerms);
                    count = queryTerms.size();
                    log.trace("Added terms. Incremented queryTerms count to: " + count);
                }
                else {
                    log.trace("Not going to increment count or add terms, will check to see if we should continue");
                }
            }

            log.trace("queryTerms empty?: " + queryTerms.isEmpty());
            if (!queryTerms.isEmpty()) {
                log.trace("Calling buildNextQuery with the queryTerms..");
                Query query = buildNextQuery(queryTerms);

                String user = query.getUserDN();
                UUID uuid = query.getId();
                UUID parentQueryUuid = initialQuery.getId();

                if (user != null && parentQueryUuid != null && uuid != null) {
                    NDC.push("[" + user + "] [" + parentQueryUuid + "] [" + uuid + "]");
                }
                else {
                    log.info("user: " + user + ", parentQueryUuid: " + parentQueryUuid + ", uuid " + uuid);
                }

                try {
                    log.info("Cloning latter query logic.");
                    runningLatterQueryLogic = (QueryLogic) latterQueryLogic.clone();
                    log.info("Initializing latter query logic.");
                    GenericQueryConfiguration config = runningLatterQueryLogic.initialize(client, query, auths);
                    log.info("Calling setupQuery for latter query logic");
                    runningLatterQueryLogic.setupQuery(config);
                }
                catch (DatawaveFatalQueryException e) {
                    log.error("Could not run latter query due to fatal query exception: " + query ,e);
                    throw e;
                }
                catch (Exception e) {
                    log.error("Could not run latter query due to fatal execption: " + query, e);
                    throw new DatawaveFatalQueryException(e);
                }

                // Capture the results from the latter query to return before processing the next element from
                // the initial query
                latterQueryResults = runningLatterQueryLogic.iterator();

                if (latterQueryResults.hasNext()) {
                    return true;
                }
            }
        }

        log.info("Exiting hasNext() to report that there are no more results..");
        return false;
    }

    public T2 next() {
        log.trace("Invoking next()..");
        if (!hasNext()) {
            log.info("next() did not discover more results, returning null.");
            this.latterQueryResults = null;
            return null;
        }

        log.trace("next() discovered more results, stepping..");
        return this.latterQueryResults.next();
    }

    protected abstract Query buildNextQuery(Set<String> queryTerms);

    public abstract Set<String> fetchQueryTerms(T1 initialResult);
}
