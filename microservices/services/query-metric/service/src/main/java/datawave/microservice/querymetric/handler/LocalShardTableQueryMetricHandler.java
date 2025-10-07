package datawave.microservice.querymetric.handler;

import static datawave.security.authorization.DatawaveUser.UserType.USER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.microservice.security.util.DnUtils;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.BaseQueryResponse;

public class LocalShardTableQueryMetricHandler<T extends BaseQueryMetric> extends ShardTableQueryMetricHandler<T> {
    private static final Logger log = LoggerFactory.getLogger(LocalShardTableQueryMetricHandler.class);

    protected final datawave.microservice.querymetric.QueryMetricFactory datawaveQueryMetricFactory;

    private final DatawavePrincipal datawavePrincipal;
    private final Map<String,CachedQuery> cachedQueryMap = new HashMap<>();

    protected ExecutorService executorService;

    public LocalShardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties, AccumuloConnectionFactory connectionFactory,
                    QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory, MarkingFunctions markingFunctions,
                    QueryMetricCombiner queryMetricCombiner, LuceneToJexlQueryParser luceneToJexlQueryParser, DnUtils dnUtils) {
        super(queryMetricHandlerProperties, connectionFactory, logicFactory, metricFactory, markingFunctions, queryMetricCombiner, luceneToJexlQueryParser,
                        dnUtils);

        this.datawaveQueryMetricFactory = metricFactory;

        Collection<String> auths = new ArrayList<>();
        if (clientAuthorizations != null) {
            auths.addAll(Arrays.asList(clientAuthorizations.split(",")));
        }
        DatawaveUser datawaveUser = new DatawaveUser(SubjectIssuerDNPair.of("admin"), USER, null, auths, null, null, System.currentTimeMillis());
        datawavePrincipal = new DatawavePrincipal(Collections.singletonList(datawaveUser));

        this.executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("metric-handler-query-thread-%d").build());
    }

    @Override
    protected BaseQueryResponse createAndNext(Query query) throws Exception {
        String queryId = query.getId().toString();

        Future<BaseQueryResponse> createAndNextFuture = null;
        final CachedQuery cachedQuery = new CachedQuery();
        try {
            createAndNextFuture = this.executorService.submit(() -> {
                RunningQuery runningQuery;
                AccumuloClient accumuloClient;

                cachedQueryMap.put(queryId, cachedQuery);

                QueryLogic<?> queryLogic = logicFactory.getObject();
                Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
                accumuloClient = connectionFactory.getClient(null, null, AccumuloConnectionFactory.Priority.NORMAL, trackingMap);

                cachedQuery.setAccumuloClient(accumuloClient);

                runningQuery = new RunningQuery(null, accumuloClient, AccumuloConnectionFactory.Priority.ADMIN, queryLogic, query,
                                query.getQueryAuthorizations(), datawavePrincipal, datawaveQueryMetricFactory);

                cachedQuery.setRunningQuery(runningQuery);

                QueryLogicTransformer<?,?> transformer = queryLogic.getTransformer(query);
                cachedQuery.setTransformer(transformer);

                BaseQueryResponse response = transformer.createResponse(runningQuery.next());
                response.setQueryId(queryId);
                return response;
            });

            return createAndNextFuture.get(
                            Math.max(0, queryMetricHandlerProperties.getMaxReadMilliseconds() - (System.currentTimeMillis() - cachedQuery.getStartTime())),
                            TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
            // unwrap the execution exception
            throw new IllegalStateException("Running query create and next call failed", e.getCause());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Running query create and next call failed", e);
        } finally {
            if (createAndNextFuture != null) {
                createAndNextFuture.cancel(true);
            }
        }
    }

    @Override
    protected BaseQueryResponse next(String queryId) throws Exception {
        Future<BaseQueryResponse> nextFuture = null;
        final CachedQuery cachedQuery = cachedQueryMap.get(queryId);
        try {
            nextFuture = this.executorService.submit(() -> cachedQuery.getTransformer().createResponse(cachedQuery.getRunningQuery().next()));

            return nextFuture.get(
                            Math.max(0, queryMetricHandlerProperties.getMaxReadMilliseconds() - (System.currentTimeMillis() - cachedQuery.getStartTime())),
                            TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
            // unwrap the execution exception
            throw new IllegalStateException("Running query next call failed", e.getCause());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Running query next call failed", e);
        } finally {
            if (nextFuture != null) {
                nextFuture.cancel(true);
            }
        }
    }

    @Override
    protected void close(String queryId) {
        try {
            CachedQuery cachedQuery = cachedQueryMap.remove(queryId);
            if (cachedQuery.getAccumuloClient() != null) {
                try {
                    this.connectionFactory.returnClient(cachedQuery.getAccumuloClient());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Running query close call failed", e);
        }
    }

    private static class CachedQuery {
        private long startTime = System.currentTimeMillis();

        private RunningQuery runningQuery;
        private QueryLogicTransformer<?,?> transformer;
        private AccumuloClient accumuloClient;

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public RunningQuery getRunningQuery() {
            return runningQuery;
        }

        public void setRunningQuery(RunningQuery runningQuery) {
            this.runningQuery = runningQuery;
        }

        public QueryLogicTransformer<?,?> getTransformer() {
            return transformer;
        }

        public void setTransformer(QueryLogicTransformer<?,?> transformer) {
            this.transformer = transformer;
        }

        public AccumuloClient getAccumuloClient() {
            return accumuloClient;
        }

        public void setAccumuloClient(AccumuloClient accumuloClient) {
            this.accumuloClient = accumuloClient;
        }
    }
}
