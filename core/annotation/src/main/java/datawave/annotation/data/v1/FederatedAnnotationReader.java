package datawave.annotation.data.v1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;

/**
 * Federates read operations across multiple Annotation Data Access objects and returns the combined set of annotations or annotation sources retrieved
 */
public class FederatedAnnotationReader implements AnnotationReader {

    protected static final Logger log = LoggerFactory.getLogger(FederatedAnnotationReader.class);
    private static final long DEFAULT_DAO_TIMEOUT_MILLIS = 5000L;

    // A stable, immutable list of annotation readers.
    private final List<Map.Entry<String,AnnotationReader>> annotationReaders;

    private final ExecutorService executorService;
    private final long daoTimeoutMillis;

    public FederatedAnnotationReader(Map<String,AnnotationReader> annotationReaders) {
        this(annotationReaders, Executors.newCachedThreadPool(), DEFAULT_DAO_TIMEOUT_MILLIS);
    }

    public FederatedAnnotationReader(Map<String,AnnotationReader> annotationReaders, ExecutorService executorService) {
        this(annotationReaders, executorService, DEFAULT_DAO_TIMEOUT_MILLIS);
    }

    public FederatedAnnotationReader(Map<String,AnnotationReader> annotationReaders, ExecutorService executorService, long daoTimeoutMillis) {
        this.annotationReaders = List.copyOf(annotationReaders.entrySet());
        this.executorService = executorService;
        this.daoTimeoutMillis = daoTimeoutMillis;
    }

    @Override
    public Optional<AnnotationSource> getAnnotationSource(String analyticHash) {
        final DaoCall<Optional<AnnotationSource>> call = dao -> dao.getAnnotationSource(analyticHash);
        return callDaosThenGetBest(call, "analyticHash %s", analyticHash);
    }

    @Override
    public Optional<Annotation> getAnnotation(String shard, String datatype, String uid, String annotationType, String annotationId) {
        final DaoCall<Optional<Annotation>> call = dao -> dao.getAnnotation(shard, datatype, uid, annotationType, annotationId);
        return callDaosThenGetBest(call, "annotation id %s type %s for %s/%s/%s", annotationId, annotationType, shard, datatype, uid);
    }

    @Override
    public Optional<Annotation> getAnnotation(String shard, String datatype, String uid, String annotationId) {
        final DaoCall<Optional<Annotation>> call = dao -> dao.getAnnotation(shard, datatype, uid, annotationId);
        return callDaosThenGetBest(call, "annotation id %s for %s/%s/%s", annotationId, shard, datatype, uid);
    }

    @Override
    public Collection<String> getAnnotationTypes(String shard, String datatype, String uid) {
        final DaoCall<Collection<String>> call = dao -> dao.getAnnotationTypes(shard, datatype, uid);
        return callDaosThenCollect(call, TreeSet::new, "annotation types for %s/%s/%s", shard, datatype, uid);
    }

    @Override
    public Collection<Annotation> getAnnotations(String shard, String datatype, String uid) {
        final DaoCall<Collection<Annotation>> call = dao -> dao.getAnnotations(shard, datatype, uid);
        return callDaosThenCollect(call, "annotations for %s/%s/%s", shard, datatype, uid);
    }

    @Override
    public Collection<Annotation> getAnnotationsForType(String shard, String datatype, String uid, String annotationType) {
        final DaoCall<Collection<Annotation>> call = dao -> dao.getAnnotationsForType(shard, datatype, uid, annotationType);
        return callDaosThenCollect(call, "annotations for type %s for %s/%s/%s", annotationType, shard, datatype, uid);
    }

    /**
     * Executes an optional-returning DAO call across all configured readers and reduces the responses to a single best value.
     * <p>
     * Empty values are discarded. If more than one non-empty value is returned, values must agree per {@link Objects#equals(Object, Object)}; conflicting
     * values result in a runtime exception from {@link #getBestDaoResponse(Stream)}.
     * </p>
     *
     * @param daoCall
     *            callback that invokes one optional-producing read against a DAO
     * @param messageTemplate
     *            logging template used when DAO calls timeout, fail, or are interrupted
     * @param templateArgs
     *            arguments used to populate the logging template when needed
     * @param <T>
     *            value type contained in each DAO's optional response
     * @return the resolved federated value, or {@link Optional#empty()} when no DAO produced one
     */
    protected <T> Optional<T> callDaosThenGetBest(DaoCall<Optional<T>> daoCall, String messageTemplate, String... templateArgs) {
        return getBestDaoResponse(callDaos(daoCall, messageTemplate, templateArgs).stream().flatMap(Optional::stream));
    }

    /**
     * Executes a collection-returning DAO call across all configured readers and merges the successful responses.
     * <p>
     * The merged result preserves stream encounter order and removes duplicates using {@link java.util.stream.Stream#distinct()}.
     * </p>
     *
     * @param daoCall
     *            callback that invokes one collection-producing read against a DAO
     * @param messageTemplate
     *            logging template used when DAO calls timeout, fail, or are interrupted
     * @param templateArgs
     *            arguments used to populate the logging template when needed
     * @param <T>
     *            element type returned by each DAO
     * @return a mutable list containing unique values from all successful DAO responses
     */
    protected <T> Collection<T> callDaosThenCollect(DaoCall<Collection<T>> daoCall, String messageTemplate, String... templateArgs) {
        return callDaosThenCollect(daoCall, ArrayList::new, messageTemplate, templateArgs);
    }

    /**
     * Executes a collection-returning DAO call across all configured readers and merges the successful responses.
     * <p>
     * The merged result preserves stream encounter order and removes duplicates using {@link java.util.stream.Stream#distinct()}.
     * </p>
     *
     * @param daoCall
     *            callback that invokes one collection-producing read against a DAO
     * @param supplier
     *            a collection supplier.
     * @param messageTemplate
     *            logging template used when DAO calls timeout, fail, or are interrupted
     * @param templateArgs
     *            arguments used to populate the logging template when needed
     * @param <T>
     *            element type returned by each DAO
     * @return a mutable list containing unique values from all successful DAO responses
     */
    protected <T> Collection<T> callDaosThenCollect(DaoCall<Collection<T>> daoCall, Supplier<Collection<T>> supplier, String messageTemplate,
                    String... templateArgs) {
        return callDaos(daoCall, messageTemplate, templateArgs).stream().flatMap(Collection::stream).distinct().collect(Collectors.toCollection(supplier));
    }

    /**
     * Invoke the supplied DAO function against every configured {@link AnnotationReader} asynchronously, waiting up to the configured timeout for each call to
     * complete.
     * <p>
     * The helper is intentionally generic so all read paths can reuse the same fan-out, timeout, and exception handling behavior without duplicating the same
     * boilerplate in every public method. The method returns only successful, non-null results. Individual DAO failures and timeouts are logged and skipped so
     * one slow or broken backend does not prevent the federated read from completing. Results are processed immediately as they complete, rather than waiting
     * for all tasks to finish.
     * </p>
     *
     * @param daoCall
     *            the DAO operation to execute for each backend
     * @param messageTemplate
     *            a {@link String#format(String, Object...)} template describing the operation for logging
     * @param templateArgs
     *            arguments used to populate the logging template only when a message must be emitted
     * @param <T>
     *            the result type produced by the DAO operation
     * @return a list of successful results, in completion order
     */
    protected <T> Collection<T> callDaos(DaoCall<T> daoCall, String messageTemplate, String... templateArgs) {
        if (annotationReaders.isEmpty()) {
            return Collections.emptyList();
        }

        // Use ExecutorCompletionService to process results as they complete, rather than waiting for all tasks.
        final ExecutorCompletionService<T> completionService = new ExecutorCompletionService<>(executorService);

        // Submit one callable per DAO. The map starts as the full pending set; entries are removed as futures complete.
        final Map<Future<T>,String> pendingDaos = new IdentityHashMap<>(annotationReaders.size());
        for (Map.Entry<String,AnnotationReader> entry : annotationReaders) {
            pendingDaos.put(completionService.submit(() -> daoCall.call(entry.getValue())), entry.getKey());
        }

        final List<T> results = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + daoTimeoutMillis;

        try {
            while (!pendingDaos.isEmpty()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    break;
                }

                Future<T> future = completionService.poll(remaining, TimeUnit.MILLISECONDS);
                if (future == null) {
                    break;
                }

                // Remove from pending so that what's left at loop exit are the timed-out DAOs.
                String daoName = pendingDaos.remove(future);

                try {
                    T value = future.get();
                    if (value != null) {
                        results.add(value);
                    }
                } catch (ExecutionException e) {
                    // Individual DAO failures should not fail the entire federated read; log and continue.
                    Throwable cause = e.getCause() == null ? e : e.getCause();
                    if (log.isDebugEnabled()) {
                        log.debug("Exception retrieving {} from {}: {}", formatOperationDescription(messageTemplate, templateArgs), daoName,
                                        cause.getMessage());
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while retrieving {}", formatOperationDescription(messageTemplate, templateArgs));
        }

        // Anything still pending did not complete before the timeout elapsed.
        if (!pendingDaos.isEmpty()) {
            long elapsed = System.currentTimeMillis() - (deadline - daoTimeoutMillis);
            if (log.isDebugEnabled()) {
                for (String daoName : pendingDaos.values()) {
                    log.debug("Timeout retrieving {} from {} after {}ms", formatOperationDescription(messageTemplate, templateArgs), daoName, elapsed);
                }
            }
        }

        return results;
    }

    /**
     * Given a stream of results and the desire to populate a singular Optional result, produce the best Optional we can under the circumstances.
     *
     * @param results
     *            the results to assess
     * @return an optional of the same type as the collection of results.
     * @param <T>
     *            the type of object in the results to return as the optional.
     * @exception RuntimeException
     *                if there is more than one result that can't be reconciled.
     */
    protected static <T> Optional<T> getBestDaoResponse(Stream<T> results) {
        // Multiple DAOs can return equivalent copies for the same lookup key.
        // For annotationId retrievals, the annotation identifier is derived from the content,
        // so all instances that have the same id should be equal and returning one copy is sufficient.
        // If this is not the case, we have a more significant runtime problem and when reduce is called
        // an exception will be thrown.
        //@formatter:off
        return results
                .distinct()
                .limit(2)
                .reduce((u,v) -> {
                    throw new RuntimeException("Conflicting federated results returned from multiple data sources");
                });
        //@formatter:on
    }

    /**
     * Formats an operation description for diagnostic logging, consolidating the {@link String#format} call so it does not need to be repeated at each log
     * site.
     */
    private String formatOperationDescription(String operationTemplate, String... operationArgs) {
        return String.format(operationTemplate, (Object[]) operationArgs);
    }

    /**
     * Simple callback used by {@link #callDaos(DaoCall, String, String...)} to execute a read operation against a single DAO.
     *
     * @param <T>
     *            the result type produced by the DAO operation
     */
    @FunctionalInterface
    protected interface DaoCall<T> {
        T call(AnnotationReader dao);
    }
}
