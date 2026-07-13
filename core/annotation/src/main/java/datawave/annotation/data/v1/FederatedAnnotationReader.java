package datawave.annotation.data.v1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;

/**
 * Federates read operations across multiple Annotation Data Access objects and returns the combined set of annotations or annotation sources retrieved
 */
public class FederatedAnnotationReader {

    protected static final Logger log = LoggerFactory.getLogger(FederatedAnnotationReader.class);
    private static final long DEFAULT_DAO_TIMEOUT_MILLIS = 5000L;

    private final Map<String,AnnotationDataAccess> annotationDataAccesses;
    private final ExecutorService executorService;
    private final long daoTimeoutMillis;

    public FederatedAnnotationReader(Map<String,AnnotationDataAccess> annotationDataAccesses) {
        this(annotationDataAccesses, Executors.newCachedThreadPool(), DEFAULT_DAO_TIMEOUT_MILLIS);
    }

    public FederatedAnnotationReader(Map<String,AnnotationDataAccess> annotationDataAccesses, ExecutorService executorService, long daoTimeoutMillis) {
        this.annotationDataAccesses = annotationDataAccesses;
        this.executorService = executorService;
        this.daoTimeoutMillis = daoTimeoutMillis;
    }

    /**
     * Given a list of results and the desire to populate a singular Optional result, produce the best Optional we can under the circumstances.
     *
     * @param results
     *            the results to assess
     * @return an optional of the same type as the collection of results.
     * @param <T>
     *            the type of object in the results to return as the optional.
     */
    public static <T> Optional<T> getBest(List<T> results) {
        if (results.isEmpty()) {
            return Optional.empty();
        } else if (results.size() == 1) {
            return Optional.of(results.get(0));
        } else {
            // Multiple DAOs can return equivalent copies for the same lookup key.
            // For annotationUid retrievals, the annotation identifier is derived from the content,
            // so all instances that have the same id should be equal and returning one copy is sufficient.
            // If this is not the case, we have a more significant runtime problem.
            T first = results.get(0);
            if (!results.stream().allMatch(result -> Objects.equals(first, result))) {
                throw new RuntimeException("Conflicting federated results returned from multiple data sources");
            }
            return Optional.of(first);
        }
    }

    public Optional<AnnotationSource> getAnnotationSource(String analyticHash) {
        List<Optional<AnnotationSource>> daoResults = callDaosWithTimeout(dao -> dao.getAnnotationSource(analyticHash), "analyticHash %s", analyticHash);
        final List<AnnotationSource> results = new ArrayList<>();
        daoResults.forEach(optional -> optional.ifPresent(results::add));
        return getBest(results);
    }

    public Optional<Annotation> getAnnotation(String shard, String datatype, String uid, String annotationType, String annotationUid) {
        List<Optional<Annotation>> daoResults = callDaosWithTimeout(dao -> dao.getAnnotation(shard, datatype, uid, annotationType, annotationUid),
                        "annotation uid %s type %s for %s/%s/%s", annotationUid, annotationType, shard, datatype, uid);
        final List<Annotation> results = new ArrayList<>();
        daoResults.forEach(optional -> optional.ifPresent(results::add));
        return getBest(results);
    }

    public Optional<Annotation> getAnnotation(String shard, String datatype, String uid, String annotationId) {
        List<Optional<Annotation>> daoResults = callDaosWithTimeout(dao -> dao.getAnnotation(shard, datatype, uid, annotationId),
                        "annotation id %s for %s/%s/%s", annotationId, shard, datatype, uid);
        final List<Annotation> results = new ArrayList<>();
        daoResults.forEach(optional -> optional.ifPresent(results::add));
        return getBest(results);
    }

    public Collection<String> getAnnotationTypes(String shard, String datatype, String uid) {
        final Set<String> result = new TreeSet<>();
        List<Collection<String>> daoResults = callDaosWithTimeout(dao -> dao.getAnnotationTypes(shard, datatype, uid), "annotation types for %s/%s/%s", shard,
                        datatype, uid);
        daoResults.forEach(result::addAll);
        return result;
    }

    public List<Annotation> getAnnotations(String shard, String datatype, String uid) {
        final Set<Annotation> result = new LinkedHashSet<>();
        List<List<Annotation>> daoResults = callDaosWithTimeout(dao -> dao.getAnnotations(shard, datatype, uid), "annotations for %s/%s/%s", shard, datatype,
                        uid);
        daoResults.forEach(result::addAll);
        return new ArrayList<>(result);
    }

    public List<Annotation> getAnnotationsForType(String shard, String datatype, String uid, String annotationType) {
        final Set<Annotation> result = new LinkedHashSet<>();
        List<List<Annotation>> daoResults = callDaosWithTimeout(dao -> dao.getAnnotationsForType(shard, datatype, uid, annotationType),
                        "annotations for type %s for %s/%s/%s", annotationType, shard, datatype, uid);
        daoResults.forEach(result::addAll);
        return new ArrayList<>(result);
    }

    /**
     * Invoke the supplied DAO function against every configured {@link AnnotationDataAccess} asynchronously, waiting up to the configured timeout for each call
     * to complete.
     * <p>
     * The helper is intentionally generic so all read paths can reuse the same fan-out, timeout, and exception handling behavior without duplicating the same
     * boilerplate in every public method. The method returns only successful, non-null results. Individual DAO failures and timeouts are logged and skipped so
     * one slow or broken backend does not prevent the federated read from completing.
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
     * @return a list of successful results, in DAO iteration order
     */
    private <T> List<T> callDaosWithTimeout(DaoCall<T> daoCall, String messageTemplate, String... templateArgs) {
        if (annotationDataAccesses.isEmpty()) {
            return Collections.emptyList();
        }

        // Build a stable list of DAO entries up front so the returned futures can be mapped back to their DAO names for diagnostics.
        final List<Map.Entry<String,AnnotationDataAccess>> daoEntries = new ArrayList<>(annotationDataAccesses.entrySet());

        // Submit one callable per DAO. The executor handles concurrency while invokeAll enforces the timeout for the batch.
        final List<Callable<T>> tasks = new ArrayList<>(daoEntries.size());
        for (Map.Entry<String,AnnotationDataAccess> entry : daoEntries) {
            tasks.add(() -> daoCall.call(entry.getValue()));
        }

        final List<T> results = new ArrayList<>();
        final List<Future<T>> futures;
        try {
            futures = executorService.invokeAll(tasks, daoTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while retrieving {}", formatOperationDescription(messageTemplate, templateArgs));
            return results;
        }

        // Walk the futures in DAO order so we can log which backend timed out or failed.
        for (int i = 0; i < futures.size(); i++) {
            Future<T> future = futures.get(i);
            String daoName = daoEntries.get(i).getKey();

            // A canceled future indicates the DAO did not complete before the timeout elapsed.
            if (future.isCancelled()) {
                log.debug("Timeout retrieving {} from {} after {}ms", formatOperationDescription(messageTemplate, templateArgs), daoName, daoTimeoutMillis);
                continue;
            }

            try {
                // Successfully completed calls are added to the output list if they produced a non-null result.
                T value = future.get();
                if (value != null) {
                    results.add(value);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while collecting {} from {}", formatOperationDescription(messageTemplate, templateArgs), daoName);
                break;
            } catch (ExecutionException e) {
                // Individual DAO failures should not fail the entire federated read; log and continue.
                Throwable cause = e.getCause() == null ? e : e.getCause();
                log.debug("Exception retrieving {} from {}: {}", formatOperationDescription(messageTemplate, templateArgs), daoName, cause.getMessage());
            }
        }

        return results;
    }

    /**
     * Formats an operation description for diagnostic logging. This is kept separate from the fan-out logic so the expensive string formatting only occurs when
     * a log message is actually emitted.
     */
    private String formatOperationDescription(String operationTemplate, String... operationArgs) {
        return String.format(operationTemplate, (Object[]) operationArgs);
    }

    /**
     * Simple callback used by {@link #callDaosWithTimeout(DaoCall, String, String...)} to execute a read operation against a single DAO.
     *
     * @param <T>
     *            the result type produced by the DAO operation
     */
    @FunctionalInterface
    private interface DaoCall<T> {
        T call(AnnotationDataAccess dao);
    }
}
