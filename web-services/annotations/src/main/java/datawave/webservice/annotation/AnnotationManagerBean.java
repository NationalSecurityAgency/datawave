package datawave.webservice.annotation;

import static datawave.annotation.util.v1.AnnotationUtils.injectAnnotationSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import datawave.annotation.data.v1.AnnotationReader;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.rest.ResponseRewriter;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.AccumuloConnectionRequestBean;

@SuppressWarnings("unused")
@Path("/Annotations/v1")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "AnnotationWriter"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class AnnotationManagerBean implements AnnotationManager {

    private static final Logger log = LoggerFactory.getLogger(AnnotationManagerBean.class);

    @Resource
    private EJBContext ctx;

    @Inject
    private AccumuloConnectionFactory connectionFactory;

    @Inject
    private ResponseObjectFactory responseObjectFactory;

    @Inject
    private AccumuloConnectionRequestBean accumuloConnectionRequestBean;

    @Inject
    @SpringBean(name = "AnnotationManagerConfig")
    private AnnotationManagerConfig config;

    @Inject
    @SpringBean(name = "AnnotationResponseRewriter")
    private ResponseRewriter responseRewriter;

    @Resource
    private ManagedExecutorService annotationFederatedReadExecutor;

    @VisibleForTesting
    public void setEJBContext(EJBContext ctx) {
        this.ctx = ctx;
    }

    @GET
    @Path("/source/{analyticHash}")
    @Produces("application/json")
    @Override
    public Response getAnnotationSource(@PathParam("analyticHash") String analyticHash) {
        final AnnotationManagerRequestContext context = newRequestContext();
        Response response;
        try {
            context.initializeAnnotationService();
            Optional<AnnotationSource> results = context.getAnnotationSource(analyticHash);
            if (results.isEmpty()) {
                response = jsonNotFound("No annotation source found for analyticHash: " + analyticHash);
            } else {
                response = jsonOk(results.get());
            }
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation source: %s", e.getMessage());
            log.error(message, e);
            response = jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
        // Apply rewriter after client is returned to avoid holding the client during remote work
        return rewriteResponse(response, context);
    }

    @GET
    @Path("/{idType}/{id}/types")
    @Produces("application/json")
    @Override
    public Response getAnnotationTypes(@PathParam("idType") String idType, @PathParam("id") String id) {
        // TODO sanitize input to make sure it contains nothing weird like nulls.
        final AnnotationManagerRequestContext context = newRequestContext();
        Response response;
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(context, idType, id);
            if (metadata.isEmpty()) {
                response = jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            } else {
                final AnnotationReader annotationDataAccess = context.initializeAnnotationService();
                final Map<Metadata,Collection<String>> results = new HashMap<>();
                for (Metadata md : metadata) {
                    final Collection<String> types = annotationDataAccess.getAnnotationTypes(md.getRow(), md.getDataType(), md.getInternalId());
                    if (!types.isEmpty()) {
                        results.put(md, types);
                    }
                }
                if (results.isEmpty()) {
                    response = jsonNotFound("annotation types", idType, id, metadata.toString(), null, null, null);
                } else {
                    response = jsonOk(results);
                }
            }
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            response = jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
        // Apply rewriter after client is returned to avoid holding the client during remote work
        return rewriteResponse(response, context);
    }

    @GET
    @Path("/{idType}/{id}")
    @Produces("application/json")
    @Override
    public Response getAnnotationsFor(@PathParam("idType") String idType, @PathParam("id") String id) {
        // TODO sanitize input to make sure it contains nothing weird like nulls.
        final AnnotationManagerRequestContext context = newRequestContext();
        Response response;
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(context, idType, id);
            if (metadata.isEmpty()) {
                response = jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            } else {
                final AnnotationReader annotationDataAccess = context.initializeAnnotationService();

                final List<Annotation> results = new ArrayList<>();
                for (Metadata md : metadata) {
                    final Collection<Annotation> annotations = annotationDataAccess.getAnnotations(md.getRow(), md.getDataType(), md.getInternalId());
                    if (!annotations.isEmpty()) {
                        List<Annotation> annotationsWithSources = lookupAndInjectAnnotationSources(context, annotations);
                        results.addAll(annotationsWithSources);
                    }
                }
                if (results.isEmpty()) {
                    response = jsonNotFound("annotations", idType, id, metadata.toString(), null, null, null);
                } else {
                    response = jsonOk(results);
                }
            }
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            response = jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
        // Apply rewriter after client is returned to avoid holding the client during remote work
        return rewriteResponse(response, context);
    }

    @GET
    @Path("/{idType}/{id}/type/{annotationType}")
    @Produces("application/json")
    @Override
    public Response getAnnotationsByType(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationType") String annotationType) {
        // TODO sanitize input to make sure it contains nothing weird like nulls.
        final AnnotationManagerRequestContext context = newRequestContext();
        Response response;
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(context, idType, id);
            if (metadata.isEmpty()) {
                response = jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            } else {
                final AnnotationReader annotationDataAccess = context.initializeAnnotationService();

                final List<Annotation> results = new ArrayList<>();
                for (Metadata md : metadata) {
                    final Collection<Annotation> annotations = annotationDataAccess.getAnnotationsForType(md.getRow(), md.getDataType(), md.getInternalId(),
                                    annotationType);
                    if (!annotations.isEmpty()) {
                        List<Annotation> annotationsWithSources = lookupAndInjectAnnotationSources(context, annotations);
                        results.addAll(annotationsWithSources);
                    }
                }
                if (results.isEmpty()) {
                    response = jsonNotFound("annotations of type", idType, id, metadata.toString(), annotationType, null, null);
                } else {
                    response = jsonOk(results);
                }
            }
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            response = jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
        // Apply rewriter after client is returned to avoid holding the client during remote work
        return rewriteResponse(response, context);
    }

    @GET
    @Path("/{idType}/{id}/annotation/{annotationId}")
    @Produces("application/json")
    @Override
    public Response getAnnotation(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId) {
        final AnnotationManagerRequestContext context = newRequestContext();
        Response response;
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(context, idType, id);
            if (metadata.isEmpty()) {
                response = jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            } else {
                final AnnotationReader annotationDataAccess = context.initializeAnnotationService();

                final List<Annotation> results = new ArrayList<>();
                for (Metadata md : metadata) {
                    final Optional<Annotation> annotations = annotationDataAccess.getAnnotation(md.getRow(), md.getDataType(), md.getInternalId(),
                                    annotationId);
                    if (annotations.isPresent()) {
                        Annotation annotationWithSource = lookupAndInjectAnnotationSource(context, annotations.get());
                        results.add(annotationWithSource);
                    }
                }
                if (results.isEmpty()) {
                    response = jsonNotFound("annotations", idType, id, metadata.toString(), null, annotationId, null);
                } else {
                    response = jsonOk(results);
                }
            }
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            response = jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
        // Apply rewriter after client is returned to avoid holding the client during remote work
        return rewriteResponse(response, context);
    }

    @GET
    @Path("/{idType}/{id}/annotation/{annotationId}/segment/{segmentHash}")
    @Produces("application/json")
    @Override
    public Response getAnnotationSegment(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId,
                    @PathParam("segmentHash") String segmentHash) {
        final AnnotationManagerRequestContext context = newRequestContext();
        Response response;
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(context, idType, id);
            if (metadata.isEmpty()) {
                response = jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            } else {
                final AnnotationReader annotationDataAccess = context.initializeAnnotationService();

                final Map<Metadata,Annotation> annotationResults = new HashMap<>();
                for (Metadata md : metadata) {
                    final Optional<Annotation> annotation = annotationDataAccess.getAnnotation(md.getRow(), md.getDataType(), md.getInternalId(), annotationId);
                    annotation.ifPresent(value -> annotationResults.put(md, value));
                }

                if (annotationResults.isEmpty()) {
                    response = jsonNotFound("annotations", idType, id, metadata.toString(), null, annotationId, segmentHash);
                } else {
                    final Map<Metadata,Collection<Segment>> results = new HashMap<>();
                    for (Map.Entry<Metadata,Annotation> entry : annotationResults.entrySet()) {
                        // now select only the segments that were requested.
                        List<Segment> matchingSegments = new ArrayList<>();
                        for (Segment s : entry.getValue().getSegmentsList()) {
                            if (s.getSegmentHash().equals(segmentHash)) {
                                matchingSegments.add(s);
                            }
                        }
                        if (!matchingSegments.isEmpty()) {
                            results.put(entry.getKey(), matchingSegments);
                        }
                    }

                    if (results.isEmpty()) {
                        response = jsonNotFound("segments", idType, id, metadata.toString(), null, annotationId, segmentHash);
                    } else {
                        response = jsonOk(results);
                    }
                }
            }
        } catch (QueryException e) {
            final String message = String.format("Internal error fetching segment: %s", e.getMessage());
            log.error(message, e);
            response = jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
        // Apply rewriter after client is returned to avoid holding the client during remote work
        return rewriteResponse(response, context);
    }

    /**
     * Look up the internal id for the annotation and return a 3 part tuple of shard, datatype uid
     *
     * @param idType
     *            the type of id provided
     * @param id
     *            the id itself.
     * @return a list of zero to many Metadata objects with the internal shard, datatype, uid and table name of the identifier(s) provided. The list will be
     *         empty if no identifier could be found using the authorizations and query logic employed by this class.
     * @throws QueryException
     *             if the id is malformed.
     */
    private List<Metadata> lookupDocumentIdentifier(AnnotationManagerRequestContext context, String idType, String id) throws QueryException {
        // If the idType is RECORD_ID or DOCUMENT, treat the id provided as an internal id and perform a direct lookup
        // against the annotations table, if that's enabled.
        if (idType.equals("DOCUMENT") || idType.equals("RECORD_ID")) {
            if (!config.isEnableInternalIdLookup()) {
                final String message = String.format("Internal identifier lookup is disabled for '%s:%s' please use a valid document id type.", idType, id);
                throw new QueryException(message);
            }

            return parseDocumentIdentifier(id);
        }

        // Otherwise, use the lookup uuid service to perform a lookup to find the internal id in the shard table.
        final LookupUUIDService lookup = context.initializeLookupUUIDService();
        return lookup.executeLookupUUIDQuery(idType, id);
    }

    /**
     * Given a list of annotations, retrieve the annotation source information that is referenced by their analyticHash. If an analyticHash is not found, we
     * simply return the annotation without the source data injected. Currently, no errors are logged.
     *
     * @param annotations
     *            the annotations to inject sources into
     * @return return annotations with sources injected where possible.
     */
    private List<Annotation> lookupAndInjectAnnotationSources(AnnotationManagerRequestContext context, Collection<Annotation> annotations) {
        final List<Annotation> results = new ArrayList<>();
        for (Annotation a : annotations) {
            results.add(lookupAndInjectAnnotationSource(context, a));
        }
        return results;
    }

    /**
     * Given an annotation, retrieve the annotation source information that is referenced by their analyticHash. Employs a per-request hash so we don't look up
     * a single source multiple times.
     */
    private Annotation lookupAndInjectAnnotationSource(AnnotationManagerRequestContext context, Annotation a) {
        // no need to inject a source if we already have one.
        if (a.hasSource()) {
            log.warn("Strange, this annotation already has a source. Annotation {}/{}/{} {}, using analyticHash {}", a.getShard(), a.getDataType(), a.getUid(),
                            a.getAnnotationId(), a.getAnalyticSourceHash());
            return a;
        }

        if (StringUtils.isBlank(a.getAnalyticSourceHash())) {
            log.warn("Strange, this annotation does not have an analytic hash. Annotation {}/{}/{} {}", a.getShard(), a.getDataType(), a.getUid(),
                            a.getAnnotationId());
            return a;
        }

        // do the deed and cache the results.
        final String analyticHash = a.getAnalyticSourceHash();
        final Optional<AnnotationSource> result = context.getAnnotationSource(analyticHash);
        if (result.isPresent()) {
            // when returning the source in the context of an annotation, mask/remove the source's visibility
            // (which currently is the union of visibility for all things annotated with that source)
            return injectAnnotationSource(a, maskSourceMetadata(result.get()));
        } else {
            log.debug("No analytic source found for annotation {}/{}/{} {}, using analyticHash {}", a.getShard(), a.getDataType(), a.getUid(),
                            a.getAnnotationId(), a.getAnalyticSourceHash());
            return a;
        }
    }

    /**
     * When returning the source in the context of an annotation mask/remove certain metadata from the source (e.g., visibility) because the metadata on the
     * annotation itself takes precedence.
     *
     * @param annotationSource
     *            the annotation source with metadata fields to mask
     * @return a new source with masked fields removed, if no fields were found to remove, the original source.
     */
    protected final AnnotationSource maskSourceMetadata(AnnotationSource annotationSource) {
        final List<String> fieldsToMask = config.getAnnotationConfig().getMaskSourceMetadata();
        if (fieldsToMask == null || fieldsToMask.isEmpty()) {
            // no fields to mask, make no changes.
            return annotationSource;
        }

        AnnotationSource.Builder builder = null;
        for (String key : fieldsToMask) {
            if (annotationSource.containsMetadata(key)) {
                if (builder == null) {
                    builder = annotationSource.toBuilder();
                }
                builder.removeMetadata(key);
            }
        }

        return (builder == null) ? annotationSource : builder.build();
    }

    /**
     * Parse an identifier that is expected to be in the shardId/datatype/eventUID format into a Metadata object
     *
     * @param identifier
     *            the identifier to parse
     * @return a singleton list the corresponding Metadata object
     * @throws IllegalArgumentException
     *             if the identifier is not in the expected shardId/datatype/eventUID format.
     */
    private List<Metadata> parseDocumentIdentifier(String identifier) {
        final String[] parts = identifier.split("/");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Identifier does not specify all needed 3 parts. Identifier must be in the form 'shardId/datatype/eventUID'.");
        }

        final Metadata md = new Metadata(config.getLookupUUIDQueryLogic().getTableName(), parts[0], parts[1], parts[2]);
        return Collections.singletonList(md);
    }

    private static Response jsonNotFound(String objectType, String idType, String id, String internalId, String annotationType, String annotationId,
                    String segmentHash) {
        String message = id.contains(internalId) ? String.format("No %s found for identifier: '%s:%s'", objectType, idType, id)
                        : String.format("No %s found for identifier '%s:%s', internalId: '%s'", objectType, idType, id, internalId);

        if (!StringUtils.isEmpty(annotationType)) {
            message += String.format(", annotationType '%s'", annotationType);
        }
        if (!StringUtils.isEmpty(annotationId)) {
            message += String.format(", annotationId '%s'", annotationId);
        }
        if (!StringUtils.isEmpty(segmentHash)) {
            message += String.format(", segmentHash '%s'", segmentHash);
        }

        return jsonNotFound(message);
    }

    private static Response jsonNotFound(String message) {
        String response = "{\"message\":\"" + message + "\"}";
        return Response.status(Response.Status.NOT_FOUND).entity(response).build();
    }

    private static Response jsonError(String message) {
        String response = "{\"message\":\"" + message + "\"}";
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(response).build();
    }

    private static Response jsonOk(Object responseObject) {
        // TODO: do we want to return more? (e.g., include fields like internal id, etc..
        return Response.ok(responseObject, MediaType.APPLICATION_JSON_TYPE.withCharset("utf-8")).build();
    }

    /**
     * Apply the response rewriter to transform the response before returning it to the client. This should be called after the Accumulo client has been
     * returned to the pool to avoid holding the client during potentially expensive rewriter work.
     *
     * @param response
     *            the response to potentially rewrite
     * @param context
     *            the request context for the rewriter
     * @return the rewritten response, or an error if an error occurs during rewriting
     */
    @VisibleForTesting
    Response rewriteResponse(Response response, AnnotationManagerRequestContext context) {
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            // Don't rewrite error responses
            return response;
        }
        if (responseRewriter == null) {
            log.warn("No ResponseRewriter configured, returning response without rewriting");
            return response;
        }
        try {
            Response rewritten = responseRewriter.rewriteResponse(response, context);
            if (rewritten == null) {
                final String message = "ResponseRewriter returned null";
                log.error(message);
                return jsonError(message);
            }
            return rewritten;
        } catch (Exception e) {
            final String message = String.format("Error applying response rewriter: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        }
    }

    @PostConstruct
    private void logRewriterConfig() {
        if (responseRewriter != null) {
            log.info("ResponseRewriter configured: {}", responseRewriter.getClass().getName());
        } else {
            log.warn("No ResponseRewriter configured, responses will not be rewritten");
        }
    }

    @VisibleForTesting
    protected AnnotationManagerConfig getConfig() {
        return config;
    }

    private AnnotationManagerRequestContext newRequestContext() {
        return new AnnotationManagerRequestContext(config, ctx, connectionFactory, accumuloConnectionRequestBean, responseObjectFactory,
                        annotationFederatedReadExecutor);
    }

}
