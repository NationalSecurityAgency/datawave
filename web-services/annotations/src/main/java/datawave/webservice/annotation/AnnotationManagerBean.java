package datawave.webservice.annotation;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

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
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.Validator;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.annotation.util.v1.AnnotationValidators;
import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.security.authorization.DatawavePrincipal;
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

    // Per-request stateful variables, managed internally.
    private Set<Authorizations> authorizations;
    private AccumuloClient client;
    private LookupUUIDService lookupUUIDService;
    private AnnotationDataAccess annotationDataAccess;

    @VisibleForTesting
    public void setEJBContext(EJBContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Calculate the auths for this query based on the principal and the queryAuths if any.
     *
     * @param queryAuths
     *            the auths to downgrade to, or null if no downgrade is desired
     * @param principal
     *            the caller principal
     * @return a Set of authorizations to use for this query
     * @throws QueryException
     *             and exception if there was a problem calculating the auths
     */
    private static Set<Authorizations> getUserAuthorizations(@SuppressWarnings("SameParameterValue") String queryAuths, DatawavePrincipal principal)
                    throws QueryException {
        try {
            if (queryAuths == null) {
                return AuthorizationsUtil.buildAuthorizations(principal.getAuthorizations());
            } else {
                final String downgradedAuths = AuthorizationsUtil.downgradeUserAuths(queryAuths, principal, principal);
                return AuthorizationsUtil.buildAuthorizations(Collections.singleton(AuthorizationsUtil.splitAuths(downgradedAuths)));
            }

        } catch (Exception e) {
            throw new QueryException("Failed to get user query authorizations", e);
        }
    }

    public AccumuloClient initializeAccumuloClient() throws QueryException {
        if (client == null || authorizations == null) {
            final Principal p = ctx.getCallerPrincipal();
            final boolean isDatawavePrincipal = DatawavePrincipal.class.isAssignableFrom(p.getClass());
            final DatawavePrincipal dp = isDatawavePrincipal ? (DatawavePrincipal) p : null;
            final String userDn = dp != null ? dp.getUserDN().subjectDN() : p.getName();
            final Collection<String> proxyServers = dp != null ? dp.getProxyServers() : null;

            // TODO: allow downgrade?
            // String queryAuths = queryParameters.getFirst(QueryParameters.QUERY_AUTHORIZATIONS);
            authorizations = getUserAuthorizations(null, dp);

            UUID transactionUUID = java.util.UUID.randomUUID();
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            if (trackingMap != null) {
                trackingMap.put("query.user", "user");
                trackingMap.put("query.id", transactionUUID.toString());
                trackingMap.put("query.query", "something else");
            }
            accumuloConnectionRequestBean.requestBegin(transactionUUID.toString(), userDn, trackingMap);
            try {
                client = connectionFactory.getClient(userDn, proxyServers, config.getConnPoolName(), config.getPriority(), trackingMap);
            } catch (Exception e) {
                throw new QueryException("Unable to get Accumulo client, exception encountered: ", e);
            } finally {
                accumuloConnectionRequestBean.requestEnd(transactionUUID.toString());
            }
        }
        return client;
    }

    public LookupUUIDService initializeLookupUUIDService() throws QueryException {
        if (lookupUUIDService == null) {
            final AccumuloClient client = initializeAccumuloClient();
            lookupUUIDService = new LookupUUIDService(config.getLookupUUIDServiceConfig(), client, authorizations, responseObjectFactory,
                            config.getLookupUUIDQueryLogic());
        }
        return lookupUUIDService;
    }

    public AnnotationDataAccess initializeAnnotationService() throws QueryException {
        if (annotationDataAccess == null) {
            final AccumuloClient client = initializeAccumuloClient();
            final AccumuloAnnotationSerializer annotationSerializer = new AccumuloAnnotationSerializer(config.getVisibilityTransformer(),
                            config.getTimestampTransformer());
            annotationDataAccess = new AnnotationDataAccess(client, authorizations, config.getTableName(), annotationSerializer);
        }
        return annotationDataAccess;
    }

    @GET
    @Path("/{idType}/{id}/types")
    @Produces("application/json")
    @Override
    public Response getAnnotationTypes(@PathParam("idType") String idType, @PathParam("id") String id) {
        // TODO sanitize input to make sure it contains nothing weird like nulls.
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(idType, id);
            if (metadata.isEmpty()) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }
            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();
            final Map<Metadata,Collection<String>> results = new HashMap<>();
            for (Metadata md : metadata) {
                final Collection<String> types = annotationDataAccess.getAnnotationTypes(md.getRow(), md.getDataType(), md.getInternalId());
                if (!types.isEmpty()) {
                    results.put(md, types);
                }
            }
            if (results.isEmpty()) {
                return jsonNotFound("annotation types", idType, id, metadata.toString(), null, null, null);
            }
            return jsonOk(results);
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        }
    }

    @GET
    @Path("/{idType}/{id}")
    @Produces("application/json")
    @Override
    public Response getAnnotationsFor(@PathParam("idType") String idType, @PathParam("id") String id) {
        // TODO sanitize input to make sure it contains nothing weird like nulls.
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(idType, id);
            if (metadata.isEmpty()) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }
            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();

            final List<Annotation> results = new ArrayList<>();
            for (Metadata md : metadata) {
                final List<Annotation> annotations = annotationDataAccess.getAnnotations(md.getRow(), md.getDataType(), md.getInternalId());
                if (!annotations.isEmpty()) {
                    results.addAll(annotations);
                }
            }
            if (results.isEmpty()) {
                return jsonNotFound("annotations", idType, id, metadata.toString(), null, null, null);
            }
            return jsonOk(results);
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        }
    }

    @GET
    @Path("/{idType}/{id}/type/{annotationType}")
    @Produces("application/json")
    @Override
    public Response getAnnotationsByType(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationType") String annotationType) {
        // TODO sanitize input to make sure it contains nothing weird like nulls.
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(idType, id);
            if (metadata.isEmpty()) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }
            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();

            final List<Annotation> results = new ArrayList<>();
            for (Metadata md : metadata) {
                final List<Annotation> annotations = annotationDataAccess.getAnnotationsForType(md.getRow(), md.getDataType(), md.getInternalId(),
                                annotationType);
                if (!annotations.isEmpty()) {
                    results.addAll(annotations);
                }
            }
            if (results.isEmpty()) {
                return jsonNotFound("annotations of type", idType, id, metadata.toString(), annotationType, null, null);
            }
            return jsonOk(results);
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        }
    }

    @GET
    @Path("/{idType}/{id}/annotation/{annotationId}")
    @Produces("application/json")
    @Override
    public Response getAnnotation(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId) {
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(idType, id);
            if (metadata.isEmpty()) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }
            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();

            final List<Annotation> results = new ArrayList<>();
            for (Metadata md : metadata) {
                final Optional<Annotation> annotations = annotationDataAccess.getAnnotation(md.getRow(), md.getDataType(), md.getInternalId(), annotationId);
                annotations.ifPresent(results::add);
            }
            if (results.isEmpty()) {
                return jsonNotFound("annotations", idType, id, metadata.toString(), null, annotationId, null);
            }
            return jsonOk(results);
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        }
    }

    @POST
    @Path("/{idType}/{id}/annotation")
    @Produces("application/json")
    @RolesAllowed({"AnnotationWriter"})
    @Override
    public Response addAnnotation(@PathParam("idType") String idType, @PathParam("id") String id, String body) {
        try {
            final Annotation rawAnnotation = AnnotationUtils.annotationFromJson(body);
            final Validator<Annotation> validator = AnnotationValidators.getAnnotationValidator();
            final Validator.ValidationState<Annotation> validationState = validator.check(rawAnnotation);
            if (!validationState.isValid()) {
                final String message = String.format("Invalid annotation json: %s", validationState.getErrors());
                log.info(message);
                return jsonError(message);
            }

            final List<Metadata> metadataList = lookupDocumentIdentifier(idType, id);
            if (metadataList.isEmpty()) {
                final String message = String.format("No internal identifier found for '%s:%s'", idType, id);
                log.info(message);
                return jsonNotFound(message);
            } else if (metadataList.size() > 1) {
                final String message = String.format("Multiple internal identifiers found for '%s:%s' must choose an id with a single internal id: %s", idType,
                                id, metadataList);
                log.info(message);
                return jsonError(message);
            }

            final Metadata metadata = metadataList.get(0);

            //@formatter:off
            final Annotation localizedAnnotation = rawAnnotation.toBuilder()
                    .setShard(metadata.getRow())
                    .setDataType(metadata.getDataType())
                    .setUid(metadata.getInternalId())
                    .build();
            //@formatter:on

            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();
            Optional<Annotation> addResult = annotationDataAccess.addAnnotation(localizedAnnotation);
            if (addResult.isPresent()) {
                log.debug("Successfully added annotation: {}", addResult.get());
                return jsonOk(addResult.get());
            }

            // if we make it here, there was a problem
            String message = String.format(
                            "Internal error: Optional return from dao addAnnotation was empty, id: %s, idType %s, internal id %s, localized annotation: %s",
                            idType, id, metadata, localizedAnnotation);
            log.warn(message);
            return jsonError(message);
        } catch (InvalidProtocolBufferException e) {
            final String message = String.format("Invalid annotation json: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } catch (QueryException e) {
            final String message = String.format("Internal error adding annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        }
    }

    @PUT
    @Path("/{idType}/{id}/annotation/{annotationId}")
    @Produces("application/json")
    @RolesAllowed({"AnnotationWriter"})
    @Override
    public Response updateAnnotation(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId,
                    String body) {
        try {
            final Annotation rawAnnotation = AnnotationUtils.annotationFromJson(body);
            final Validator<Annotation> validator = AnnotationValidators.getAnnotationValidator();
            final Validator.ValidationState<Annotation> validationState = validator.check(rawAnnotation);
            if (!validationState.isValid()) {
                final String message = String.format("Invalid annotation json: %s", validationState.getErrors());
                log.info(message);
                return jsonError(message);
            }

            final List<Metadata> metadataList = lookupDocumentIdentifier(idType, id);
            if (metadataList.isEmpty()) {
                final String message = String.format("No internal identifier found for '%s:%s'", idType, id);
                log.info(message);
                return jsonNotFound(message);
            } else if (metadataList.size() > 1) {
                final String message = String.format("Multiple internal identifiers found for '%s:%s' must choose an id with a single internal id: %s", idType,
                                id, metadataList);
                log.info(message);
                return jsonError(message);
            }

            final Metadata metadata = metadataList.get(0);

            //@formatter:off
            final Annotation localizedAnnotation = rawAnnotation.toBuilder()
                    .setShard(metadata.getRow())
                    .setDataType(metadata.getDataType())
                    .setUid(metadata.getInternalId())
                    .build();
            //@formatter:on

            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();
            Optional<Annotation> addResult = annotationDataAccess.updateAnnotation(localizedAnnotation);
            if (addResult.isPresent()) {
                log.debug("Successfully updated annotation: {}", addResult.get());
                return jsonOk(addResult.get());
            }
            // if we make it here, there was a problem
            String message = String.format(
                            "Internal error: Optional return from dao updateAnnotation was empty, id: %s, idType %s, internal id %s, localized annotation: %s",
                            idType, id, metadata, localizedAnnotation);
            log.warn(message);
            return jsonError(message);
        } catch (InvalidProtocolBufferException e) {
            final String message = String.format("Invalid annotation json: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } catch (QueryException e) {
            final String message = String.format("Internal error updating annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        }
    }

    @GET
    @Path("/{idType}/{id}/annotation/{annotationId}/segment/{segmentId}")
    @Produces("application/json")
    @Override
    public Response getAnnotationSegment(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId,
                    @PathParam("segmentId") String segmentId) {
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(idType, id);
            if (metadata.isEmpty()) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }
            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();

            final Map<Metadata,Annotation> annotationResults = new HashMap<>();
            for (Metadata md : metadata) {
                final Optional<Annotation> annotation = annotationDataAccess.getAnnotation(md.getRow(), md.getDataType(), md.getInternalId(), annotationId);
                annotation.ifPresent(value -> annotationResults.put(md, value));
            }

            if (annotationResults.isEmpty()) {
                return jsonNotFound("annotations", idType, id, metadata.toString(), null, annotationId, segmentId);
            }

            final Map<Metadata,Collection<Segment>> results = new HashMap<>();
            for (Map.Entry<Metadata,Annotation> entry : annotationResults.entrySet()) {
                // now select only the segments that were requested.
                List<Segment> matchingSegments = new ArrayList<>();
                for (Segment s : entry.getValue().getSegmentsList()) {
                    if (s.getSegmentId().equals(segmentId)) {
                        matchingSegments.add(s);
                    }
                }
                if (!matchingSegments.isEmpty()) {
                    results.put(entry.getKey(), matchingSegments);
                }
            }

            if (results.isEmpty()) {
                return jsonNotFound("segments", idType, id, metadata.toString(), null, annotationId, segmentId);
            }
            return jsonOk(results);
        } catch (QueryException e) {
            final String message = String.format("Internal error fetching segment: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        }
    }

    @POST
    @Path("/{idType}/{id}/annotation/{annotationId}/segment")
    @Consumes("application/json")
    @Produces("application/json")
    @RolesAllowed({"AnnotationWriter"})
    @Override
    public Response addSegment(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId, String body) {
        try {
            Segment segment = AnnotationUtils.segmentFromJson(body);

            final List<Metadata> metadataList = lookupDocumentIdentifier(idType, id);
            if (metadataList.isEmpty()) {
                final String message = String.format("No internal identifier found for '%s:%s'", idType, id);
                log.info(message);
                return jsonNotFound(message);
            } else if (metadataList.size() > 1) {
                final String message = String.format("Multiple internal identifiers found for '%s:%s' must choose an id with a single internal id: %s", idType,
                                id, metadataList);
                log.info(message);
                return jsonError(message);
            }
            final Metadata metadata = metadataList.get(0);

            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();
            annotationDataAccess.addSegment(metadata.getRow(), metadata.getDataType(), metadata.getInternalId(), annotationId, segment);
            return jsonOk(segment.getSegmentId());
        } catch (InvalidProtocolBufferException e) {
            final String message = String.format("Invalid annotation json: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } catch (QueryException e) {
            final String message = String.format("Internal error adding segment: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        }
    }

    @PUT
    @Path("/{idType}/{id}/annotation/{annotationId}/segment/{segmentId}")
    @Consumes("application/json")
    @Produces("application/json")
    @RolesAllowed({"AnnotationWriter"})
    @Override
    public Response updateSegment(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId,
                    @PathParam("segmentId") String segmentId, String body) {
        // TODO: determine update semantics.
        return jsonError("Not implemented");
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
    private List<Metadata> lookupDocumentIdentifier(String idType, String id) throws QueryException {
        if (idType.equals("DOCUMENT") || idType.equals("RECORD_ID")) {
            // If the idType is RECORD_ID or DOCUMENT treat the id provided is an internal id.
            return parseDocumentIdentifier(id);
        } else {
            // Otherwise, perform a lookup to find the internal id.
            final LookupUUIDService lookup = initializeLookupUUIDService();
            return lookup.executeLookupUUIDQuery(idType, id);
        }
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
                    String segmentId) {
        String message = id.contains(internalId) ? String.format("No %s found for identifier: '%s:%s'", objectType, idType, id)
                        : String.format("No %s found for identifier '%s:%s', internalId: '%s'", objectType, idType, id, internalId);

        if (!StringUtils.isEmpty(annotationType)) {
            message += String.format(", annotationType '%s'", annotationType);
        }
        if (!StringUtils.isEmpty(annotationId)) {
            message += String.format(", annotationId '%s'", annotationId);
        }
        if (!StringUtils.isEmpty(segmentId)) {
            message += String.format(", segmentId '%s'", segmentId);
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
}
