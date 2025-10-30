package datawave.webservice.annotation;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import datawave.webservice.query.result.event.Metadata;
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
        try {
            final Metadata metadata = lookupDocumentIdentifier(idType, id);
            if (metadata == null) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }

            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();
            final Collection<String> types = annotationDataAccess.getAnnotationTypes(metadata.getRow(), metadata.getDataType(), metadata.getInternalId());
            if (types.isEmpty()) {
                return jsonNotFound("annotation types", idType, id, mdFmt(metadata), null, null);
            }
            return jsonOk(types);
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
            final Metadata metadata = lookupDocumentIdentifier(idType, id);
            if (metadata == null) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }

            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();
            final List<Annotation> annotations = annotationDataAccess.getAnnotations(metadata.getRow(), metadata.getDataType(), metadata.getInternalId());
            if (annotations.isEmpty()) {
                return jsonNotFound("annotations", idType, id, mdFmt(metadata), null, null);
            }
            return jsonOk(annotations);
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
            final Metadata metadata = lookupDocumentIdentifier(idType, id);
            if (metadata == null) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }

            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();
            final List<Annotation> annotations = annotationDataAccess.getAnnotationsForType(metadata.getRow(), metadata.getDataType(), metadata.getInternalId(),
                            annotationType);
            if (annotations.isEmpty()) {
                return jsonNotFound("annotations of type", idType, id, mdFmt(metadata), annotationType, null);
            }
            return jsonOk(annotations);
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
            final Metadata metadata = lookupDocumentIdentifier(idType, id);
            if (metadata == null) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }

            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();
            final Optional<Annotation> annotations = annotationDataAccess.getAnnotation(metadata.getRow(), metadata.getDataType(), metadata.getInternalId(),
                            annotationId);
            if (annotations.isEmpty()) {
                return jsonNotFound("annotations", idType, id, mdFmt(metadata), null, annotationId);
            }
            return jsonOk(annotations);
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

            final Metadata metadata = lookupDocumentIdentifier(idType, id);
            if (metadata == null) {
                final String message = String.format("No internal identifier found for '%s:%s'", idType, id);
                log.info(message);
                return jsonNotFound(message);
            }

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

            final Metadata metadata = lookupDocumentIdentifier(idType, id);
            if (metadata == null) {
                final String message = String.format("No internal identifier found for '%s:%s'", idType, id);
                log.info(message);
                return jsonNotFound(message);
            }

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

            // TODO: validate that we still need to retrieve individual segments. This is sorta brute force for now, we
            // retrieve the entire annotation and return the segment with the matching id. Optimize later if this is a
            // heavily used case.
            final Metadata metadata = lookupDocumentIdentifier(idType, id);
            if (metadata == null) {
                final String message = String.format("No internal identifier found for '%s:%s'", idType, id);
                log.info(message);
                return jsonNotFound(message);
            }

            final AnnotationDataAccess annotationDataAccess = initializeAnnotationService();
            final Optional<Annotation> annotation = annotationDataAccess.getAnnotation(metadata.getRow(), metadata.getDataType(), metadata.getInternalId(),
                            annotationId);
            if (annotation.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("No annotation found for idType: {}, id: {}, internalId: {}", idType, id, mdFmt(metadata));
                }
                return jsonNotFound("annotations", idType, id, mdFmt(metadata), null, annotationId);
            }

            // now filter out the segment that was requested
            Annotation a = annotation.get();
            List<Segment> matchingSegments = new ArrayList<>();
            for (Segment s : a.getSegmentsList()) {
                if (s.getSegmentId().equals(segmentId)) {
                    matchingSegments.add(s);
                }
            }

            if (matchingSegments.isEmpty()) {
                final String message = String.format("No segments found for identifier '%s:%s', internalId: '%s', with annotation id '%s' and segment id '%s'",
                                idType, id, mdFmt(metadata), annotationId, segmentId);
                log.debug(message);
                return jsonNotFound(message);
            } else if (matchingSegments.size() > 1) {
                throw new QueryException(
                                String.format("Multiple segments found for identifier '%s:%s', internalId: '%s', with annotation id '%s' and segment id '%s'",
                                                idType, id, mdFmt(metadata), annotationId, segmentId));
            }
            return jsonOk(Optional.of(matchingSegments.get(0)));

        } catch (QueryException e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
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
            final Metadata metadata = lookupDocumentIdentifier(idType, id);
            if (metadata == null) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }
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

        return Response.ok().build();
    }

    /**
     * Look up the internal id for the annotation and return a 3 part tuple of shard, datatype uid
     *
     * @param idType
     *            the type of id provided
     * @param id
     *            the id itself.
     * @return a Metadata object containing the source table, row, datatype and internal document id (uid).
     * @throws QueryException
     *             if the id is malformed.
     */
    private Metadata lookupDocumentIdentifier(String idType, String id) throws QueryException {
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
     * @return the corresponding Metadata object
     * @throws IllegalArgumentException
     *             if the identifier is not in the expected shardId/datatype/eventUID format.
     */
    private Metadata parseDocumentIdentifier(String identifier) {
        final String[] parts = identifier.split("/");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Identifier does not specify all needed 3 parts. Identifier must be in the form 'shardId/datatype/eventUID'.");
        }

        final Metadata md = new Metadata();
        md.setTable(config.getLookupUUIDQueryLogic().getTableName());
        md.setRow(parts[0]);
        md.setDataType(parts[1]);
        md.setInternalId(parts[2]);
        return md;
    }

    private static Response jsonNotFound(String objectType, String idType, String id, String internalId, String annotationType, String annotationId) {
        String message = id.contains(internalId) ? String.format("No %s found for identifier: '%s:%s'", objectType, idType, id)
                        : String.format("No %s found for identifier '%s:%s', internalId: '%s'", objectType, idType, id, internalId);

        if (!StringUtils.isEmpty(annotationType)) {
            message += String.format(", annotationType '%s'", annotationType);
        }
        if (!StringUtils.isEmpty(annotationId)) {
            message += String.format(", annotationId '%s'", annotationId);
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

    /** Format metadata for error messages, since it doesn't have a reasonable {@code toString} method */
    private static String mdFmt(Metadata metadata) {
        return String.format("%s/%s/%s [%s]", metadata.getRow(), metadata.getDataType(), metadata.getInternalId(), metadata.getTable());
    }
}
