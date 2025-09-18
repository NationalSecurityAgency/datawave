package datawave.webservice.annotation;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.annotation.data.AnnotationDataAccess;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.visibility.AnnotationVisibilityTransformer;
import datawave.annotation.data.visibility.DefaultAnnotationVisibilityTransformer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.SegmentUtils;
import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.UserOperations;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.AccumuloConnectionRequestBean;
import datawave.webservice.query.runner.QueryExecutor;
import datawave.webservice.query.util.LookupUUIDUtil;

@Path("/Annotations/v1")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
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
    private QueryExecutor queryExecutor;

    @Inject
    private QueryLogicFactory queryLogicFactory;

    @Inject
    private ResponseObjectFactory responseObjectFactory;

    @Inject
    private UserOperations userOperations;

    @Inject
    private AccumuloConnectionRequestBean accumuloConnectionRequestBean;

    @Inject
    @SpringBean(refreshable = true)
    private LookupUUIDConfiguration lookupUUIDConfiguration;

    private LookupUUIDUtil lookupUUIDUtil;

    private String tableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @PostConstruct
    public void init() {
        this.lookupUUIDUtil = new LookupUUIDUtil(this.lookupUUIDConfiguration, queryExecutor, this.ctx, responseObjectFactory, this.queryLogicFactory,
                        this.userOperations);
    }

    public AnnotationDataAccess<Annotation,Segment> initializeAnnotationService() throws QueryException {
        AccumuloClient client;
        AccumuloConnectionFactory.Priority priority = AccumuloConnectionFactory.Priority.LOW;
        String connectionPoolName = "DEFAULT";
        String id = "something";
        UUID transactionUUID = java.util.UUID.fromString(id);
        final Principal p = ctx.getCallerPrincipal();
        String userDn = p.getName();
        Collection<String> proxyServers = null;

        if (DatawavePrincipal.class.isAssignableFrom(p.getClass())) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            userDn = dp.getUserDN().subjectDN();
            proxyServers = dp.getProxyServers();
        }

        // TODO get authorizations from user principal
        Authorizations authorizations = new Authorizations("PUBLIC");

        Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
        if (trackingMap != null) {
            trackingMap.put("query.user", "user");
            trackingMap.put("query.id", id);
            trackingMap.put("query.query", "something else");
        }
        accumuloConnectionRequestBean.requestBegin(transactionUUID.toString(), userDn, trackingMap);
        try {
            client = connectionFactory.getClient(userDn, proxyServers, connectionPoolName, priority, trackingMap);
        } catch (Exception e) {
            throw new QueryException("Unable to get Accumulo client, exception encountered: ", e);
        } finally {
            accumuloConnectionRequestBean.requestEnd(transactionUUID.toString());
        }

        if (client == null) {
            throw new QueryException("Unable to get Accumulo client, client was null");
        }

        AnnotationVisibilityTransformer visibilityTransformer = new DefaultAnnotationVisibilityTransformer();
        AccumuloAnnotationSerializer annotationSerializer = new AccumuloAnnotationSerializer(visibilityTransformer);
        return new AnnotationDataAccess<>(client, authorizations, tableName, annotationSerializer);
    }

    @GET
    @Path("/{idType}/{id}/types")
    @Produces("application/json")
    @Override
    public Response getAllAnnotationTypes(@PathParam("idType") String idType, @PathParam("id") String id) {
        try {
            final InternalRecordId internalId = lookupInternalId(idType, id);
            if (internalId == null) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }

            final AnnotationDataAccess<Annotation,Segment> annotationDataAccess = initializeAnnotationService();
            final Collection<String> types = annotationDataAccess.getTypes(internalId.getShard(), internalId.getDataType(), internalId.getUid());
            if (types.isEmpty()) {
                return jsonNotFound(String.format("No types found for identifier '%s:%s', internalId: '%s'", idType, id, internalId));
            }
            return jsonOk(types);
        } catch (Exception e) {
            log.error("Internal error fetching annotations", e);
            return jsonError(String.format("Internal error fetching annotation types: %s", e.getMessage()));
        }
    }

    @GET
    @Path("/{idType}/{id}")
    @Produces("application/json")
    @Override
    public Response getAnnotationsFor(@PathParam("idType") String idType, @PathParam("id") String id) {
        // TODO sanitize input to make sure it contains nothing weird like nulls.

        try {
            final InternalRecordId internalId = lookupInternalId(idType, id);
            if (internalId == null) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }

            final AnnotationDataAccess<Annotation,Segment> annotationDataAccess = initializeAnnotationService();
            final List<Annotation> annotations = annotationDataAccess.getAll(internalId.getShard(), internalId.getDataType(), internalId.getUid());
            if (annotations.isEmpty()) {
                return jsonNotFound(String.format("No annotations found for identifier '%s:%s', internalId: '%s'", idType, id, internalId));
            }
            return jsonOk(annotations);
        } catch (Exception e) {
            log.error("Internal error fetching annotations", e);
            return jsonError(String.format("Internal error fetching annotations: %s", e.getMessage()));
        }
    }

    @GET
    @Path("/{idType}/{id}/type/{annotationType}")
    @Produces("application/json")
    @Override
    public Response getAnnotationsByType(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationType") String annotationType) {
        // TODO sanitize input to make sure it contains nothing weird like nulls.

        try {
            final InternalRecordId internalId = lookupInternalId(idType, id);
            if (internalId == null) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }

            final AnnotationDataAccess<Annotation,Segment> annotationDataAccess = initializeAnnotationService();
            final List<Annotation> annotations = annotationDataAccess.getAllForType(internalId.getShard(), internalId.getDataType(), internalId.getUid(),
                            annotationType);
            if (annotations.isEmpty()) {
                return jsonNotFound(String.format("No annotations found for identifier '%s:%s', internalId: '%s', of type '%s'", idType, id, internalId,
                                annotationType));
            }
            return jsonOk(annotations);
        } catch (Exception e) {
            log.error("Internal error fetching annotations", e);
            return jsonError(String.format("Internal error fetching annotations: %s", e.getMessage()));
        }
    }

    @GET
    @Path("/{idType}/{id}/annotation/{annotationId}")
    @Produces("application/json")
    @Override
    public Response getAnnotation(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId) {
        try {
            final InternalRecordId internalId = lookupInternalId(idType, id);
            if (internalId == null) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }

            final AnnotationDataAccess<Annotation,Segment> annotationDataAccess = initializeAnnotationService();
            final Optional<Annotation> annotations = annotationDataAccess.getAnnotation(internalId.getShard(), internalId.getDataType(), internalId.getUid(),
                            annotationId);
            if (annotations.isEmpty()) {
                return jsonNotFound(String.format("No annotations found for identifier '%s:%s', internalId: '%s', with id '%s'", idType, id, internalId,
                                annotationId));
            }
            return jsonOk(annotations);
        } catch (Exception e) {
            log.error("Internal error fetching annotations", e);
            return jsonError(String.format("Internal error fetching annotations: %s", e.getMessage()));
        }
    }

    @PUT
    @Path("/{idType}/{id}/annotation/{annotationId}")
    @Produces("application/json")
    @Override
    public Response updateAnnotation(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId,
                    String body) {
        // TODO return the updated annotation

        return Response.ok().build();
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
            final InternalRecordId internalId = lookupInternalId(idType, id);
            if (internalId == null) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }

            final AnnotationDataAccess<Annotation,Segment> annotationDataAccess = initializeAnnotationService();
            final Optional<Annotation> annotation = annotationDataAccess.getAnnotation(internalId.getShard(), internalId.getDataType(), internalId.getUid(),
                            annotationId);
            if (annotation.isEmpty()) {
                return jsonNotFound(String.format("No annotation found for identifier '%s:%s', internalId: '%s', with annotation id '%s'", idType, id,
                                internalId, annotationId));
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
                return jsonNotFound(String.format("No segment found for identifier '%s:%s', internalId: '%s', with annotation id '%s' and segment id '%s'",
                                idType, id, internalId, annotationId, segmentId));
            } else if (matchingSegments.size() > 1) {
                // TODO some sorta warning?
            }
            return jsonOk(matchingSegments);

        } catch (QueryException e) {
            log.error("Internal error fetching annotations", e);
            return jsonError(String.format("Internal error fetching annotations: %s", e.getMessage()));
        }
    }

    @POST
    @Path("/{idType}/{id}/annotation/{annotationId}/segment")
    @Consumes("application/json")
    @Produces("application/json")
    @Override
    public Response addSegment(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId, String body) {
        try {
            Segment segment = SegmentUtils.fromJson(body);
            final InternalRecordId internalId = lookupInternalId(idType, id);
            if (internalId == null) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }

        } catch (InvalidProtocolBufferException e) {
            return jsonError("Invalid annotation json: " + e.getMessage());
        } catch (QueryException e) {
            log.error("Internal error fetching annotations", e);
            return jsonError(String.format("Internal error fetching annotations: %s", e.getMessage()));
        }
        return Response.ok().build();
    }

    @PUT
    @Path("/{idType}/{id}/annotation/{annotationId}/segment/{segmentId}")
    @Consumes("application/json")
    @Produces("application/json")
    @Override
    public Response updateSegment(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId,
                    @PathParam("segmentId") String segmentId, String body) {
        // TODO

        return Response.ok().build();
    }

    /**
     * Look up the internal id for the annotation and return a 3 part tuple of shard, datatype uid
     *
     * @param idType
     *            the type of id provided
     * @param id
     *            the id itself.
     * @return a String[] of length 3: 0: shard, 1: datatype, 2: uid or null if the internal id can't be found.
     * @throws QueryException
     *             if the id is malformed.
     */
    public InternalRecordId lookupInternalId(String idType, String id) throws QueryException {
        // TODO: if the idType is CONTENT or DOCUMENT, assume that the id provided is an internal id, otherwise
        if (idType.equals("DOCUMENT")) {
            return InternalRecordId.parse(id);
        } else {
            // TODO: do lookup id here.
            return null;
        }
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

    public static class InternalRecordId {
        final String shard;
        final String dataType;
        final String uid;

        public InternalRecordId(String shard, String dataType, String uid) {
            this.shard = shard;
            this.dataType = dataType;
            this.uid = uid;
        }

        public static InternalRecordId parse(String identifier) {
            // consider optimizing to use indexOf instead of split.
            final String[] parts = identifier.split("/");
            if (parts.length != 3) {
                throw new IllegalArgumentException(
                                "Identifier does not specify all needed 3 parts. Identifier must be in the form 'shardId/datatype/eventUID'.");
            }
            return new InternalRecordId(parts[0], parts[1], parts[2]);
        }

        public String getShard() {
            return shard;
        }

        public String getDataType() {
            return dataType;
        }

        public String getUid() {
            return uid;
        }

        public String toString() {
            return String.format("%s/%s/%s", shard, dataType, uid);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass())
                return false;
            InternalRecordId that = (InternalRecordId) o;
            return Objects.equal(shard, that.shard) && Objects.equal(dataType, that.dataType) && Objects.equal(uid, that.uid);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(shard, dataType, uid);
        }
    }
}
