package datawave.webservice.annotation;

import datawave.annotation.data.AnnotationDataAccess;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.visibility.AnnotationVisibilityTransformer;
import datawave.annotation.data.visibility.DefaultAnnotationVisibilityTransformer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.user.UserOperationsBean;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.AccumuloConnectionRequestBean;
import datawave.webservice.query.runner.QueryExecutor;
import datawave.webservice.query.util.LookupUUIDUtil;
import org.apache.accumulo.core.client.AccumuloClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
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
    private UserOperationsBean userOperationsBean;

    @Inject
    private AccumuloConnectionRequestBean accumuloConnectionRequestBean;

    @Inject
    @SpringBean(refreshable = true)
    private LookupUUIDConfiguration lookupUUIDConfiguration;

    private LookupUUIDUtil lookupUUIDUtil;

    @PostConstruct
    public void init() {
        this.lookupUUIDUtil = new LookupUUIDUtil(this.lookupUUIDConfiguration, queryExecutor, this.ctx, responseObjectFactory, this.queryLogicFactory,
                this.userOperationsBean);
    }

    public AnnotationDataAccess<Annotation> initializeAnnotationService() throws Exception {
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

        Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
        if (trackingMap != null) {
            trackingMap.put("query.user", "user");
            trackingMap.put("query.id", id);
            trackingMap.put("query.query", "something else");
        }
        accumuloConnectionRequestBean.requestBegin(transactionUUID.toString(), userDn, trackingMap);
        try {
            client = connectionFactory.getClient(userDn, proxyServers, connectionPoolName, priority, trackingMap);
        } finally {
            accumuloConnectionRequestBean.requestEnd(transactionUUID.toString());
        }

        if (client == null) {
            throw new QueryException("Unable to get Accumulo client");
        }

        AnnotationVisibilityTransformer visibilityTransformer = new DefaultAnnotationVisibilityTransformer();
        AccumuloAnnotationSerializer annotationSerializer = new AccumuloAnnotationSerializer(visibilityTransformer);
        AnnotationDataAccess<Annotation> annotationService = new AnnotationDataAccess<>(client, "tableName", annotationSerializer);
    }

    /** Look up the internal id for the annotation and return a 3 part tuple of shard, datatype uid
     *
     * @param idType the type of id provided
     * @param id the id itself.
     * @return a String[] of length 3: 0: shard, 1: datatype, 2: uid
     * @throws QueryException if the id is malformed.
     */
    public String[] lookupInternalId(String idType, String id) throws QueryException {
        //TODO: if the idType is CONTENT or DOCUMENT, assume that the id provided is an internal id, otherwise
        if (idType.equals("DOCUMENT")) {
            String[] parts = id.split("/");
            if (parts.length != 3) {
                throw new QueryException("Query does not specify all needed parts. Each space-delimited term" +
                        "should be of the form 'DOCUMENT:shardId/datatype/eventUID'.");
            }
            return parts;
        }
        else {
            // TODO: do lookup id here.
            throw new QueryException("Unable to resolve internal id for id=: " + idType + ":" + id);
        }
    }

    @GET
    @Path("/{idType}/{id}/types")
    @Produces("application/json")
    public Response getAllAnnotationTypes(@PathParam("idType") String idType, @PathParam("id") String id) {
        try {
            // 0: shard, 1: datatype, 2: uid
            String[] internalId = lookupInternalId(idType, id);


            AnnotationDataAccess<Annotation> annotationDataAccess = initializeAnnotationService();
            Set<String> types = annotationDataAccess.getTypesFor(id);
            return Response.ok(types).build();
        }
        catch (Exception e) {
            return Response.serverError().build();
        }
    }

    @GET
    @Path("/{idType}/{id}")
    @Produces("application/json")
    public Response getAnnotationsFor(@PathParam("idType") String idType, @PathParam("id") String id) {
        // TODO

        return Response.ok().build();
    }

    @GET
    @Path("/{idType}/{id}/type/{annotationType}")
    @Produces("application/json")
    public Response getAnnotationsByType(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationType") String annotationType) {
        // TODO

        return Response.ok().build();
    }

    @GET
    @Path("/{idType}/{id}/annotation/{annotationId}")
    @Produces("application/json")
    public Response getAnnotation(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId) {
        // TODO

        return Response.ok().build();
    }

    @PUT
    @Path("/{idType}/{id}/annotation/{annotationId}")
    @Produces("application/json")
    public Response updateAnnotation(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId) {
        // TODO return the updated annotation

        return Response.ok().build();
    }

    @GET
    @Path("/{idType}/{id}/annotation/{annotationId}/segment/{segmentId}")
    @Produces("application/json")
    public Response getAnnotationSegment(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId,
                    @PathParam("segmentId") String segmentId) {
        // TODO

        return Response.ok().build();
    }

    @POST
    @Path("/{idType}/{id}/annotation/{annotationId}/segment")
    @Consumes("application/json")
    @Produces("application/json")
    public Response addSegment(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId) {
        // TODO return the new segment

        return Response.ok().build();
    }

    @PUT
    @Path("/{idType}/{id}/annotation/{annotationId}/segment/{segmentId}")
    @Consumes("application/json")
    @Produces("application/json")
    public Response updateSegment(@PathParam("idType") String idType, @PathParam("id") String id, @PathParam("annotationId") String annotationId,
                    @PathParam("segmentId") String segmentId) {
        // TODO

        return Response.ok().build();
    }

    private static Response okJson(String json) {
        return Response.ok(json, MediaType.APPLICATION_JSON_TYPE.withCharset("utf-8")).build();
    }
}
