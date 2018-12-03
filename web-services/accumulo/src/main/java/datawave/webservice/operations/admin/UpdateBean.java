package datawave.webservice.operations.admin;

import datawave.configuration.spring.SpringBean;
import datawave.marking.MarkingFunctions;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.exception.AccumuloWebApplicationException;
import datawave.webservice.exception.UnauthorizedException;
import datawave.webservice.query.util.OptionallyEncodedString;
import datawave.webservice.request.UpdateRequest;
import datawave.webservice.request.objects.MutationEntry;
import datawave.webservice.request.objects.ReferencedValue;
import datawave.webservice.request.objects.TableUpdate;
import datawave.webservice.request.objects.ValueReference;
import datawave.webservice.response.UpdateResponse;
import datawave.webservice.response.ValidateVisibilityResponse;
import datawave.webservice.response.objects.AuthorizationFailure;
import datawave.webservice.response.objects.ConstraintViolation;
import datawave.webservice.response.objects.Visibility;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.xml.bind.JAXBException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class UpdateBean {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    
    @Inject
    @SpringBean(refreshable = true)
    private MarkingFunctions markingFunctions;
    
    @PostConstruct
    public void init() {}
    
    /**
     * <strong>Administrator credentials required.</strong> Perform one or more mutations via webservice
     * 
     * @param request
     *            update request
     * @HTTP 200 Success
     * @HTTP 401 User does not have permission to write to one of the specified tables
     * @HTTP 500 AccumuloException or AccumuloSecurityException
     * @returnWrapped datawave.webservice.response.UpdateResponse
     */
    @Path("/Update")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @Consumes("application/xml")
    @PUT
    public UpdateResponse doUpdate(UpdateRequest request) {
        
        UpdateResponse response = new UpdateResponse();
        
        Connector connection = null;
        try {
            
            List<TableUpdate> tableUpdateList = request.getTableUpdates();
            Set<String> tableNameSet = new HashSet<>();
            if (tableUpdateList != null) {
                for (TableUpdate tu : tableUpdateList) {
                    tableNameSet.add(tu.getTable());
                }
            }
            String[] tableNameArray = new String[tableNameSet.size()];
            tableNameSet.toArray(tableNameArray);
            
            if (log.isTraceEnabled()) {
                log.trace("Processing Update Request - Authorization Passed!");
            }
            
            int mutationsAccepted = 0;
            int mutationsDenied = 0;
            
            ArrayList<String> tablesNotFound = new ArrayList<>();
            HashMap<String,byte[]> globalDataRefs = new HashMap<>();
            
            MultiTableBatchWriter writer = null;
            
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            
            writer = connection.createMultiTableBatchWriter(new BatchWriterConfig().setMaxLatency(3, TimeUnit.SECONDS).setMaxMemory(50000)
                            .setMaxWriteThreads(5));
            if (log.isTraceEnabled()) {
                log.trace("Processing Update Request - Connector and MultiTableBatchWriter created!");
            }
            
            List<ReferencedValue> refValues = request.getReferencedValues();
            if (refValues != null) {
                for (ReferencedValue currRef : refValues) {
                    String name = currRef.getId();
                    byte[] payload = currRef.getValueAsBytes();
                    globalDataRefs.put(name, payload);
                    if (log.isTraceEnabled()) {
                        log.trace("Processing Update Request - Retrieved ReferencedValue " + name + " from the message");
                    }
                }
            }
            
            if (tableUpdateList != null) {
                
                for (TableUpdate next : tableUpdateList) {
                    String tableName = next.getTable();
                    List<datawave.webservice.request.objects.Mutation> mutations = next.getMutations();
                    if (log.isTraceEnabled()) {
                        log.trace("Processing Update Request - Processing mutations for " + tableName);
                    }
                    
                    try {
                        BatchWriter bw = writer.getBatchWriter(tableName);
                        
                        for (datawave.webservice.request.objects.Mutation nextMutation : mutations) {
                            String rowId = nextMutation.getRow().getValue();
                            
                            Mutation m = new Mutation(new Text(rowId));
                            
                            List<MutationEntry> mutationEntries = nextMutation.getMutationEntries();
                            if (mutationEntries != null) {
                                
                                for (MutationEntry currEntry : mutationEntries) {
                                    String colFamily = currEntry.getColFam().getValue();
                                    String colQualifier = currEntry.getColQual().getValue();
                                    String visibilityString = currEntry.getVisibility();
                                    if (log.isTraceEnabled()) {
                                        log.trace("mutation visibility string " + visibilityString);
                                    }
                                    ColumnVisibility visibility = new ColumnVisibility(visibilityString);
                                    if (log.isTraceEnabled()) {
                                        log.trace("Processing Update Request - Processing mutation:" + rowId + ":" + colFamily + ":" + colQualifier + ":"
                                                        + visibility);
                                    }
                                    
                                    Object valueInfo = currEntry.getValue();
                                    if (valueInfo instanceof ValueReference) {
                                        ValueReference ref = (ValueReference) valueInfo;
                                        String refid = ref.getId();
                                        if (refid != null) {
                                            byte[] data = globalDataRefs.get(refid);
                                            if (data != null) {
                                                m.put(new Text(colFamily), new Text(colQualifier), visibility, new Value(data));
                                                mutationsAccepted++;
                                                if (log.isTraceEnabled()) {
                                                    log.trace("Processing Update Request - Mutation Accepted (SetValueRef)");
                                                }
                                            } else {
                                                mutationsDenied++;
                                                // TODO refid used that is not defined at
                                                // the row level!!!
                                                if (log.isTraceEnabled()) {
                                                    log.trace("Processing Update Request - Mutation Denied (SetValueRef)");
                                                }
                                            }
                                        } else {
                                            // even though this is required in the schema,
                                            // it may not actually be there.
                                            mutationsDenied++;
                                            if (log.isTraceEnabled()) {
                                                log.trace("Processing Update Request - Mutation Denied (SetValueRef)");
                                            }
                                        }
                                    } else if (valueInfo instanceof OptionallyEncodedString) {
                                        OptionallyEncodedString value = (OptionallyEncodedString) valueInfo;
                                        
                                        m.put(new Text(colFamily), new Text(colQualifier), visibility, new Value(value.getValueAsBytes()));
                                        mutationsAccepted++;
                                        if (log.isTraceEnabled()) {
                                            log.trace("Processing Update Request - Mutation Accepted (SetValue)");
                                        }
                                    } else if (valueInfo instanceof Boolean) {
                                        Boolean remove = (Boolean) valueInfo;
                                        
                                        if (remove.equals(Boolean.TRUE)) {
                                            m.putDelete(new Text(colFamily), new Text(colQualifier), visibility);
                                            mutationsAccepted++;
                                            if (log.isTraceEnabled()) {
                                                log.trace("Processing Update Request - Mutation Accepted (Remove)");
                                            }
                                        }
                                    } else {
                                        mutationsDenied++;
                                        // TODO just in case...
                                        if (log.isTraceEnabled()) {
                                            log.trace("Processing Update Request - Mutation Denied (NoValidActionType)");
                                        }
                                    }
                                }
                            }
                            
                            bw.addMutation(m);
                        }
                    } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
                        log.info(e.getMessage());
                        tablesNotFound.add(tableName);
                    }
                }
            }
            
            Map<KeyExtent,Set<SecurityErrorCode>> authFailures = null;
            List<ConstraintViolationSummary> cvs = null;
            
            try {
                writer.close();
            } catch (MutationsRejectedException e) {
                authFailures = e.getAuthorizationFailuresMap();
                cvs = e.getConstraintViolationSummaries();
            }
            
            response.setMutationsAccepted(mutationsAccepted);
            response.setMutationsDenied(mutationsDenied);
            
            if (authFailures != null) {
                List<AuthorizationFailure> authorizationFailures = new ArrayList<>();
                for (Entry<KeyExtent,Set<SecurityErrorCode>> next : authFailures.entrySet()) {
                    AuthorizationFailure failure = new AuthorizationFailure();
                    
                    String mappedTableName = null;
                    try {
                        mappedTableName = Tables.getTableName(connection.getInstance(), next.getKey().getTableId().toString());
                    } catch (TableNotFoundException e) {
                        mappedTableName = "unknown";
                    }
                    failure.setTableName(new OptionallyEncodedString(mappedTableName));
                    failure.setEndRow(new OptionallyEncodedString(next.getKey().getEndRow().toString()));
                    failure.setPrevEndRow(new OptionallyEncodedString(next.getKey().getPrevEndRow().toString()));
                    // TODO: Add SecurityErrorCode to the AuthorizationFailure object
                    authorizationFailures.add(failure);
                }
                response.setAuthorizationFailures(authorizationFailures);
            }
            if (cvs != null) {
                List<ConstraintViolation> constraintViolations = new ArrayList<>();
                for (ConstraintViolationSummary next : cvs) {
                    ConstraintViolation cvsEntry = new ConstraintViolation();
                    cvsEntry.setConstraintClass(next.constrainClass);
                    cvsEntry.setViolationCode(Integer.valueOf(next.violationCode));
                    cvsEntry.setNumberViolations(Long.toString(next.numberOfViolatingMutations));
                    cvsEntry.setDescription(next.violationDescription);
                    constraintViolations.add(cvsEntry);
                }
                response.setConstraintViolations(constraintViolations);
            }
            
            if (!tablesNotFound.isEmpty()) {
                response.setTableNotFoundList(tablesNotFound);
            }
            
            return response;
        } catch (JAXBException e) {
            log.error(e.getMessage(), e);
            response.addException(e);
            throw new AccumuloWebApplicationException(e, response);
        } catch (AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
            response.addException(e);
            throw new UnauthorizedException(e, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(e);
            throw new AccumuloWebApplicationException(e, response);
        } finally {
            try {
                connectionFactory.returnConnection(connection);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Validate that the accumulo user can see this visibility and return the printable strings that
     * correspond with this visibility
     *
     * @param visibilityArray
     * @HTTP 200 Success
     * @HTTP 500 AccumuloException or AccumuloSecurityException
     * @returnWrapped datawave.webservice.response.ValidateVisibilityResponse
     */
    @Path("ValidateVisibilities")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @POST
    public ValidateVisibilityResponse validateVisibilities(@FormParam("visibility") String[] visibilityArray) {
        
        ValidateVisibilityResponse response = new ValidateVisibilityResponse();
        Connector connection = null;
        
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
            SecurityOperations securityOps = connection.securityOperations();
            Authorizations authorizations = securityOps.getUserAuthorizations(connection.whoami());
            
            List<Visibility> visibilityList = new ArrayList<>();
            
            for (String v : visibilityArray) {
                try {
                    Visibility vis = new Visibility();
                    vis.setValid(false);
                    visibilityList.add(vis);
                    
                    try {
                        Map<String,String> markings = markingFunctions.translateFromColumnVisibilityForAuths(new ColumnVisibility(v), authorizations);
                        vis.setVisibility(v);
                        vis.setValid(true);
                        vis.setMarkings(markings);
                    } catch (Exception e) {
                        response.addMessage("Could not interpret " + v);
                    }
                } catch (RuntimeException e) {
                    log.info(e.getMessage());
                    response.addMessage("Could not interpret " + v);
                }
            }
            response.setVisibilityList(visibilityList);
            return response;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(e);
            throw new AccumuloWebApplicationException(e, response);
        } finally {
            try {
                connectionFactory.returnConnection(connection);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
