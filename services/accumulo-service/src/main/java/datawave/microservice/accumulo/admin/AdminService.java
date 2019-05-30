package datawave.microservice.accumulo.admin;

import datawave.marking.MarkingFunctions;
import datawave.webservice.query.util.OptionallyEncodedString;
import datawave.webservice.request.UpdateRequest;
import datawave.webservice.request.objects.MutationEntry;
import datawave.webservice.request.objects.ReferencedValue;
import datawave.webservice.request.objects.TableUpdate;
import datawave.webservice.request.objects.ValueReference;
import datawave.webservice.response.ListTablesResponse;
import datawave.webservice.response.ListUserAuthorizationsResponse;
import datawave.webservice.response.ListUserPermissionsResponse;
import datawave.webservice.response.ListUsersResponse;
import datawave.webservice.response.UpdateResponse;
import datawave.webservice.response.ValidateVisibilityResponse;
import datawave.webservice.response.objects.AuthorizationFailure;
import datawave.webservice.response.objects.ConstraintViolation;
import datawave.webservice.response.objects.UserPermissions;
import datawave.webservice.response.objects.Visibility;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

/**
 * This service provides utility methods for common Accumulo operations and administrative functions
 */
@Service
@ConditionalOnProperty(name = "accumulo.admin.enabled", havingValue = "true", matchIfMissing = true)
public class AdminService {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final MarkingFunctions markingFunctions;
    private final Connector warehouseConnector;
    
    @Autowired
    public AdminService(@Qualifier("warehouse") Connector warehouseConnector, MarkingFunctions markingFunctions) {
        this.warehouseConnector = warehouseConnector;
        this.markingFunctions = markingFunctions;
    }
    
    /**
     * Grants the specified Accumulo permission to the specified user
     * 
     * @param userName
     *            Accumulo user
     * @param permission
     *            Accumulo SystemPermission
     * @return {@link VoidResponse}
     */
    public VoidResponse grantSystemPermission(String userName, String permission) {
        VoidResponse response = new VoidResponse();
        try {
            SecurityOperations ops = warehouseConnector.securityOperations();
            ops.grantSystemPermission(userName, SystemPermission.valueOf(permission));
        } catch (Exception e) {
            log.error("Failed to grant " + permission + " to user " + userName, e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Revokes the specified Accumulo permission from the specified user
     *
     * @param userName
     *            Accumulo user
     * @param permission
     *            Accumulo SystemPermission
     * @return {@link VoidResponse}
     */
    public VoidResponse revokeSystemPermission(String userName, String permission) {
        VoidResponse response = new VoidResponse();
        try {
            SecurityOperations ops = warehouseConnector.securityOperations();
            ops.revokeSystemPermission(userName, SystemPermission.valueOf(permission));
        } catch (Exception e) {
            log.error("Failed to revoke " + permission + " to user " + userName, e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Grants the specified table permission to the specified Accumulo user
     *
     * @param userName
     *            Accumulo user
     * @param tableName
     *            Accumulo table
     * @param permission
     *            Accumulo TablePermission
     * @return {@link VoidResponse}
     */
    public VoidResponse grantTablePermission(String userName, String tableName, String permission) {
        VoidResponse response = new VoidResponse();
        try {
            SecurityOperations ops = warehouseConnector.securityOperations();
            ops.grantTablePermission(userName, tableName, TablePermission.valueOf(permission));
        } catch (Exception e) {
            log.error("Failed to grant " + permission + " to user " + userName, e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Revokes the specified table permission to the specified Accumulo user
     *
     * @param userName
     *            Accumulo user
     * @param tableName
     *            Accumulo table
     * @param permission
     *            Accumulo TablePermission
     * @return {@link VoidResponse}
     */
    public VoidResponse revokeTablePermission(String userName, String tableName, String permission) {
        VoidResponse response = new VoidResponse();
        try {
            SecurityOperations ops = warehouseConnector.securityOperations();
            ops.revokeTablePermission(userName, tableName, TablePermission.valueOf(permission));
        } catch (Exception e) {
            log.error("Failed to revoke " + permission + " to user " + userName, e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Creates the specified table in Accumulo
     * 
     * @param tableName
     *            Table to be created
     * @return {@link VoidResponse}
     */
    public VoidResponse createTable(String tableName) {
        VoidResponse response = new VoidResponse();
        try {
            TableOperations ops = warehouseConnector.tableOperations();
            ops.create(tableName);
        } catch (Exception e) {
            log.error("Table creation failed for table: " + tableName, e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Flushes the memory buffer of the specified table to disk (minor compaction)
     * 
     * @param tableName
     *            Table to be flushed
     * @return {@link VoidResponse}
     */
    public VoidResponse flushTable(String tableName) {
        VoidResponse response = new VoidResponse();
        try {
            TableOperations ops = warehouseConnector.tableOperations();
            ops.flush(tableName, null, null, false);
        } catch (Exception e) {
            log.error("Table flush failed for table: " + tableName, e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Sets the specified property on the specified Accumulo table
     * 
     * @param tableName
     *            Table to be configured
     * @param propertyName
     *            Property to be set
     * @param propertyValue
     *            Value to be set
     * @return {@link VoidResponse}
     */
    public VoidResponse setTableProperty(String tableName, String propertyName, String propertyValue) {
        VoidResponse response = new VoidResponse();
        try {
            TableOperations ops = warehouseConnector.tableOperations();
            ops.setProperty(tableName, propertyName, propertyValue);
        } catch (Exception e) {
            log.error("Failed to set property: " + propertyName + ", value: " + propertyValue + ", table: " + tableName, e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Removes the specified property from the specified Accumulo table
     * 
     * @param tableName
     *            Table to be configured
     * @param propertyName
     *            Property to be removed
     * @return {@link VoidResponse}
     */
    public VoidResponse removeTableProperty(String tableName, String propertyName) {
        VoidResponse response = new VoidResponse();
        try {
            TableOperations ops = warehouseConnector.tableOperations();
            ops.removeProperty(tableName, propertyName);
        } catch (Exception e) {
            log.error("Failed to remove property: " + propertyName + ", table: " + tableName, e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Returns the list of Accumulo table names
     * 
     * @return {@link ListTablesResponse}
     */
    public ListTablesResponse listTables() {
        ListTablesResponse response = new ListTablesResponse();
        try {
            TableOperations ops = warehouseConnector.tableOperations();
            SortedSet<String> availableTables = ops.list();
            List<String> tables = new ArrayList<>();
            tables.addAll(availableTables);
            response.setTables(tables);
        } catch (Exception e) {
            log.error("Failed to retrieve table list", e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Returns the current authorizations assigned to the specified Accumulo user
     * 
     * @param userName
     *            Accumulo user name
     * @return {@link ListUserAuthorizationsResponse}
     */
    public ListUserAuthorizationsResponse listUserAuthorizations(String userName) {
        ListUserAuthorizationsResponse response = new ListUserAuthorizationsResponse();
        try {
            SecurityOperations ops = warehouseConnector.securityOperations();
            Authorizations authorizations = ops.getUserAuthorizations(userName);
            List<String> authorizationsList = new ArrayList<>();
            for (byte[] b : authorizations.getAuthorizations()) {
                authorizationsList.add(new String(b));
            }
            response.setUserAuthorizations(authorizationsList);
        } catch (Exception e) {
            log.error("Failed to retrieve authorizations for user: " + userName, e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Returns the current permissions granted to the specified Accumulo user
     * 
     * @param userName
     *            Accumulo user name
     * @return {@link ListUserPermissionsResponse}
     */
    public ListUserPermissionsResponse listUserPermissions(String userName) {
        ListUserPermissionsResponse response = new ListUserPermissionsResponse();
        try {
            SecurityOperations ops = warehouseConnector.securityOperations();
            
            List<datawave.webservice.response.objects.SystemPermission> systemPermissions = new ArrayList<>();
            SystemPermission[] allSystemPerms = SystemPermission.values();
            for (SystemPermission next : allSystemPerms) {
                if (ops.hasSystemPermission(userName, next)) {
                    systemPermissions.add(new datawave.webservice.response.objects.SystemPermission(next.name()));
                }
            }
            
            List<datawave.webservice.response.objects.TablePermission> tablePermissions = new ArrayList<>();
            TableOperations tops = warehouseConnector.tableOperations();
            SortedSet<String> tables = tops.list();
            TablePermission[] allTablePerms = TablePermission.values();
            for (String next : tables) {
                for (TablePermission nextPerm : allTablePerms) {
                    if (ops.hasTablePermission(userName, next, nextPerm)) {
                        tablePermissions.add(new datawave.webservice.response.objects.TablePermission(next, nextPerm.name()));
                    }
                }
            }
            
            List<datawave.webservice.response.objects.NamespacePermission> namespacePermissions = new ArrayList<>();
            NamespaceOperations nops = warehouseConnector.namespaceOperations();
            SortedSet<String> namespaces = nops.list();
            NamespacePermission[] allNamespacePerms = NamespacePermission.values();
            for (String next : namespaces) {
                for (NamespacePermission nextPerm : allNamespacePerms) {
                    if (ops.hasNamespacePermission(userName, next, nextPerm)) {
                        namespacePermissions.add(new datawave.webservice.response.objects.NamespacePermission(next, nextPerm.name()));
                    }
                }
            }
            
            UserPermissions userPermissions = new UserPermissions();
            userPermissions.setSystemPermissions(systemPermissions);
            userPermissions.setTablePermissions(tablePermissions);
            userPermissions.setNamespacePermissions(namespacePermissions);
            response.setUserPermissions(userPermissions);
            
        } catch (Exception e) {
            log.error("Failed to retrieve permissions for user: " + userName, e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Returns list of local Accumulo users
     * 
     * @return {@link ListUsersResponse}
     */
    public ListUsersResponse listUsers() {
        ListUsersResponse response = new ListUsersResponse();
        try {
            SecurityOperations ops = warehouseConnector.securityOperations();
            Set<String> users = ops.listLocalUsers();
            List<String> userList = new ArrayList<>();
            userList.addAll(users);
            response.setUsers(userList);
        } catch (Exception e) {
            log.error("Failed to retrieve Accumulo users", e);
            throw new RuntimeException(e);
        }
        return response;
    }
    
    /**
     * Performs the specified mutations requested by the {@link UpdateRequest} object
     * 
     * @param request
     *            {@link UpdateRequest} containing mutations to write to Accumulo
     * @return {@link UpdateResponse} instance
     */
    public UpdateResponse updateAccumulo(UpdateRequest request) {
        
        UpdateResponse response = new UpdateResponse();
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
            
            log.trace("Processing Update Request - Authorization Passed!");
            
            int mutationsAccepted = 0;
            int mutationsDenied = 0;
            
            ArrayList<String> tablesNotFound = new ArrayList<>();
            HashMap<String,byte[]> globalDataRefs = new HashMap<>();
            
            MultiTableBatchWriter writer = warehouseConnector
                            .createMultiTableBatchWriter(new BatchWriterConfig().setMaxLatency(3, TimeUnit.SECONDS).setMaxMemory(50000).setMaxWriteThreads(5));
            
            log.trace("Processing Update Request - Connector and MultiTableBatchWriter created!");
            
            List<ReferencedValue> refValues = request.getReferencedValues();
            if (refValues != null) {
                for (ReferencedValue currRef : refValues) {
                    String name = currRef.getId();
                    byte[] payload = currRef.getValueAsBytes();
                    globalDataRefs.put(name, payload);
                    log.trace("Processing Update Request - Retrieved ReferencedValue '{}' from the message", name);
                }
            }
            
            if (tableUpdateList != null) {
                
                for (TableUpdate next : tableUpdateList) {
                    String tableName = next.getTable();
                    List<datawave.webservice.request.objects.Mutation> mutations = next.getMutations();
                    
                    log.trace("Processing Update Request - Processing mutations for '{}'", tableName);
                    
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
                                    
                                    log.trace("Mutation visibility string '{}'", visibilityString);
                                    
                                    ColumnVisibility visibility = new ColumnVisibility(visibilityString);
                                    
                                    log.trace("Processing Update Request - Processing mutation: {}:{}:{}:{}", rowId, colFamily, colQualifier, visibility);
                                    
                                    Object valueInfo = currEntry.getValue();
                                    if (valueInfo instanceof ValueReference) {
                                        ValueReference ref = (ValueReference) valueInfo;
                                        String refid = ref.getId();
                                        if (refid != null) {
                                            byte[] data = globalDataRefs.get(refid);
                                            if (data != null) {
                                                m.put(new Text(colFamily), new Text(colQualifier), visibility, new Value(data));
                                                mutationsAccepted++;
                                                log.trace("Processing Update Request - Mutation Accepted (SetValueRef)");
                                            } else {
                                                mutationsDenied++;
                                                // TODO refid used that is not defined at
                                                // the row level!!!
                                                log.trace("Processing Update Request - Mutation Denied (SetValueRef)");
                                            }
                                        } else {
                                            // even though this is required in the schema,
                                            // it may not actually be there.
                                            mutationsDenied++;
                                            
                                            log.trace("Processing Update Request - Mutation Denied (SetValueRef)");
                                        }
                                    } else if (valueInfo instanceof OptionallyEncodedString) {
                                        OptionallyEncodedString value = (OptionallyEncodedString) valueInfo;
                                        
                                        m.put(new Text(colFamily), new Text(colQualifier), visibility, new Value(value.getValueAsBytes()));
                                        mutationsAccepted++;
                                        log.trace("Processing Update Request - Mutation Accepted (SetValue)");
                                    } else if (valueInfo instanceof Boolean) {
                                        Boolean remove = (Boolean) valueInfo;
                                        
                                        if (remove.equals(Boolean.TRUE)) {
                                            m.putDelete(new Text(colFamily), new Text(colQualifier), visibility);
                                            mutationsAccepted++;
                                            log.trace("Processing Update Request - Mutation Accepted (Remove)");
                                        }
                                    } else {
                                        mutationsDenied++;
                                        // TODO just in case...
                                        log.trace("Processing Update Request - Mutation Denied (NoValidActionType)");
                                    }
                                }
                            }
                            
                            bw.addMutation(m);
                        }
                    } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
                        log.info("Accumulo table operation(s) failed", e);
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
                for (Map.Entry<KeyExtent,Set<SecurityErrorCode>> next : authFailures.entrySet()) {
                    AuthorizationFailure failure = new AuthorizationFailure();
                    
                    String mappedTableName = null;
                    try {
                        mappedTableName = Tables.getTableName(warehouseConnector.getInstance(), next.getKey().getTableId().toString());
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
        } catch (Exception e) {
            log.error("Update operation encountered errors", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Validates that the accumulo user can see this visibility and return the printable strings that correspond with this visibility
     * 
     * @param visibilityArray
     *            Array of visibility strings to check
     * @return {@link ValidateVisibilityResponse}
     */
    public ValidateVisibilityResponse validateVisibilities(String[] visibilityArray) {
        
        ValidateVisibilityResponse response = new ValidateVisibilityResponse();
        try {
            SecurityOperations securityOps = warehouseConnector.securityOperations();
            Authorizations authorizations = securityOps.getUserAuthorizations(warehouseConnector.whoami());
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
                    log.info("Marking functions operation failed", e);
                    response.addMessage("Could not interpret " + v);
                }
            }
            response.setVisibilityList(visibilityList);
            return response;
        } catch (Exception e) {
            log.error("ValidateVisibilities operation failed", e);
            throw new RuntimeException(e);
        }
    }
}
