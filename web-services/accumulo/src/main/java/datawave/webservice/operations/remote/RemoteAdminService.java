package datawave.webservice.operations.remote;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import com.google.common.base.Preconditions;
import datawave.configuration.RefreshableScope;
import datawave.webservice.request.UpdateRequest;
import datawave.webservice.response.ListTablesResponse;
import datawave.webservice.response.ListUserAuthorizationsResponse;
import datawave.webservice.response.ListUserPermissionsResponse;
import datawave.webservice.response.ListUsersResponse;
import datawave.webservice.response.UpdateResponse;
import datawave.webservice.response.ValidateVisibilityResponse;
import datawave.webservice.result.VoidResponse;
import org.apache.commons.lang.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;

import javax.annotation.PostConstruct;
import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.interceptor.Interceptor;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RefreshableScope
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
public class RemoteAdminService extends RemoteAccumuloService {
    
    private enum Suffix {
        CREATE_TABLE("admin/createTable/%s"),
        FLUSH_TABLE("admin/flushTable/%s"),
        GRANT_SYSTEM_PERM("admin/grantSystemPermission/%s/%s"),
        GRANT_TABLE_PERM("admin/grantTablePermission/%s/%s/%s"),
        LIST_TABLES("admin/listTables"),
        LIST_USERS("admin/listUsers"),
        LIST_USER_AUTHS("admin/listUserAuthorizations/%s"),
        LIST_USER_PERM("admin/listUserPermissions/%s"),
        REMOVE_TABLE_PROP("admin/removeTableProperty/%s/%s"),
        REVOKE_SYSTEM_PERM("admin/revokeSystemPermission/%s/%s"),
        REVOKE_TABLE_PERM("admin/revokeTablePermission/%s/%s/%s"),
        SET_TABLE_PROP("admin/setTableProperty/%s/%s/%s"),
        UPDATE("admin/update"),
        VALIDATE_VIZ("admin/validateVisibilities");
        
        final String suffix;
        
        Suffix(String suffix) {
            this.suffix = suffix;
        }
        
        String get() {
            return suffix;
        }
    }
    
    private Map<Suffix,ObjectReader> readers;
    private ObjectMapper mapper;
    
    @Override
    @PostConstruct
    public void init() {
        super.init();
        readers = new HashMap<>();
        mapper = new ObjectMapper();
        mapper.registerModule(new JaxbAnnotationModule());
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(TypeFactory.defaultInstance()));
        
        ObjectReader voidReader = mapper.readerFor(VoidResponse.class);
        
        readers.put(Suffix.GRANT_SYSTEM_PERM, voidReader);
        readers.put(Suffix.REVOKE_SYSTEM_PERM, voidReader);
        readers.put(Suffix.GRANT_TABLE_PERM, voidReader);
        readers.put(Suffix.REVOKE_TABLE_PERM, voidReader);
        readers.put(Suffix.CREATE_TABLE, voidReader);
        readers.put(Suffix.FLUSH_TABLE, voidReader);
        readers.put(Suffix.SET_TABLE_PROP, voidReader);
        readers.put(Suffix.REMOVE_TABLE_PROP, voidReader);
        readers.put(Suffix.UPDATE, mapper.readerFor(UpdateResponse.class));
        readers.put(Suffix.VALIDATE_VIZ, mapper.readerFor(ValidateVisibilityResponse.class));
        readers.put(Suffix.LIST_TABLES, mapper.readerFor(ListTablesResponse.class));
        readers.put(Suffix.LIST_USER_AUTHS, mapper.readerFor(ListUserAuthorizationsResponse.class));
        readers.put(Suffix.LIST_USER_PERM, mapper.readerFor(ListUserPermissionsResponse.class));
        readers.put(Suffix.LIST_USERS, mapper.readerFor(ListUsersResponse.class));
    }
    
    public VoidResponse grantSystemPermission(String userName, String permission) {
        Preconditions.checkState(StringUtils.isNotBlank(userName), "userName is required");
        Preconditions.checkState(StringUtils.isNotBlank(permission), "permission is required");
        return execPost(String.format(Suffix.GRANT_SYSTEM_PERM.get(), userName, permission), readers.get(Suffix.GRANT_SYSTEM_PERM), null);
    }
    
    public VoidResponse revokeSystemPermission(String userName, String permission) {
        Preconditions.checkState(StringUtils.isNotBlank(userName), "userName is required");
        Preconditions.checkState(StringUtils.isNotBlank(permission), "permission is required");
        return execPost(String.format(Suffix.REVOKE_SYSTEM_PERM.get(), userName, permission), readers.get(Suffix.REVOKE_SYSTEM_PERM), null);
    }
    
    public VoidResponse grantTablePermission(String userName, String tableName, String permission) {
        Preconditions.checkState(StringUtils.isNotBlank(userName), "userName is required");
        Preconditions.checkState(StringUtils.isNotBlank(tableName), "tableName is required");
        Preconditions.checkState(StringUtils.isNotBlank(permission), "permission is required");
        return execPost(String.format(Suffix.GRANT_TABLE_PERM.get(), userName, tableName, permission), readers.get(Suffix.GRANT_TABLE_PERM), null);
    }
    
    public VoidResponse revokeTablePermission(String userName, String tableName, String permission) {
        Preconditions.checkState(StringUtils.isNotBlank(userName), "userName is required");
        Preconditions.checkState(StringUtils.isNotBlank(tableName), "tableName is required");
        Preconditions.checkState(StringUtils.isNotBlank(permission), "permission is required");
        return execPost(String.format(Suffix.REVOKE_TABLE_PERM.get(), userName, tableName, permission), readers.get(Suffix.REVOKE_TABLE_PERM), null);
    }
    
    public VoidResponse createTable(String tableName) {
        Preconditions.checkState(StringUtils.isNotBlank(tableName), "tableName is required");
        return execPost(String.format(Suffix.CREATE_TABLE.get(), tableName), readers.get(Suffix.CREATE_TABLE), null);
    }
    
    public VoidResponse flushTable(String tableName) {
        Preconditions.checkState(StringUtils.isNotBlank(tableName), "tableName is required");
        return execPost(String.format(Suffix.FLUSH_TABLE.get(), tableName), readers.get(Suffix.FLUSH_TABLE), null);
    }
    
    public VoidResponse setTableProperty(String tableName, String propertyName, String propertyValue) {
        Preconditions.checkState(StringUtils.isNotBlank(tableName), "tableName is required");
        Preconditions.checkState(StringUtils.isNotBlank(propertyName), "propertyName is required");
        Preconditions.checkState(StringUtils.isNotBlank(propertyValue), "propertyValue is required");
        return execPost(String.format(Suffix.SET_TABLE_PROP.get(), tableName, propertyName, propertyValue), readers.get(Suffix.SET_TABLE_PROP), null);
    }
    
    public VoidResponse removeTableProperty(String tableName, String propertyName) {
        Preconditions.checkState(StringUtils.isNotBlank(tableName), "tableName is required");
        Preconditions.checkState(StringUtils.isNotBlank(propertyName), "propertyName is required");
        return execPost(String.format(Suffix.REMOVE_TABLE_PROP.get(), tableName, propertyName), readers.get(Suffix.REMOVE_TABLE_PROP), null);
    }
    
    public ListTablesResponse listTables() {
        return execGet(Suffix.LIST_TABLES.get(), readers.get(Suffix.LIST_TABLES));
    }
    
    public ListUserAuthorizationsResponse listUserAuthorizations(String userName) {
        Preconditions.checkState(StringUtils.isNotBlank(userName), "userName is required");
        return execGet(String.format(Suffix.LIST_USER_AUTHS.get(), userName), readers.get(Suffix.LIST_USER_AUTHS));
    }
    
    public ListUserPermissionsResponse listUserPermissions(String userName) {
        Preconditions.checkState(StringUtils.isNotBlank(userName), "userName is required");
        return execGet(String.format(Suffix.LIST_USER_PERM.get(), userName), readers.get(Suffix.LIST_USER_PERM));
    }
    
    public ListUsersResponse listUsers() {
        return execGet(Suffix.LIST_USERS.get(), readers.get(Suffix.LIST_USERS));
    }
    
    public UpdateResponse update(UpdateRequest updateRequest) {
        Preconditions.checkNotNull(updateRequest, "updateRequest is required");
        
        final StringEntity putBody;
        try {
            putBody = new StringEntity(mapper.writeValueAsString(updateRequest), StandardCharsets.UTF_8);
        } catch (IOException ioe) {
            throw new RuntimeException("updateRequest could not be serialized", ioe);
        }
        // @formatter:off
        return executePutMethodWithRuntimeException(
            Suffix.UPDATE.get(),
            uriBuilder -> {},
            httpPut -> {
                httpPut.setEntity(putBody);
                httpPut.setHeader("Authorization", getBearer());
                httpPut.setHeader("Content-type", "application/json");
            },
            entity -> readers.get(Suffix.UPDATE).readValue(entity.getContent()),
            () -> Suffix.UPDATE.get() + " [ " + putBody + " ]"
        );
        // @formatter:on
    }
    
    public ValidateVisibilityResponse validateVisibilities(String[] visibilities) {
        Preconditions.checkState(null != visibilities && visibilities.length > 0, "visibilities array cannot be null/empty");
        
        final List<NameValuePair> nvpList = new ArrayList<>();
        Arrays.stream(visibilities).forEach(s -> nvpList.add(new BasicNameValuePair("visibility", s)));
        final UrlEncodedFormEntity postBody = new UrlEncodedFormEntity(nvpList::iterator);
        
        // @formatter:off
        return execPost(
            Suffix.VALIDATE_VIZ.get(),
            readers.get(Suffix.VALIDATE_VIZ),
            postBody,
            () -> Suffix.VALIDATE_VIZ.get() + ": " + Arrays.toString(visibilities));
        // @formatter:on
    }
}
