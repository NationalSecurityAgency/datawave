package datawave.webservice.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotBlank;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "datawave.modification.handlers")
@Validated
public class ModificationHandlerProperties {
    List<String> authorizedRoles = new ArrayList<>();

    @NotBlank
    String eventTableName;
    @NotBlank
    String metadataTableName;
    @NotBlank
    String indexTableName;
    @NotBlank
    String reverseIndexTableName;

    List<String> securityMarkingExemptFields = new ArrayList<>();
    boolean requiresAudit = false;
    Map<String,String> indexOnlyMap = new HashMap<>();
    List<String> indexOnlySuffixes = new ArrayList<>();
    List<String> contentFields = new ArrayList<>();

    public List<String> getAuthorizedRoles() {
        return authorizedRoles;
    }

    public void setAuthorizedRoles(List<String> authorizedRoles) {
        this.authorizedRoles = authorizedRoles;
    }

    public String getEventTableName() {
        return eventTableName;
    }

    public void setEventTableName(String eventTableName) {
        this.eventTableName = eventTableName;
    }

    public String getMetadataTableName() {
        return metadataTableName;
    }

    public void setMetadataTableName(String metadataTableName) {
        this.metadataTableName = metadataTableName;
    }

    public String getIndexTableName() {
        return indexTableName;
    }

    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }

    public String getReverseIndexTableName() {
        return reverseIndexTableName;
    }

    public void setReverseIndexTableName(String reverseIndexTableName) {
        this.reverseIndexTableName = reverseIndexTableName;
    }

    public List<String> getSecurityMarkingExemptFields() {
        return securityMarkingExemptFields;
    }

    public void setSecurityMarkingExemptFields(List<String> securityMarkingExemptFields) {
        this.securityMarkingExemptFields = securityMarkingExemptFields;
    }

    public boolean isRequiresAudit() {
        return requiresAudit;
    }

    public void setRequiresAudit(boolean requiresAudit) {
        this.requiresAudit = requiresAudit;
    }

    public Map<String,String> getIndexOnlyMap() {
        return indexOnlyMap;
    }

    public void setIndexOnlyMap(Map<String,String> indexOnlyMap) {
        this.indexOnlyMap = indexOnlyMap;
    }

    public List<String> getIndexOnlySuffixes() {
        return indexOnlySuffixes;
    }

    public void setIndexOnlySuffixes(List<String> indexOnlySuffixes) {
        this.indexOnlySuffixes = indexOnlySuffixes;
    }

    public List<String> getContentFields() {
        return contentFields;
    }

    public void setContentFields(List<String> contentFields) {
        this.contentFields = contentFields;
    }
}
