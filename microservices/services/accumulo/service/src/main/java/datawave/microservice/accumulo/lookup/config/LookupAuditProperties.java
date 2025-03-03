package datawave.microservice.accumulo.lookup.config;

import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import datawave.webservice.common.audit.Auditor.AuditType;

@Validated
@ConfigurationProperties(prefix = "accumulo.lookup.audit")
public class LookupAuditProperties {
    
    /**
     * Default audit type to be assigned to lookup audit records
     */
    @NotNull
    private AuditType defaultAuditType = AuditType.ACTIVE;
    
    /**
     * Audit configs to be applied on a per-table/row/col basis, as matched against incoming query
     */
    private List<AuditConfiguration> tableConfig = new ArrayList<>();
    
    public AuditType getDefaultAuditType() {
        return defaultAuditType;
    }
    
    public void setDefaultAuditType(AuditType defaultAuditType) {
        this.defaultAuditType = defaultAuditType;
    }
    
    public List<AuditConfiguration> getTableConfig() {
        return tableConfig;
    }
    
    public void setTableConfig(List<AuditConfiguration> auditConfig) {
        this.tableConfig = auditConfig;
    }
    
    @Validated
    public static class AuditConfiguration {
        
        // rowRegex, colFamRegex, colQualRegex will match everything if not configured
        @NotBlank
        private String tableRegex = null;
        @NotBlank
        private String rowRegex = ".*";
        @NotBlank
        private String colFamRegex = ".*";
        @NotBlank
        private String colQualRegex = ".*";
        @NotNull
        private AuditType auditType = null;
        
        private AuditConfiguration() {}
        
        public AuditConfiguration(String tableRegex, String rowRegex, String colFamRegex, String colQualRegex, AuditType auditType) {
            
            this.tableRegex = tableRegex;
            if (rowRegex != null) {
                this.rowRegex = rowRegex;
            }
            if (colFamRegex != null) {
                this.colFamRegex = colFamRegex;
            }
            if (colQualRegex != null) {
                this.colQualRegex = colQualRegex;
            }
            this.auditType = auditType;
            
            validate();
        }
        
        public void validate() throws IllegalArgumentException {
            
            if (this.tableRegex == null) {
                throw new IllegalArgumentException("tableRegex can not be null");
            }
            if (this.auditType == null) {
                throw new IllegalArgumentException("auditType can not be null");
            }
        }
        
        public String getTableRegex() {
            return tableRegex;
        }
        
        public void setTableRegex(String tableRegex) {
            this.tableRegex = tableRegex;
        }
        
        public String getRowRegex() {
            return rowRegex;
        }
        
        public void setRowRegex(String rowRegex) {
            this.rowRegex = rowRegex;
        }
        
        public String getColFamRegex() {
            return colFamRegex;
        }
        
        public void setColFamRegex(String colFamRegex) {
            this.colFamRegex = colFamRegex;
        }
        
        public String getColQualRegex() {
            return colQualRegex;
        }
        
        public void setColQualRegex(String colQualRegex) {
            this.colQualRegex = colQualRegex;
        }
        
        public AuditType getAuditType() {
            return auditType;
        }
        
        public void setAuditType(AuditType auditType) {
            this.auditType = auditType;
        }
        
        public boolean isMatch(String table, String row, String colFam, String colQual) {
            
            boolean isMatch = false;
            
            // table == null should never happen, but this protects against a NPE
            if (table == null) {
                table = "";
            }
            // if row == null, we still want it to match against a rowRegex of .*
            if (row == null) {
                row = "";
            }
            // if colFam == null, we still want it to match against a colFamRegex of .*
            if (colFam == null) {
                colFam = "";
            }
            // if colQual == null, we still want it to match against a colQualRegex of .*
            if (colQual == null) {
                colQual = "";
            }
            
            if (this.tableRegex != null && this.rowRegex != null && this.colFamRegex != null && this.colQualRegex != null) {
                if (table.matches(this.tableRegex) && row.matches(this.rowRegex) && colFam.matches(this.colFamRegex) && colQual.matches(this.colQualRegex)) {
                    isMatch = true;
                }
            } else if (this.tableRegex != null && this.rowRegex != null && this.colFamRegex != null) {
                if (table.matches(this.tableRegex) && row.matches(this.rowRegex) && colFam.matches(this.colFamRegex)) {
                    isMatch = true;
                }
            } else if (this.tableRegex != null && this.rowRegex != null) {
                if (table.matches(this.tableRegex) && row.matches(this.rowRegex)) {
                    isMatch = true;
                }
            } else if (this.tableRegex != null) {
                if (table.matches(this.tableRegex)) {
                    isMatch = true;
                }
            }
            return isMatch;
        }
    }
}
