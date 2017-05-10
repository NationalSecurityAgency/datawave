package datawave.webservice.operations.configuration;

import datawave.webservice.common.audit.Auditor.AuditType;

public class LookupAuditConfiguration {
    
    // rowRegex, colFamRegex, colQualRegex will match everything if not configured
    private String tableRegex = null;
    private String rowRegex = ".*";
    private String colFamRegex = ".*";
    private String colQualRegex = ".*";
    private AuditType auditType = null;
    
    public LookupAuditConfiguration() {
        
    }
    
    public LookupAuditConfiguration(String tableRegex, String rowRegex, String colFamRegex, String colQualRegex, AuditType auditType) {
        
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
