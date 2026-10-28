package datawave.webservice.config;

public class ModelProperties {

    private String defaultModelTableName = "datawave.metadata";
    private String jqueryUri = "/jquery.min.js";
    private String dataTablesUri = "/jquery.dataTables.min.js";
    private String systemName = "unknown";

    public String getDefaultModelTableName() {
        return defaultModelTableName;
    }

    public void setDefaultModelTableName(String defaultModelTableName) {
        this.defaultModelTableName = defaultModelTableName;
    }

    public String getJqueryUri() {
        return jqueryUri;
    }

    public void setJqueryUri(String jqueryUri) {
        this.jqueryUri = jqueryUri;
    }

    public String getDataTablesUri() {
        return dataTablesUri;
    }

    public void setDataTablesUri(String dataTablesUri) {
        this.dataTablesUri = dataTablesUri;
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }
}
