package datawave.microservice.config.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.swagger")
public class SwaggerProperties {
    private String title;
    private String description;
    private String licenseName = "Apache License 2.0";
    private String licenseUrl = "https://www.apache.org/licenses/LICENSE-2.0";
    private String externalDocsDesc = "DataWave documentation available on GitHub";
    private String externalDocsUrl = "https://github.com/NationalSecurityAgency/datawave";
    
    public String getTitle() {
        return title;
    }
    
    public void setTitle(String title) {
        this.title = title;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getLicenseName() {
        return licenseName;
    }
    
    public void setLicenseName(String licenseName) {
        this.licenseName = licenseName;
    }
    
    public String getLicenseUrl() {
        return licenseUrl;
    }
    
    public void setLicenseUrl(String licenseUrl) {
        this.licenseUrl = licenseUrl;
    }
    
    public String getExternalDocsDesc() {
        return externalDocsDesc;
    }
    
    public void setExternalDocsDesc(String externalDocsDesc) {
        this.externalDocsDesc = externalDocsDesc;
    }
    
    public String getExternalDocsUrl() {
        return externalDocsUrl;
    }
    
    public void setExternalDocsUrl(String externalDocsUrl) {
        this.externalDocsUrl = externalDocsUrl;
    }
}
