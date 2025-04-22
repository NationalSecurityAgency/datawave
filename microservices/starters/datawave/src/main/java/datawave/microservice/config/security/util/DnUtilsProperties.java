package datawave.microservice.config.security.util;

import java.util.List;
import java.util.regex.Pattern;

import javax.validation.constraints.NotEmpty;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "datawave.security.util")
public class DnUtilsProperties {
    @NotEmpty
    private String subjectDnPattern;
    @NotEmpty
    private List<String> npeOuList;
    
    public Pattern getCompiledSubjectDnPattern() {
        return Pattern.compile(subjectDnPattern, Pattern.CASE_INSENSITIVE);
    }
    
    public String getSubjectDnPattern() {
        return subjectDnPattern;
    }
    
    public void setSubjectDnPattern(String subjectDnPattern) {
        this.subjectDnPattern = subjectDnPattern;
    }
    
    public List<String> getNpeOuList() {
        return npeOuList;
    }
    
    public void setNpeOuList(List<String> npeOuList) {
        this.npeOuList = npeOuList;
    }
}
