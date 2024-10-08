package datawave.microservice.configcheck;

import static datawave.microservice.configcheck.XmlPropertyAnalyzer.PLACEHOLDERS_HEADER;
import static datawave.microservice.configcheck.XmlPropertyAnalyzer.PROPERTIES_HEADER;
import static datawave.microservice.configcheck.XmlPropertyAnalyzer.REFS_HEADER;
import static datawave.microservice.configcheck.XmlPropertyAnalyzer.VALUES_HEADER;
import static datawave.microservice.configcheck.XmlPropertyAnalyzer.YML_HEADER;

/**
 * Analysis is used to represent an xml analysis, splitting the various sections of the report (full or partial) into separate values.
 */
public class Analysis {
    private String file;
    private String analysis;
    private String placeholders;
    private String values;
    private String refs;
    private String properties;
    private String yml;
    
    public Analysis(String file, String analysis) {
        this.file = file;
        this.analysis = analysis;
        decomposeAnalysis();
    }
    
    private void decomposeAnalysis() {
        if (analysis.contains(PLACEHOLDERS_HEADER) && analysis.contains(VALUES_HEADER) && analysis.contains(REFS_HEADER) && analysis.contains(PROPERTIES_HEADER)
                        && analysis.contains(YML_HEADER)) {
            placeholders = extractSection(PLACEHOLDERS_HEADER, VALUES_HEADER);
            values = extractSection(VALUES_HEADER, REFS_HEADER);
            refs = extractSection(REFS_HEADER, PROPERTIES_HEADER);
            properties = extractSection(PROPERTIES_HEADER, YML_HEADER);
            yml = extractSection(YML_HEADER);
        } else if (analysis.contains(VALUES_HEADER)) {
            values = extractSection(VALUES_HEADER);
        }
    }
    
    private String extractSection(String header) {
        return extractSection(header, null);
    }
    
    private String extractSection(String header, String nextHeader) {
        return analysis.substring(analysis.indexOf(header) + header.length(), (nextHeader != null) ? analysis.indexOf(nextHeader) : analysis.length());
    }
    
    public String getFile() {
        return file;
    }
    
    public void setFile(String file) {
        this.file = file;
    }
    
    public String getAnalysis() {
        return analysis;
    }
    
    public void setAnalysis(String analysis) {
        this.analysis = analysis;
    }
    
    public String getPlaceholders() {
        return placeholders;
    }
    
    public void setPlaceholders(String placeholders) {
        this.placeholders = placeholders;
    }
    
    public String getValues() {
        return values;
    }
    
    public void setValues(String values) {
        this.values = values;
    }
    
    public String getRefs() {
        return refs;
    }
    
    public void setRefs(String refs) {
        this.refs = refs;
    }
    
    public String getProperties() {
        return properties;
    }
    
    public void setProperties(String properties) {
        this.properties = properties;
    }
    
    public String getYml() {
        return yml;
    }
    
    public void setYml(String yml) {
        this.yml = yml;
    }
}
