package datawave.ingest.mapreduce.handler.edge.define;

import datawave.data.type.LcNoDiacriticsType;

import java.util.List;

/**
 * Helper class to parse the various Edge Definition formats
 *
 */
public class EdgeDefinition {
    
    public static final char EDGE_SEP = ',';
    private static final char TUP_SEP = ':';
    
    private static LcNoDiacriticsType lcNorm = new LcNoDiacriticsType();
    
    private boolean hasDuration = false;
    
    private boolean useRealm = false;
    
    private boolean useDefaultDuration = true;
    
    private String jexlPrecondition;
    
    private boolean isGroupAware = true;
    
    private String sourceRelationship;
    
    private String sinkRelationship;
    
    private String sourceFieldName;
    
    private String sinkFieldName;
    
    private String sourceEventFieldRealm;
    
    private String sinkEventFieldRealm;
    
    private String sourceIndexedFieldRealm;
    
    private String sinkIndexedFieldRealm;
    
    private EdgeDirection direction;
    
    private String edgeType;
    
    private String sourceCollection;
    
    private String sinkCollection;
    
    private String ID;
    
    private String enrichmentField;
    
    private boolean isEnrichmentEdge;
    
    private String upTime;
    
    private String downTime;
    
    private String elapsedTime;
    
    private boolean duration = false;
    
    private boolean isUDDuration = false;
    
    private List<EdgeNode> allPairs;
    
    private EdgeGroup groupPairs;
    
    public String getSourceEventFieldRealm() {
        return sourceEventFieldRealm;
    }
    
    public void setSourceEventFieldRealm(String sourceEventFieldRealm) {
        this.sourceEventFieldRealm = "<" + sourceEventFieldRealm + ">";
        try {
            this.sourceIndexedFieldRealm = "<" + lcNorm.normalize(sourceEventFieldRealm) + ">";
        } catch (Exception e) {
            // couldn't normalize this field TODO: verify.
            this.sourceIndexedFieldRealm = "<" + sourceEventFieldRealm + ">";
        }
        useRealm = true;
    }
    
    public String getSinkEventFieldRealm() {
        return sinkEventFieldRealm;
    }
    
    public void setSinkEventFieldRealm(String sinkEventFieldRealm) {
        this.sinkEventFieldRealm = "<" + sinkEventFieldRealm + ">";
        try {
            this.sinkIndexedFieldRealm = "<" + lcNorm.normalize(sinkEventFieldRealm) + ">";
        } catch (Exception e) {
            // couldn't normalize this field TODO: verify.
            this.sinkIndexedFieldRealm = "<" + sinkEventFieldRealm + ">";
        }
        
        useRealm = true;
    }
    
    public String getSourceIndexedFieldRealm() {
        return sourceIndexedFieldRealm;
    }
    
    public boolean isUseRealm() {
        return useRealm;
    }
    
    public String getSinkIndexedFieldRealm() {
        return sinkIndexedFieldRealm;
    }
    
    public String getSourceRelationship() {
        return sourceRelationship;
    }
    
    public void setSourceRelationship(String sourceRelationship) {
        
        this.sourceRelationship = sourceRelationship;
    }
    
    public String getSinkRelationship() {
        return sinkRelationship;
    }
    
    public void setSinkRelationship(String sinkRelationship) {
        
        this.sinkRelationship = sinkRelationship;
    }
    
    public String getSourceFieldName() {
        return sourceFieldName;
    }
    
    public void setSourceFieldName(String sourceFieldName) {
        this.sourceFieldName = sourceFieldName;
    }
    
    public String getSinkFieldName() {
        return sinkFieldName;
    }
    
    public void setSinkFieldName(String sinkFieldName) {
        this.sinkFieldName = sinkFieldName;
    }
    
    public EdgeDirection getDirection() {
        return direction;
    }
    
    public void setDirection(String direction) {
        this.direction = EdgeDirection.parse(direction);
    }
    
    public String getEdgeType() {
        return edgeType;
    }
    
    public void setEdgeType(String edgeType) {
        this.edgeType = edgeType;
    }
    
    public String getSourceCollection() {
        return sourceCollection;
    }
    
    public void setSourceCollection(String sourceCollection) {
        this.sourceCollection = sourceCollection;
    }
    
    public String getSinkCollection() {
        return sinkCollection;
    }
    
    public void setSinkCollection(String sinkCollection) {
        this.sinkCollection = sinkCollection;
    }
    
    public String getID() {
        return ID;
    }
    
    public void setID(String ID) {
        this.ID = ID;
    }
    
    public String getEnrichmentField() {
        return enrichmentField;
    }
    
    public void setEnrichmentField(String enrichmentField) {
        this.enrichmentField = enrichmentField;
    }
    
    public boolean isEnrichmentEdge() {
        return isEnrichmentEdge;
    }
    
    public void setEnrichmentEdge(boolean isEnrichmentEdge) {
        this.isEnrichmentEdge = isEnrichmentEdge;
    }
    
    public String getUpTime() {
        return upTime;
    }
    
    public void setUpTime(String upTime) {
        this.upTime = upTime;
        duration = true;
        isUDDuration = true;
    }
    
    public String getDownTime() {
        return downTime;
    }
    
    public void setDownTime(String downTime) {
        this.downTime = downTime;
        duration = true;
        isUDDuration = true;
    }
    
    public String getElapsedTime() {
        return elapsedTime;
    }
    
    public void setElapsedTime(String elapsedTime) {
        this.elapsedTime = elapsedTime;
        duration = true;
        isUDDuration = false;
    }
    
    public boolean hasDuration() {
        return duration;
    }
    
    public boolean getUDDuration() {
        return isUDDuration;
    }
    
    public List<EdgeNode> getAllPairs() {
        return allPairs;
    }
    
    public void setAllPairs(List<EdgeNode> allPairs) {
        this.allPairs = allPairs;
    }
    
    public EdgeGroup getGroupPairs() {
        return groupPairs;
    }
    
    public void setGroupPairs(EdgeGroup groupPairs) {
        this.groupPairs = groupPairs;
    }
    
    public String comparisonString() {
        StringBuilder representation = new StringBuilder();
        
        representation.append("\nEdgeType=" + getEdgeType());
        
        representation.append("\nSourceFieldName=" + getSourceFieldName());
        
        representation.append("\nSourceRelationship=" + getSourceRelationship());
        
        representation.append("\nSourceCollection=" + getSourceCollection());
        
        representation.append("\nSourceRealm=" + getSourceIndexedFieldRealm());
        
        representation.append("\nSinkFieldName=" + getSinkFieldName());
        
        representation.append("\nSinkRelationship=" + getSinkRelationship());
        
        representation.append("\nSinkCollection=" + getSinkCollection());
        
        representation.append("\nSinkRealm=" + getSinkIndexedFieldRealm());
        
        representation.append("\nEnrichmentField=" + getEnrichmentField());
        
        representation.append("\nisEnrichmentEdge=" + isEnrichmentEdge());
        
        representation.append("\nDirection=" + getDirection());
        
        representation.append("\nSinkEventFieldRealm=" + getSinkEventFieldRealm());
        
        representation.append("\nSourceEventFieldRealm=" + getSourceEventFieldRealm());
        
        representation.append("\nhasDuration=" + hasDuration());
        
        if (hasDuration()) {
            representation.append("\nDownTime=" + getDownTime());
            
            representation.append("\nUpTime=" + getUpTime());
            
            representation.append("\nElapsedTime=" + getElapsedTime());
            
            representation.append("\nisUDDuration=" + isUDDuration);
        }
        
        if (null != getJexlPrecondition()) {
            representation.append("\nPrecondition=" + getJexlPrecondition());
        }
        
        return representation.toString();
    }
    
    public String getJexlPrecondition() {
        return jexlPrecondition;
    }
    
    public void setJexlPrecondition(String jexlPrecondition) {
        this.jexlPrecondition = jexlPrecondition;
    }
    
    public boolean hasJexlPrecondition() {
        if (null == getJexlPrecondition()) {
            return false;
        } else {
            return true;
        }
    }
    
    public boolean isGroupAware() {
        return isGroupAware;
    }
    
    public void setIsGroupAware(boolean aware) {
        this.isGroupAware = aware;
    }
}
