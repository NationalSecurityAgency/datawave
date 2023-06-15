package datawave.ingest.mapreduce.handler.edge.define;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EdgeDefinitionConfigurationHelper {

    private static final Logger log = LoggerFactory.getLogger(EdgeDefinitionConfigurationHelper.class);

    private List<EdgeDefinition> edges;

    private String edgeAttribute2 = null;

    private String edgeAttribute3 = null;

    private String activityDateField = null;

    private Map<String,String> enrichmentTypeMappings;

    private boolean initialized = false;

    public List<EdgeDefinition> getEdges() {
        if (initialized) {
            return edges;
        } else {
            log.error("Edge definitions must be initialized first call the init() method");
            return null;
        }

    }

    public void setEdges(List<EdgeDefinition> edges) {
        this.edges = edges;
    }

    public void init(HashSet<String> edgeRelationships, HashSet<String> collectionType) {
        // Sanity check before we continue
        validateRequiredVariablesSet();

        List<EdgeDefinition> realEdges = new ArrayList<>();

        for (EdgeDefinition edgeDefinition : edges) {
            List<EdgeNode> edgeNodes = edgeDefinition.getAllPairs();
            if (edgeNodes != null) {
                int nPieces = edgeNodes.size();
                for (int ii = 0; ii < nPieces - 1; ii++) {
                    for (int jj = ii + 1; jj < nPieces; jj++) {

                        if (validateEdgeNode(edgeNodes.get(ii), edgeRelationships, collectionType)
                                        && validateEdgeNode(edgeNodes.get(jj), edgeRelationships, collectionType)) {

                            EdgeDefinition edgePair = buildEdgePair(edgeDefinition, edgeNodes.get(ii), edgeNodes.get(jj));

                            realEdges.add(edgePair);
                        }

                    }
                }
            } else if (edgeDefinition.getGroupPairs() != null) {

                for (EdgeNode group1 : edgeDefinition.getGroupPairs().getGroup1()) {
                    for (EdgeNode group2 : edgeDefinition.getGroupPairs().getGroup2()) {

                        if (validateEdgeNode(group1, edgeRelationships, collectionType) && validateEdgeNode(group2, edgeRelationships, collectionType)) {

                            EdgeDefinition groupPair = buildEdgePair(edgeDefinition, group1, group2);

                            realEdges.add(groupPair);
                        }

                    }
                }
            } else {
                realEdges.add(edgeDefinition);
            }

        }
        initialized = true;
        this.edges = realEdges;
    }

    public EdgeDefinition buildEdgePair(EdgeDefinition edgeDefinition, EdgeNode node1, EdgeNode node2) {
        EdgeDefinition edgePair = new EdgeDefinition();

        edgePair.setSourceFieldName(node1.getSelector());
        edgePair.setSourceCollection(node1.getCollection().toString());
        if (node1.getRelationship() != null) {
            edgePair.setSourceRelationship(node1.getRelationship());
        }
        if (node1.getRealm() != null) {
            edgePair.setSourceEventFieldRealm(node1.getRealm());
        }

        edgePair.setSinkFieldName((node2.getSelector()));
        edgePair.setSinkCollection(node2.getCollection().toString());
        if (node2.getRelationship() != null) {
            edgePair.setSinkRelationship(node2.getRelationship());
        }
        if (node2.getRealm() != null) {
            edgePair.setSinkEventFieldRealm(node2.getRealm());
        }

        edgePair.setEdgeType(edgeDefinition.getEdgeType());

        if (edgeDefinition.getDirection() != null) {
            edgePair.setDirection(edgeDefinition.getDirection().confLabel);
        }
        edgePair.setEnrichmentField(edgeDefinition.getEnrichmentField());

        if (edgeDefinition.getDownTime() != null) {
            edgePair.setDownTime(edgeDefinition.getDownTime());
        }
        if (edgeDefinition.getUpTime() != null) {
            edgePair.setUpTime(edgeDefinition.getUpTime());
        }
        if (edgeDefinition.getElapsedTime() != null) {
            edgePair.setElapsedTime(edgeDefinition.getElapsedTime());
        }
        if (edgeDefinition.getJexlPrecondition() != null) {
            edgePair.setJexlPrecondition(edgeDefinition.getJexlPrecondition());
        }

        edgePair.setIsGroupAware(edgeDefinition.isGroupAware());

        return edgePair;
    }

    private boolean validateEdgeNode(EdgeNode node, HashSet<String> edgeRelationships, HashSet<String> collectionType) {
        if (edgeRelationships.contains(node.getRelationship()) && collectionType.contains(node.getCollection())) {
            return true;
        } else {
            log.error("Edge Definition in config file does not have a matching edge relationship and collection type for Relationship: "
                            + node.getRelationship() + " and Collection: " + node.getCollection());
            return false;
        }
    }

    // Sanity checks to make sure configuration was set up properly
    private void validateRequiredVariablesSet() {

        if (edges == null) {
            throw new NullPointerException("EdgeDefinition configs are null!");
        } else if (edgeAttribute2 == null) {
            throw new NullPointerException("Edge Attribute2 field config is null!");
        } else if (edgeAttribute3 == null) {
            throw new NullPointerException("Edge Attribute3 field config is null!");
        } else if (activityDateField == null) {
            throw new NullPointerException("Activity date field config is null!");
        }
    }

    public String getEdgeAttribute2() {
        return edgeAttribute2;
    }

    public void setEdgeAttribute2(String edgeAttribute2) {
        this.edgeAttribute2 = edgeAttribute2;
    }

    public String getEdgeAttribute3() {
        return edgeAttribute3;
    }

    public void setEdgeAttribute3(String edgeAttribute3) {
        this.edgeAttribute3 = edgeAttribute3;
    }

    public String getActivityDateField() {
        return activityDateField;
    }

    public void setActivityDateField(String activityDateField) {
        this.activityDateField = activityDateField;
    }

    public Map<String,String> getEnrichmentTypeMappings() {
        return enrichmentTypeMappings;
    }

    public void setEnrichmentTypeMappings(Map<String,String> enrichmentTypeMappings) {
        this.enrichmentTypeMappings = enrichmentTypeMappings;
    }
}
