package datawave.microservice.configcheck;

import static datawave.microservice.configcheck.XmlPropertyAnalyzer.PLACEHOLDERS_HEADER;
import static datawave.microservice.configcheck.XmlPropertyAnalyzer.REFS_HEADER;
import static datawave.microservice.configcheck.XmlPropertyAnalyzer.VALUES_HEADER;
import static datawave.microservice.configcheck.util.XmlRenderUtils.valueToObject;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class AnalysisComparator {
    private static final String FIRST_HEADER = "# FIRST: ";
    private static final String SECOND_HEADER = "# SECOND: ";
    private static final String FIRST = "<<<<<<< FIRST\n";
    private static final String SEPARATOR = "=======\n";
    private static final String SECOND = ">>>>>>> SECOND\n";
    
    private Analysis firstAnalysis;
    private Analysis secondAnalysis;
    
    public AnalysisComparator(Analysis firstAnalysis, Analysis secondAnalysis) {
        this.firstAnalysis = firstAnalysis;
        this.secondAnalysis = secondAnalysis;
    }
    
    public String compareAnalyses() {
        String output = "";
        if (firstAnalysis != null && secondAnalysis != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(FIRST_HEADER).append(firstAnalysis.getFile()).append("\n");
            sb.append(SECOND_HEADER).append(secondAnalysis.getFile()).append("\n");
            
            if (firstAnalysis.getPlaceholders() != null && !firstAnalysis.getPlaceholders().isEmpty() && secondAnalysis.getPlaceholders() != null
                            && !secondAnalysis.getPlaceholders().isEmpty()) {
                sb.append(PLACEHOLDERS_HEADER);
                sb.append(diffProperties(firstAnalysis.getPlaceholders(), secondAnalysis.getPlaceholders()));
                sb.append("\n");
            }
            
            if (firstAnalysis.getValues() != null && !firstAnalysis.getValues().isEmpty() && secondAnalysis.getValues() != null
                            && !secondAnalysis.getValues().isEmpty()) {
                sb.append(VALUES_HEADER);
                sb.append(diffProperties(firstAnalysis.getValues(), secondAnalysis.getValues()));
                sb.append("\n");
            }
            
            if (firstAnalysis.getRefs() != null && !firstAnalysis.getRefs().isEmpty() && secondAnalysis.getRefs() != null
                            && !secondAnalysis.getRefs().isEmpty()) {
                sb.append(REFS_HEADER);
                sb.append(diffProperties(firstAnalysis.getRefs(), secondAnalysis.getRefs()));
                sb.append("\n");
            }
            
            output = sb.toString().trim();
        }
        return output;
    }
    
    private String diffProperties(String first, String second) {
        StringBuilder sb = new StringBuilder();
        Map<String,Object> firstMap = Arrays.stream(first.split("\ndoc.")).map(x -> x.split(": ", 2))
                        .collect(Collectors.toMap(x -> addPrefix(x[0], "doc."), x -> valueToObject(x[1])));
        Map<String,Object> secondMap = Arrays.stream(second.split("\ndoc.")).map(x -> x.split(": ", 2))
                        .collect(Collectors.toMap(x -> addPrefix(x[0], "doc."), x -> valueToObject(x[1])));
        
        Set<String> sortedKeys = new TreeSet<>();
        sortedKeys.addAll(firstMap.keySet());
        sortedKeys.addAll(secondMap.keySet());
        
        boolean buildingDiff = false;
        boolean buildingMissing = false;
        StringBuilder firstBuilder = new StringBuilder();
        StringBuilder secondBuilder = new StringBuilder();
        for (String key : sortedKeys) {
            boolean firstContains = firstMap.containsKey(key);
            boolean secondContains = secondMap.containsKey(key);
            boolean valuesMatch = firstContains && secondContains && Objects.equals(firstMap.get(key), secondMap.get(key));
            
            if (valuesMatch) {
                if (buildingDiff) {
                    writeDiff(sb, firstBuilder, secondBuilder);
                    buildingDiff = false;
                } else if (buildingMissing) {
                    writeDiff(sb, firstBuilder, secondBuilder);
                    buildingMissing = false;
                }
                sb.append(key).append(": ").append(firstMap.get(key)).append("\n");
            } else {
                if (firstContains && secondContains) {
                    if (buildingMissing) {
                        writeDiff(sb, firstBuilder, secondBuilder);
                        buildingMissing = false;
                    }
                    buildingDiff = true;
                } else {
                    if (buildingDiff) {
                        writeDiff(sb, firstBuilder, secondBuilder);
                        buildingDiff = false;
                    }
                    buildingMissing = true;
                }
                
                if (firstContains) {
                    firstBuilder.append(key).append(": ").append(firstMap.get(key)).append("\n");
                }
                if (secondContains) {
                    secondBuilder.append(key).append(": ").append(secondMap.get(key)).append("\n");
                }
            }
        }
        if (buildingDiff || buildingMissing) {
            writeDiff(sb, firstBuilder, secondBuilder);
        }
        return sb.toString();
    }
    
    private String addPrefix(String key, String prefix) {
        if (!key.startsWith(prefix)) {
            key = prefix + key;
        }
        return key;
    }
    
    private void writeDiff(StringBuilder sb, StringBuilder first, StringBuilder second) {
        sb.append(FIRST);
        sb.append(first);
        sb.append(SEPARATOR);
        sb.append(second);
        sb.append(SECOND);
        first.setLength(0);
        second.setLength(0);
    }
}
