package datawave.microservice.configcheck;

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
    
    private String firstFile;
    private String firstAnalysis;
    private String secondFile;
    private String secondAnalysis;
    
    public AnalysisComparator(String firstFile, String firstAnalysis, String secondFile, String secondAnalysis) {
        this.firstFile = firstFile;
        this.firstAnalysis = firstAnalysis;
        this.secondFile = secondFile;
        this.secondAnalysis = secondAnalysis;
    }
    
    public String compareAnalyses() {
        String output = "";
        if (firstFile != null && firstAnalysis != null && secondFile != null && secondAnalysis != null) {
            
            Map<String,Object> firstMap = Arrays.stream(firstAnalysis.split("\ndoc.")).map(x -> x.split(": ", 2))
                            .collect(Collectors.toMap(x -> "doc." + x[0], x -> valueToObject(x[1])));
            Map<String,Object> secondMap = Arrays.stream(secondAnalysis.split("\ndoc.")).map(x -> x.split(": ", 2))
                            .collect(Collectors.toMap(x -> "doc." + x[0], x -> valueToObject(x[1])));
            
            Set<String> sortedKeys = new TreeSet<>();
            sortedKeys.addAll(firstMap.keySet());
            sortedKeys.addAll(secondMap.keySet());
            
            StringBuilder sb = new StringBuilder();
            sb.append(FIRST_HEADER).append(firstFile).append("\n");
            sb.append(SECOND_HEADER).append(secondFile).append("\n");
            
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
            output = sb.toString();
        }
        return output;
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
