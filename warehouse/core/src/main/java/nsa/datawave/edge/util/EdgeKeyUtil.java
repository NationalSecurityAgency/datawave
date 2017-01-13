package nsa.datawave.edge.util;

import nsa.datawave.edge.model.EdgeModelAware.Fields.FieldKey;

import nsa.datawave.data.type.Type;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.hadoop.io.Text;

import java.nio.charset.CharacterCodingException;
import java.util.*;

/**
 * Utility class for generating regular expressions to scan various formats of the edge table.
 */
public class EdgeKeyUtil {
    private static final String edgeTypePrefix = "(?:^|STATS/[^/]+/)";
    
    public static Set<String> normalizeSource(String source, List<? extends Type<?>> dataTypes, boolean protobuffEdgeFormat) {
        Set<String> normalized = new HashSet<>();
        
        if (dataTypes != null) {
            
            for (Type<?> dataType : dataTypes) {
                try {
                    String normalizedSource = dataType.normalize(source);
                    if (normalizedSource == null || "".equals(normalizedSource.trim())) {
                        continue;
                    }
                    
                    normalized.add(normalizedSource);
                } catch (Exception e) {
                    // ignore -- couldn't normalize with this normalizer
                }
            }
        }
        
        return normalized;
    }
    
    public static Iterable<? extends String> normalizeRegexSource(String source, List<? extends Type<?>> dataTypes, boolean protobuffEdgeFormat) {
        Set<String> normalized = new HashSet<>();
        
        if (dataTypes != null) {
            
            for (Type<?> dataType : dataTypes) {
                try {
                    String normalizedSource = dataType.normalizeRegex(source);
                    if (normalizedSource == null || "".equals(normalizedSource.trim())) {
                        continue;
                    }
                    
                    normalized.add(normalizedSource);
                } catch (Exception e) {
                    // ignore -- couldn't normalize with this normalizer
                }
            }
        }
        
        return normalized;
    }
    
    /**
     * Method to break an edge key into it's respective parts and place them in a map of edge fieldname => value.
     *
     * Warning, will neglect to add fields if the key is corrupted. Test for null in return value to use safely.
     *
     * @param key
     *            - Accumulo key to parse
     *
     * @return Map of @EdgeKeyUtil.FieldName to value String representing the value for this key.
     *
     *         protofuf: SOURCE%00;SINK EDGE_TYPE/EDGE_RELATIONSHIP:DATE/EDGE_ATTRIBUTE1/EDGE_ATTRIBUTE2/EDGE_ATTRIBUTE3");
     *
     *         non-proto: SOURCE%00;SINK EDGE_TYPE/EDGE_RELATIONSHIP/EDGE_ATTRIBUTE1/EDGE_ATTRIBUTE2/EDGE_ATTRIBUTE3:DATE");
     *
     */
    public static Map<FieldKey,String> dissasembleKey(Key key, boolean protobuffEdgeFormat) {
        Map<FieldKey,String> value = new HashMap<>();
        String row = key.getRow().toString();
        String colFam = key.getColumnFamily().toString();
        String colQual = key.getColumnQualifier().toString();
        
        String[] rowParts = row.split("\000");
        
        if (rowParts.length == 2) {
            value.put(FieldKey.EDGE_SOURCE, rowParts[0]);
            value.put(FieldKey.EDGE_SINK, rowParts[1]);
        } else if (rowParts.length == 1) {
            value.put(FieldKey.EDGE_SOURCE, rowParts[0]);
        }
        
        if (!colFam.startsWith("STATS")) {
            if (protobuffEdgeFormat) {
                String[] colFamParts = colFam.split("/");
                
                if (colFamParts.length >= 2) {
                    value.put(FieldKey.EDGE_TYPE, colFamParts[0]);
                    value.put(FieldKey.EDGE_RELATIONSHIP, colFamParts[1]);
                }
                
                String[] colQualParts = colQual.split("/");
                
                if (colQualParts.length >= 1) {
                    value.put(FieldKey.DATE, colQualParts[0]);
                }
                
                if (colQualParts.length >= 2) {
                    value.put(FieldKey.EDGE_ATTRIBUTE1, colQualParts[1]);
                }
                
                if (colQualParts.length >= 3) {
                    value.put(FieldKey.EDGE_ATTRIBUTE2, colQualParts[2]);
                }
                if (colQualParts.length >= 4) {
                    value.put(FieldKey.EDGE_ATTRIBUTE3, colQualParts[3]);
                }
            } else {
                String[] colFamParts = colFam.split("/");
                
                if (colFamParts.length >= 2) {
                    value.put(FieldKey.EDGE_TYPE, colFamParts[0]);
                    value.put(FieldKey.EDGE_RELATIONSHIP, colFamParts[1]);
                }
                
                if (colFamParts.length >= 3) {
                    value.put(FieldKey.EDGE_ATTRIBUTE1, colFamParts[2]);
                }
                
                if (colFamParts.length >= 4) {
                    value.put(FieldKey.EDGE_ATTRIBUTE2, colFamParts[3]);
                }
                
                if (colFamParts.length >= 5) {
                    value.put(FieldKey.EDGE_ATTRIBUTE3, colFamParts[4]);
                }
                
                value.put(FieldKey.DATE, colQual);
            }
        } else {
            value.put(FieldKey.STATS_EDGE, "true");
            
            if (protobuffEdgeFormat) {
                String[] colFamParts = colFam.split("/");
                
                if (colFamParts.length >= 4) {
                    value.put(FieldKey.EDGE_TYPE, colFamParts[2]);
                    value.put(FieldKey.EDGE_RELATIONSHIP, colFamParts[3]);
                }
                
                String[] colQualParts = colQual.split("/");
                
                if (colQualParts.length >= 1) {
                    value.put(FieldKey.DATE, colQualParts[0]);
                }
                
                if (colQualParts.length >= 2) {
                    value.put(FieldKey.EDGE_ATTRIBUTE1, colQualParts[1]);
                }
                
                if (colQualParts.length >= 3) {
                    value.put(FieldKey.EDGE_ATTRIBUTE2, colQualParts[2]);
                }
                if (colQualParts.length >= 4) {
                    value.put(FieldKey.EDGE_ATTRIBUTE3, colQualParts[3]);
                }
            }
        }
        
        return value;
    }
    
    public static String getEdgeColumnFamilyRegex(String edgeType, String edgeRelationship, String edgeAttribute1) {
        StringBuilder cfsb = new StringBuilder();
        if (edgeType != null) {
            cfsb.append(edgeTypePrefix).append(edgeType).append("/");
            if (edgeRelationship != null && edgeAttribute1 == null) {
                cfsb.append(edgeRelationship);
            } else if (edgeRelationship != null && edgeAttribute1 != null) {
                cfsb.append(edgeRelationship).append("/").append(edgeAttribute1);
            } else if (edgeRelationship == null && edgeAttribute1 != null) {
                cfsb.append("[^/]+/").append(edgeAttribute1);
            } else {
                cfsb.append(".*");
            }
        } else if (edgeRelationship != null && edgeAttribute1 == null) {
            cfsb.append(edgeTypePrefix + "[^/]+/").append(edgeRelationship).append(".*");
        } else if (edgeAttribute1 != null && edgeRelationship == null) {
            cfsb.append(edgeTypePrefix + "[^/]+/[^/]+/").append(edgeAttribute1).append(".*");
        } else if (edgeAttribute1 != null && edgeRelationship != null) {
            cfsb.append(edgeTypePrefix + "[^/]+/").append(edgeRelationship).append("/").append(edgeAttribute1).append(".*");
        }
        
        return cfsb.toString();
    }
    
    public static String getEdgeRow(String source, String sink) {
        String str = source + "\000" + sink;
        return str;
    }
    
    public static String decodeDate(Text date) throws CharacterCodingException {
        return Text.decode(date.getBytes(), 0, 8);
    }
    
    // stuff for iterators
    public static Key getSeekToFutureKey(Key topKey, String startDate) {
        Key newKey = new Key(topKey.getRow(), topKey.getColumnFamily(), new Text(startDate));
        return newKey;
    }
    
    // Assuming we're not on the correct date yet.
    public static PartialKey getSeekToFuturePartialKey() {
        return PartialKey.ROW_COLFAM_COLQUAL;
    }
    
    // date is after start date so we are no longer looking at dates...?
    public static PartialKey getSeekToNextKey() {
        PartialKey part = PartialKey.ROW_COLFAM;
        return part;
    }
    
    /**
     *
     * @param fieldName
     * @param fieldValue
     * @param protobufEdgeFormat
     * @return
     */
    @Deprecated
    public static String getCFRegexForEQNode(String fieldName, String fieldValue, boolean protobufEdgeFormat) {
        StringBuilder regex = new StringBuilder();
        
        if (FieldKey.EDGE_TYPE == FieldKey.parse(fieldName)) {
            regex.append(edgeTypePrefix).append(fieldValue.toUpperCase()).append("/.*");
        } else {
            regex.append(edgeTypePrefix + ".+/" + fieldValue.toUpperCase());
            if (!protobufEdgeFormat) {
                regex.append("/.*");
            }
        }
        
        return regex.toString();
    }
    
    @Deprecated
    public static String getEdgeRowRegex(String source, String sink) {
        String str = source + ".*\0" + sink + "$";
        
        return str;
    }
    
    @Deprecated
    public static String getEdgeRowRegex(String sourcePattern) {
        String str = "";
        
        return str;
    }
    
    @Deprecated
    public static String getProtobufEdgeColumnFamilyRegex(String edgeType, String edgeRelationship) {
        String columnFamily = "";
        
        if (edgeType != null) {
            columnFamily += edgeTypePrefix + edgeType + "/";
            if (edgeRelationship != null) {
                columnFamily += edgeRelationship;
            } else {
                columnFamily += ".*";
            }
        } else if (edgeRelationship != null) {
            columnFamily += edgeTypePrefix + ".+/" + edgeRelationship;
        }
        
        return columnFamily;
    }
    
    @Deprecated
    public static String getColumnQualifierRegexForProtobuf(String edgeAttribute1) {
        return "[0-9]+/" + edgeAttribute1 + "/.*";
    }
    
    /**
     * Get Column family Regex string for an or node.
     *
     * @param lFieldName
     *            - Left child field name
     * @param lFieldValue
     *            - Left child field value
     * @param rFieldName
     *            - Right child field name
     * @param rFieldValue
     *            - Right child field value
     * @param protobufEdgeFormat
     *            - Are we querying protobufEdge?
     * @return
     */
    @Deprecated
    public static String getCFRegexForAndNode(String lFieldName, String lFieldValue, String rFieldName, String rFieldValue, boolean protobufEdgeFormat) {
        StringBuilder regex = new StringBuilder();
        
        String edgeType = (FieldKey.EDGE_TYPE == FieldKey.parse(lFieldName)) ? lFieldValue : rFieldValue;
        String edgeRelationship = (FieldKey.EDGE_RELATIONSHIP == FieldKey.parse(lFieldName)) ? lFieldValue : rFieldValue;
        
        regex.append(edgeTypePrefix).append(edgeType.toUpperCase()).append('/').append(edgeRelationship.toUpperCase());
        
        if (!protobufEdgeFormat) {
            regex.append("/.*");
        }
        
        return regex.toString();
    }
}
