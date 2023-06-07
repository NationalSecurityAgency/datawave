package datawave.edge.util;

import datawave.edge.model.EdgeModelAware.Fields.FieldKey;

import datawave.data.type.Type;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;

import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for generating regular expressions to scan various formats of the edge table.
 */
public class EdgeKeyUtil {
    protected static final String edgeTypePrefix = "(?:^|STATS/[^/]+/)";
    public static final String MAX_UNICODE_STRING = new String(Character.toChars(Character.MAX_CODE_POINT));
    
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
     * Method to break an edge key into it's respective parts and place them in a map of edge fieldname =&gt; value.
     *
     * Warning, will neglect to add fields if the key is corrupted. Test for null in return value to use safely.
     *
     * @param key
     *            - Accumulo key to parse
     * @param protobuffEdgeFormat
     *            - flag to check if the format is protobuf or not
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
        
        String[] rowParts = StringUtils.split(row, '\0');
        
        if (rowParts.length == 2) {
            value.put(FieldKey.EDGE_SOURCE, StringEscapeUtils.unescapeJava(rowParts[0]));
            value.put(FieldKey.EDGE_SINK, StringEscapeUtils.unescapeJava(rowParts[1]));
        } else if (rowParts.length == 1) {
            value.put(FieldKey.EDGE_SOURCE, StringEscapeUtils.unescapeJava(rowParts[0]));
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
    
    /**
     * Create escaped ranges over Edge Keys given only the source vertex
     * 
     * @param source
     *            - the source vertex
     * @param sourceRegex
     *            - flag indicating if range should include all keys with matching prefixes
     * @param includeStats
     *            - flag indicating if stats edges should be included in the range
     * @param includeRelationships
     *            - flag indicating if relationship edges should be included in the range
     * @return escaped ranges for the edge keys
     */
    public static Range createEscapedRange(String source, boolean sourceRegex, boolean includeStats, boolean includeRelationships) {
        Key start, end;
        String escapedSource = StringEscapeUtils.escapeJava(source);
        if (includeStats || sourceRegex) {
            start = new Key(escapedSource);
        } else {
            start = new Key(escapedSource + '\0');
        }
        
        if (!includeRelationships) {
            end = new Key(escapedSource + '\0');
        } else if (sourceRegex) {
            end = new Key(escapedSource + MAX_UNICODE_STRING);
        } else {
            end = new Key(escapedSource + '\1');
        }
        
        return new Range(start, end);
    }
    
    /**
     * Create escaped ranges over Edge Keys given source and sink vertex's
     * 
     * @param source
     *            - the source vertex
     * @param sink
     *            - the sink vertex
     * @param sinkRegex
     *            - flag indicating if range should include all keys with matching source+sink prefixes
     * @return escaped ranges for the given keys
     */
    public static Range createEscapedRange(String source, String sink, boolean sinkRegex) {
        Key start, end;
        String escapedSource = StringEscapeUtils.escapeJava(source);
        String escapedSink = StringEscapeUtils.escapeJava(sink);
        
        start = new Key(escapedSource + '\0' + escapedSink);
        
        if (sinkRegex) {
            end = new Key(escapedSource + '\0' + escapedSink + MAX_UNICODE_STRING);
        } else {
            end = new Key(escapedSource + '\0' + escapedSink + '\1');
        }
        
        return new Range(start, end);
    }
    
    public static String getEdgeRow(String source, String sink) {
        return source + "\000" + sink;
    }
    
    public static String decodeDate(Text date) throws CharacterCodingException {
        return Text.decode(date.getBytes(), 0, 8);
    }
    
    // stuff for iterators
    public static Key getSeekToFutureKey(Key topKey, String startDate) {
        return new Key(topKey.getRow(), topKey.getColumnFamily(), new Text(startDate));
    }
    
    // Assuming we're not on the correct date yet.
    public static PartialKey getSeekToFuturePartialKey() {
        return PartialKey.ROW_COLFAM_COLQUAL;
    }
    
    // date is after start date so we are no longer looking at dates...?
    public static PartialKey getSeekToNextKey() {
        return PartialKey.ROW_COLFAM;
    }
}
