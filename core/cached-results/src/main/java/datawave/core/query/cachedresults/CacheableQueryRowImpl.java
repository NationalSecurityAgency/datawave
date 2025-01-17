package datawave.core.query.cachedresults;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.data.type.Type;
import datawave.marking.MarkingFunctions;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.data.ObjectSizeOf;
import datawave.webservice.query.util.TypedValue;

public class CacheableQueryRowImpl extends CacheableQueryRow implements ObjectSizeOf {

    private static Logger log = LoggerFactory.getLogger(CacheableQueryRowImpl.class);

    private String user = null;
    private String queryId = null;
    private String logicName = null;
    private String dataType = null;
    private String eventId = null;
    private String row = null;
    private String colFam = null;
    private Map<String,String> markings = new HashMap<>();
    private Map<String,Map<String,String>> columnMarkingsMap = new HashMap<>();
    private Map<String,String> columnColumnVisibilityMap = new HashMap<>();
    private Map<String,String> columnTypeMap = Maps.newHashMap();
    private Map<String,Long> columnTimestampMap = new HashMap<>();
    private Map<String,Set<String>> columnValues = new HashMap<>();
    private Set<String> variableColumnNames = new TreeSet<>();
    private String queryOrigin = null;
    private String resultOrigin = null;

    public void addColumn(String columnName, String columnValueString, Map<String,String> markings, String columnVisibility, Long timestamp) {
        addColumn(columnName, new TypedValue(columnValueString), markings, columnVisibility, timestamp);
    }

    public void addColumn(String columnName, TypedValue columnTypedValue, Map<String,String> markings, String columnVisibility, Long timestamp) {

        columnName = columnName.replaceAll(" ", "_");

        // if new markings are the same as the old markings, skip all of this
        // they are the same and the markings value has already been validated
        if (this.markings.equals(markings) == false) {
            if (this.markings.isEmpty()) {
                // validate the markings
                try {
                    markingFunctions.translateToColumnVisibility(markings);
                    if (this.markings.isEmpty()) {
                        // markings were empty, so use the one passed in.
                        this.markings = markings;
                    }
                } catch (MarkingFunctions.Exception e) {
                    log.error("Invalid markings {} skipping column {} = {}", markings, columnName, columnTypedValue, e);
                    return;
                }
            } else {
                try {
                    Set<ColumnVisibility> columnVisibilities = Sets.newHashSet();
                    columnVisibilities.add(markingFunctions.translateToColumnVisibility(this.markings));
                    columnVisibilities.add(markingFunctions.translateToColumnVisibility(markings));
                    ColumnVisibility combinedVisibility = markingFunctions.combine(columnVisibilities);

                    // use combined marking as new markings
                    this.markings = markingFunctions.translateFromColumnVisibility(combinedVisibility);
                } catch (MarkingFunctions.Exception e) {
                    log.error("Invalid markings {} skipping column {} = {}", markings, columnName, columnTypedValue, e);
                    return;
                }
            }
        }

        Long currTimestamp = columnTimestampMap.get(columnName);
        if (currTimestamp == null || timestamp > currTimestamp) {
            columnTimestampMap.put(columnName, timestamp);
        }

        Type<?> datawaveType = null;
        String typedColumnName = "";
        if (columnTypedValue.getValue() instanceof Type<?>) {
            datawaveType = (Type<?>) columnTypedValue.getValue();
            typedColumnName = columnTypedValue.getType().replaceAll(":", "_").toUpperCase();
            typedColumnName += "_" + columnName;
        }

        manageColumnInsert(datawaveType, columnName, columnTypedValue, markings, columnVisibility);
        if (!typedColumnName.isEmpty() && typedColumnName.startsWith("XS_STRING") == false) {
            manageColumnInsert(datawaveType, typedColumnName, columnTypedValue, markings, columnVisibility);
        }
    }

    private void manageColumnInsert(Type<?> datawaveType, String columnName, TypedValue columnTypedValue, Map<String,String> markings,
                    String columnVisibility) {
        if (this.columnValues.containsKey(columnName) == false) {

            Set<String> valuesSet = Sets.newLinkedHashSet();
            if (datawaveType != null) {
                if (columnName.startsWith("XS")) {
                    valuesSet.add(datawaveType.getNormalizedValue());
                } else {
                    valuesSet.add(datawaveType.getDelegate().toString());
                }
                this.columnTypeMap.put(columnName, datawaveType.getClass().toString());
            } else {
                valuesSet.add(columnTypedValue.getValue().toString());
            }
            this.columnValues.put(columnName, valuesSet);
            this.columnMarkingsMap.put(columnName, markings);
            this.columnColumnVisibilityMap.put(columnName, columnVisibility);
        } else {

            if (datawaveType != null) {
                if (columnName.startsWith("XS")) {
                    columnValues.get(columnName).add(datawaveType.getNormalizedValue());
                } else {
                    columnValues.get(columnName).add(datawaveType.getDelegate().toString());
                }
                this.columnTypeMap.put(columnName, datawaveType.getClass().toString());
            } else {
                columnValues.get(columnName).add(columnTypedValue.getValue().toString());
            }
            Map<String,String> currMarkings = columnMarkingsMap.get(columnName);

            if (currMarkings.equals(markings) == false) {
                try {
                    Set<ColumnVisibility> columnVisibilities = Sets.newHashSet();
                    columnVisibilities.add(markingFunctions.translateToColumnVisibility(currMarkings));
                    columnVisibilities.add(markingFunctions.translateToColumnVisibility(markings));
                    ColumnVisibility combinedVisibility = markingFunctions.combine(columnVisibilities);
                    Map<String,String> minMarkings = markingFunctions.translateFromColumnVisibility(combinedVisibility);

                    // use combined marking as new markings
                    columnMarkingsMap.put(columnName, minMarkings);
                    // new combined marking means that this colVis is greater than old colVis
                    columnColumnVisibilityMap.put(columnName, columnVisibility);
                } catch (MarkingFunctions.Exception e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            }
        }

    }

    public String getUser() {
        return user;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getLogicName() {
        return logicName;
    }

    public String getDataType() {
        return dataType;
    }

    public String getEventId() {
        return eventId;
    }

    public String getRow() {
        return row;
    }

    public String getColFam() {
        return colFam;
    }

    public Map<String,String> getMarkings() {
        return Maps.newHashMap(markings);
    }

    public Map<String,String> getColumnMarkings(String columnName) {
        columnName = columnName.replaceAll(" ", "_");
        Map<String,String> markings = null;
        if (this.columnMarkingsMap.containsKey(columnName)) {
            markings = this.columnMarkingsMap.get(columnName);
        } else {
            markings = this.columnMarkingsMap.get("_DEFAULT_");
        }

        if (markings == null)
            markings = Maps.newHashMap();
        return markings;
    }

    public String getColumnTimestampString(Map<String,Integer> columnMap) {

        Map<Long,Set<String>> timestampMap = new HashMap<>();
        for (Map.Entry<String,Long> entry : columnTimestampMap.entrySet()) {
            String currColumnName = entry.getKey();
            Integer currColumnNumber = columnMap.get(currColumnName);
            if (currColumnNumber != null) {
                Long currLong = entry.getValue();
                Set<String> columnSet = timestampMap.get(currLong);
                if (columnSet == null) {
                    columnSet = new TreeSet<>();
                    timestampMap.put(currLong, columnSet);
                }
                columnSet.add(currColumnName);
            }
        }

        int largestSetCount = 0;
        Long largestSetTimestamp = 0L;
        for (Map.Entry<Long,Set<String>> entry : timestampMap.entrySet()) {
            int currSize = entry.getValue().size();
            if (currSize > largestSetCount) {
                largestSetCount = currSize;
                largestSetTimestamp = entry.getKey();
            }
        }

        StringBuilder sb = new StringBuilder();
        timestampMap.remove(largestSetTimestamp);
        sb.append(largestSetTimestamp).append("\0").append(0);

        for (Map.Entry<Long,Set<String>> entry : timestampMap.entrySet()) {
            sb.append("\0\0");
            sb.append(entry.getKey());
            sb.append("\0");
            sb.append(CacheableQueryRow.createColumnList(entry.getValue(), columnMap));
        }

        return sb.toString();
    }

    public String getColumnVisibility(String columnName) {
        columnName = columnName.replaceAll(" ", "_");
        String columnVisibility = null;
        if (this.columnColumnVisibilityMap.containsKey(columnName)) {
            columnVisibility = this.columnColumnVisibilityMap.get(columnName);
        } else {
            columnVisibility = this.columnColumnVisibilityMap.get("_DEFAULT_");
        }
        return columnVisibility;
    }

    public Long getColumnTimestamp(String columnName) {

        columnName = columnName.replaceAll(" ", "_");
        Long timestamp = null;
        if (this.columnTimestampMap.containsKey(columnName)) {
            timestamp = this.columnTimestampMap.get(columnName);
        } else {
            timestamp = this.columnTimestampMap.get("_DEFAULT_");
        }
        return timestamp;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public void setLogicName(String logicName) {
        this.logicName = logicName;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public void setRow(String row) {
        this.row = row;
    }

    public void setColFam(String colFam) {
        this.colFam = colFam;
    }

    public void setMarkings(Map<String,String> markings) {
        // validate the markings
        try {
            markingFunctions.translateToColumnVisibility(markings);
            this.markings = markings;
        } catch (MarkingFunctions.Exception e) {
            log.error("Invalid markings {}", markings, e);
        }
    }

    public void setColumnMarkingsMap(Map<String,Map<String,String>> columnMarkingsMap) {
        this.columnMarkingsMap = columnMarkingsMap;
    }

    public void setColumnTimestampMap(Map<String,Long> columnTimestampMap) {
        this.columnTimestampMap = columnTimestampMap;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getColumnSecurityMarkingString(Map<String,Integer> columnMap) {

        Map<String,String> combinedMap = new HashMap<>();

        for (String field : columnMarkingsMap.keySet()) {

            // sort the map
            Map<String,String> m = columnMarkingsMap.get(field);
            String v = columnColumnVisibilityMap.get(field);
            if (m == null) {
                m = new HashMap<>();
            }
            // use TreeMap to sort the map
            String mStr = MarkingFunctions.Encoding.toString(new TreeMap<>(m));
            if (v == null) {
                combinedMap.put(field, mStr);
            } else {
                combinedMap.put(field, mStr + ':' + v);
            }

        }

        int largestSetCount = 0;
        String largestSetMarkingString = "";
        Map<String,Set<String>> markingToField = new HashMap<>();
        for (Map.Entry<String,String> entry : combinedMap.entrySet()) {
            String combinedMarking = entry.getValue();
            Set<String> fields = markingToField.get(combinedMarking);
            if (fields == null) {
                fields = new HashSet<>();
                markingToField.put(combinedMarking, fields);
            }
            fields.add(entry.getKey());
            if (fields.size() > largestSetCount) {
                largestSetCount = fields.size();
                largestSetMarkingString = combinedMarking;
            }
        }

        StringBuilder sb = new StringBuilder();
        markingToField.remove(largestSetMarkingString);
        sb.append(largestSetMarkingString).append("\0").append(0);

        for (Map.Entry<String,Set<String>> entry : markingToField.entrySet()) {
            sb.append("\0\0");
            sb.append(entry.getKey());
            sb.append("\0");
            sb.append(CacheableQueryRow.createColumnList(entry.getValue(), columnMap));
        }

        return sb.toString();
    }

    public Map<String,String> getColumnValues() {
        Map<String,String> columnValues = new HashMap<>();
        for (Map.Entry<String,Set<String>> entry : this.columnValues.entrySet()) {
            columnValues.put(entry.getKey(), StringUtils.join(entry.getValue(), ","));
        }
        return columnValues;
    }

    public void setColumnValues(Map<String,Set<String>> columnValues) {
        this.columnValues = columnValues;
    }

    public List<String> getVariableColumnNames() {
        List<String> l = new ArrayList<>();
        l.addAll(variableColumnNames);
        return l;
    }

    public void setVariableColumnNames(Collection<String> c) {
        this.variableColumnNames.clear();
        this.variableColumnNames.addAll(c);
    }

    public String getQueryOrigin() {
        return queryOrigin;
    }

    public void setQueryOrigin(String queryOrigin) {
        this.queryOrigin = queryOrigin;
    }

    public String getResultOrigin() {
        return resultOrigin;
    }

    public void setResultOrigin(String resultOrigin) {
        this.resultOrigin = resultOrigin;
    }

    public Map<String,String> getColumnColumnVisibilityMap() {
        return columnColumnVisibilityMap;
    }

    public void setColumnColumnVisibilityMap(Map<String,String> columnColumnVisibilityMap) {
        this.columnColumnVisibilityMap = columnColumnVisibilityMap;
    }

    // the approximate size in bytes of this event
    private long size = -1;

    /**
     * Set the size based on the number of characters stored in the DB
     *
     * @param size
     *            The number of characters
     */
    public void setSizeInStoredCharacters(long size) {
        // in practice the size of a row will be about 6 times the number of character bytes
        setSizeInBytes(size * 6);
    }

    /**
     * @param size
     *            the approximate size of this event in bytes
     */
    public void setSizeInBytes(long size) {
        this.size = size;
    }

    /**
     * @return The set size in bytes, -1 if unset
     */
    public long getSizeInBytes() {
        return this.size;
    }

    /**
     * Get the approximate size of this event in bytes. Used by the ObjectSizeOf mechanism in the webservice. Throws an exception if the local size was not set
     * to allow the ObjectSizeOf mechanism to do its thang.
     */
    @Override
    public long sizeInBytes() {
        if (size <= 0) {
            throw new UnsupportedOperationException();
        }
        return this.size;
    }

}
