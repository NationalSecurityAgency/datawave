package datawave.core.query.cachedresults;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import datawave.data.type.Type;
import datawave.marking.AccessExpressionMarkings;
import datawave.marking.Markings;
import datawave.visibility.AccessExpressionUtil;
import datawave.visibility.VisibilityFunctions;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.data.ObjectSizeOf;
import datawave.webservice.query.util.TypedValue;

/**
 * Default {@link CacheableQueryRow}. Each column carries a column visibility expression; the row-level visibility ({@link #getColumnVisibility()}) is the
 * combination of all column visibilities using {@link VisibilityFunctions}.
 */
public class CacheableQueryRowImpl extends CacheableQueryRow implements ObjectSizeOf {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheableQueryRowImpl.class);

    private String user = null;
    private String queryId = null;
    private String logicName = null;
    private String dataType = null;
    private String eventId = null;
    private String row = null;
    private String colFam = null;
    private Map<String,String> columnColumnVisibilityMap = new HashMap<>();
    private Map<String,String> columnTypeMap = new HashMap<>();
    private Map<String,Long> columnTimestampMap = new HashMap<>();
    private Map<String,Set<String>> columnValues = new HashMap<>();
    private Set<String> variableColumnNames = new TreeSet<>();
    private String queryOrigin = null;
    private String resultOrigin = null;

    /**
     * Add a column value.
     *
     * @param columnName
     *            the column name; spaces are replaced with underscores
     * @param columnValueString
     *            the column value
     * @param columnVisibility
     *            the column visibility expression of the value; null means no visibility
     * @param timestamp
     *            the timestamp of the value
     */
    public void addColumn(String columnName, String columnValueString, String columnVisibility, Long timestamp) {
        addColumn(columnName, new TypedValue(columnValueString), columnVisibility, timestamp);
    }

    /**
     * Add a column value. The value's visibility is combined into the row-level visibility and, if the column already has a different visibility, into the
     * column's visibility using {@link VisibilityFunctions}.
     *
     * @param columnName
     *            the column name; spaces are replaced with underscores
     * @param columnTypedValue
     *            the column value
     * @param columnVisibility
     *            the column visibility expression of the value; null means no visibility
     * @param timestamp
     *            the timestamp of the value
     */
    public void addColumn(String columnName, TypedValue columnTypedValue, String columnVisibility, Long timestamp) {

        final String cleanColumnName = columnName.replace(' ', '_');

        final String normalizedVisibility;
        try {
            normalizedVisibility = normalize(columnVisibility);
            this.columnVisibility = combine(this.columnVisibility, normalizedVisibility);
        } catch (IllegalArgumentException e) {
            LOGGER.error("Invalid column visibility {} skipping column {} = {}", columnVisibility, cleanColumnName, columnTypedValue, e);
            return;
        }

        Long currTimestamp = columnTimestampMap.get(cleanColumnName);
        if (currTimestamp == null || timestamp > currTimestamp) {
            columnTimestampMap.put(cleanColumnName, timestamp);
        }

        Type<?> datawaveType = null;
        String typedColumnName = "";
        if (columnTypedValue.getValue() instanceof Type<?>) {
            datawaveType = (Type<?>) columnTypedValue.getValue();
            typedColumnName = columnTypedValue.getType().replace(':', '_').toUpperCase();
            typedColumnName += "_" + cleanColumnName;
        }

        manageColumnInsert(datawaveType, cleanColumnName, columnTypedValue, normalizedVisibility);
        if (!typedColumnName.isEmpty() && !typedColumnName.startsWith("XS_STRING")) {
            manageColumnInsert(datawaveType, typedColumnName, columnTypedValue, normalizedVisibility);
        }
    }

    /**
     * @param columnName
     *            the column name
     * @param columnStringValue
     *            the column value
     * @param markings
     *            ignored unless {@code columnVisibility} is null or empty, in which case its access expression is used as the column visibility
     * @param columnVisibility
     *            the column visibility expression
     * @param timestamp
     *            the timestamp
     * @deprecated use {@link #addColumn(String, String, String, Long)}
     */
    // TODO remove-accumulo-utils
    @Override
    @Deprecated(forRemoval = true)
    public void addColumn(String columnName, String columnStringValue, Markings<?> markings, String columnVisibility, Long timestamp) {
        addColumn(columnName, columnStringValue, toVisibility(markings, columnVisibility), timestamp);
    }

    /**
     * @param columnName
     *            the column name
     * @param columnValue
     *            the column value
     * @param markings
     *            ignored unless {@code columnVisibility} is null or empty, in which case its access expression is used as the column visibility
     * @param columnVisibility
     *            the column visibility expression
     * @param timestamp
     *            the timestamp
     * @deprecated use {@link #addColumn(String, TypedValue, String, Long)}
     */
    // TODO remove-accumulo-utils
    @Override
    @Deprecated(forRemoval = true)
    public void addColumn(String columnName, TypedValue columnValue, Markings<?> markings, String columnVisibility, Long timestamp) {
        addColumn(columnName, columnValue, toVisibility(markings, columnVisibility), timestamp);
    }

    private void manageColumnInsert(Type<?> datawaveType, String columnName, TypedValue columnTypedValue, String columnVisibility) {
        if (!this.columnValues.containsKey(columnName)) {

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
            String currVisibility = columnColumnVisibilityMap.get(columnName);

            if (currVisibility == null) {
                // existing visibility was null so we update with new one
                columnColumnVisibilityMap.put(columnName, columnVisibility);
            } else if (!currVisibility.equals(columnVisibility)) {
                // the column now holds values with different visibilities, so it gets the combination of them
                columnColumnVisibilityMap.put(columnName, combine(currVisibility, columnVisibility));
            }
        }

    }

    /**
     * Normalize a visibility expression, as the response layer did when it translated visibilities.
     *
     * @param visibility
     *            the visibility expression, may be null
     * @return the normalized expression; null or empty input is returned unchanged
     * @throws IllegalArgumentException
     *             if the expression is invalid
     */
    private static String normalize(String visibility) {
        if (visibility == null || visibility.isEmpty()) {
            return visibility;
        }
        return AccessExpressionUtil.normalize(visibility).getExpression();
    }

    /**
     * Combine two visibility expressions using {@link VisibilityFunctions#combine(ColumnVisibility, ColumnVisibility)}, which preserves the absence of a
     * visibility (null).
     */
    private static String combine(String first, String second) {
        ColumnVisibility combined = VisibilityFunctions.getInstance().combine(toColumnVisibility(first), toColumnVisibility(second));
        return combined == null ? null : new String(combined.getExpression(), UTF_8);
    }

    private static ColumnVisibility toColumnVisibility(String visibility) {
        return visibility == null ? null : new ColumnVisibility(visibility);
    }

    // TODO remove-accumulo-utils: bridging helpers for the deprecated Markings-based members
    private static String toVisibility(Markings<?> markings, String columnVisibility) {
        if ((columnVisibility == null || columnVisibility.isEmpty()) && markings != null && !markings.isEmpty()) {
            return markings.toAccessExpression().getExpression();
        }
        return columnVisibility;
    }

    // TODO remove-accumulo-utils: bridging helpers for the deprecated Markings-based members
    private static Markings<?> toMarkings(String visibility) {
        return AccessExpressionMarkings.createMarkings(visibility);
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

    /**
     * @return the row-level visibility as markings
     * @deprecated use {@link #getColumnVisibility()}
     */
    // TODO remove-accumulo-utils
    @Override
    @Deprecated(forRemoval = true)
    public Markings<?> getMarkings() {
        return toMarkings(getColumnVisibility());
    }

    /**
     * @param columnName
     *            the column name
     * @return the column visibility as markings
     * @deprecated use {@link #getColumnVisibility(String)}
     */
    // TODO remove-accumulo-utils
    @Override
    @Deprecated(forRemoval = true)
    public Markings<?> getColumnMarkings(String columnName) {
        return toMarkings(getColumnVisibility(columnName));
    }

    public String getColumnTimestampString(Map<String,Integer> columnMap) {

        Map<Long,Set<String>> timestampMap = new HashMap<>();
        for (Map.Entry<String,Long> entry : columnTimestampMap.entrySet()) {
            String currColumnName = entry.getKey();
            Integer currColumnNumber = columnMap.get(currColumnName);
            if (currColumnNumber != null) {
                Long currLong = entry.getValue();
                Set<String> columnSet = timestampMap.computeIfAbsent(currLong, k -> new TreeSet<>());
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
        final String cleanColumnName = columnName.replace(' ', '_');
        String columnVisibility;
        if (this.columnColumnVisibilityMap.containsKey(cleanColumnName)) {
            columnVisibility = this.columnColumnVisibilityMap.get(cleanColumnName);
        } else {
            columnVisibility = this.columnColumnVisibilityMap.get("_DEFAULT_");
        }
        return columnVisibility;
    }

    public Long getColumnTimestamp(String columnName) {

        final String cleanColumnName = columnName.replace(' ', '_');
        Long timestamp = null;
        if (this.columnTimestampMap.containsKey(cleanColumnName)) {
            timestamp = this.columnTimestampMap.get(cleanColumnName);
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

    /**
     * @param markings
     *            the row-level markings; their access expression becomes the row-level visibility
     * @deprecated use {@link #setColumnVisibility(String)}
     */
    // TODO remove-accumulo-utils
    @Override
    @Deprecated(forRemoval = true)
    public void setMarkings(Markings<?> markings) {
        setColumnVisibility(toVisibility(markings, null));
    }

    /**
     * @param columnMarkingsMap
     *            per-column markings; their access expressions replace the per-column visibilities
     * @deprecated use {@link #setColumnColumnVisibilityMap(Map)}
     */
    // TODO remove-accumulo-utils
    @Override
    @Deprecated(forRemoval = true)
    public void setColumnMarkingsMap(Map<String,Markings<?>> columnMarkingsMap) {
        Map<String,String> visibilities = new HashMap<>();
        if (columnMarkingsMap != null) {
            columnMarkingsMap.forEach((column, markings) -> visibilities.put(column, toVisibility(markings, null)));
        }
        setColumnColumnVisibilityMap(visibilities);
    }

    public void setColumnTimestampMap(Map<String,Long> columnTimestampMap) {
        this.columnTimestampMap = columnTimestampMap;
    }

    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Encode the per-column visibilities for the {@code _column_markings_} column of the cached results table. The most common visibility is written first as
     * the default ({@code <visibility>\0 0}); every other visibility is appended as {@code \0\0<visibility>\0<comma separated column numbers>}. A column with
     * no visibility is written as the empty string.
     *
     * @param columnMap
     *            the column name to column number mapping
     * @return the encoded column visibilities
     */
    public String getColumnSecurityMarkingString(Map<String,Integer> columnMap) {

        int largestSetCount = 0;
        String largestSetVisibility = "";
        Map<String,Set<String>> visibilityToField = new HashMap<>();
        for (Map.Entry<String,String> entry : columnColumnVisibilityMap.entrySet()) {
            String visibility = entry.getValue() == null ? "" : entry.getValue();
            Set<String> fields = visibilityToField.computeIfAbsent(visibility, k -> new HashSet<>());
            fields.add(entry.getKey());
            if (fields.size() > largestSetCount) {
                largestSetCount = fields.size();
                largestSetVisibility = visibility;
            }
        }

        StringBuilder sb = new StringBuilder();
        visibilityToField.remove(largestSetVisibility);
        sb.append(largestSetVisibility).append("\0").append(0);

        for (Map.Entry<String,Set<String>> entry : visibilityToField.entrySet()) {
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
        return new ArrayList<>(variableColumnNames);
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
