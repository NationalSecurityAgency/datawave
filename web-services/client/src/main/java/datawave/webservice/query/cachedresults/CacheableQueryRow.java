package datawave.webservice.query.cachedresults;

import datawave.marking.MarkingFunctions;
import datawave.webservice.query.result.event.HasMarkings;
import datawave.webservice.query.util.TypedValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class CacheableQueryRow implements HasMarkings {

    private static final Set<String> fixedColumnSet = new LinkedHashSet<String>();
    static {
        fixedColumnSet.add("_user_");
        fixedColumnSet.add("_queryId_");
        fixedColumnSet.add("_logicName_");
        fixedColumnSet.add("_datatype_");
        fixedColumnSet.add("_eventId_");
        fixedColumnSet.add("_row_");
        fixedColumnSet.add("_colf_");
        fixedColumnSet.add("_markings_");
        fixedColumnSet.add("_column_markings_");
        fixedColumnSet.add("_column_timestamps_");
    }

    protected MarkingFunctions markingFunctions;

    public static Set<String> getFixedColumnSet() {
        return Collections.unmodifiableSet(fixedColumnSet);
    }

    public abstract void addColumn(String columnName, String columnStringValue, Map<String,String> markings, String columnVisibility, Long timestamp);

    public abstract void addColumn(String columnName, TypedValue columnValue, Map<String,String> markings, String columnVisibility, Long timestamp);

    public abstract String getUser();

    public abstract String getQueryId();

    public abstract String getLogicName();

    public abstract String getDataType();

    public abstract String getEventId();

    public abstract String getRow();

    public abstract String getColFam();

    public abstract Map<String,String> getColumnMarkings(String columnName);

    public abstract String getColumnVisibility(String columnName);

    public abstract Map<String,String> getColumnValues();

    public abstract Long getColumnTimestamp(String columnName);

    public abstract void setQueryId(String queryId);

    public abstract void setLogicName(String logicName);

    public abstract void setDataType(String dataType);

    public abstract void setEventId(String eventId);

    public abstract void setRow(String row);

    public abstract void setColFam(String colFam);

    public abstract List<String> getVariableColumnNames();

    public abstract String getColumnTimestampString(Map<String,Integer> columnMap);

    public abstract String getColumnSecurityMarkingString(Map<String,Integer> columnMap);

    public abstract void setSizeInBytes(long sizeInBytes);

    protected static String createColumnList(Set<String> columnNames, Map<String,Integer> columnMap) {
        StringBuilder sb = new StringBuilder();
        for (String s : columnNames) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(columnMap.get(s));
        }
        return sb.toString();
    }

    public abstract void setSizeInStoredCharacters(long characters);

    public abstract void setVariableColumnNames(Collection<String> variableColumnNames);

    public abstract void setColumnValues(Map<String,Set<String>> columnValues);

    public abstract void setUser(String user_);

    public abstract void setColumnMarkingsMap(Map<String,Map<String,String>> columnMarkingsMap);

    public abstract void setColumnColumnVisibilityMap(Map<String,String> columnVisibilityMap);

    public abstract void setColumnTimestampMap(Map<String,Long> parseColumnTimestamps);

    public MarkingFunctions getMarkingFunctions() {
        return markingFunctions;
    }

    public void setMarkingFunctions(MarkingFunctions markingFunctions) {
        this.markingFunctions = markingFunctions;
    }
}
