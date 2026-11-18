package datawave.core.query.cachedresults;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.result.event.ResponseObjectFactory;

public class CacheableQueryRowImplTest {

    @Test
    public void testRowVisibilityIsCombinationOfColumnVisibilities() {
        CacheableQueryRowImpl row = new CacheableQueryRowImpl();
        row.addColumn("FIELD1", "value1", "A", 1L);
        row.addColumn("FIELD2", "value2", "B", 1L);

        assertEquals("A&B", row.getColumnVisibility());
        assertEquals("A", row.getColumnVisibility("FIELD1"));
        assertEquals("B", row.getColumnVisibility("FIELD2"));
    }

    @Test
    public void testVisibilitiesAreNormalized() {
        CacheableQueryRowImpl row = new CacheableQueryRowImpl();
        row.addColumn("FIELD1", "value1", "C&(B|A)", 1L);

        assertEquals("C&(A|B)", row.getColumnVisibility());
        assertEquals("C&(A|B)", row.getColumnVisibility("FIELD1"));
    }

    @Test
    public void testRepeatedColumnWithDifferentVisibilitiesIsCombined() {
        CacheableQueryRowImpl row = new CacheableQueryRowImpl();
        row.addColumn("FIELD1", "value1", "A", 1L);
        row.addColumn("FIELD1", "value2", "B", 2L);
        row.addColumn("FIELD1", "value3", "A", 3L);

        assertEquals("A&B", row.getColumnVisibility("FIELD1"));
        assertEquals("A&B", row.getColumnVisibility());
        assertEquals("value1,value2,value3", row.getColumnValues().get("FIELD1"));
        assertEquals(3L, row.getColumnTimestamp("FIELD1"));
    }

    @Test
    public void testNullVisibilityDoesNotClearExistingVisibility() {
        CacheableQueryRowImpl row = new CacheableQueryRowImpl();
        row.addColumn("FIELD1", "value1", "A", 1L);
        row.addColumn("FIELD1", "value2", null, 1L);
        row.addColumn("FIELD2", "value3", null, 1L);

        assertEquals("A", row.getColumnVisibility());
        assertEquals("A", row.getColumnVisibility("FIELD1"));
        assertNull(row.getColumnVisibility("FIELD2"));
    }

    @Test
    public void testNoVisibilities() {
        CacheableQueryRowImpl row = new CacheableQueryRowImpl();
        row.addColumn("FIELD1", "value1", null, 1L);

        assertNull(row.getColumnVisibility());
    }

    @Test
    public void testInvalidVisibilitySkipsColumn() {
        CacheableQueryRowImpl row = new CacheableQueryRowImpl();
        row.addColumn("FIELD1", "value1", "A", 1L);
        row.addColumn("FIELD2", "value2", "A&|B", 1L);

        assertEquals("A", row.getColumnVisibility());
        assertFalse(row.getColumnValues().containsKey("FIELD2"));
    }

    @Test
    public void testColumnVisibilityEncodingRoundTrip() throws Exception {
        CacheableQueryRowImpl row = new CacheableQueryRowImpl();
        row.addColumn("X", "x", "A", 1L);
        row.addColumn("Y", "y", "A", 1L);
        row.addColumn("Z", "z", "B&A", 1L);
        row.addColumn("W", "w", null, 1L);

        // column numbers are assigned after the fixed columns, as CachedResultsBean does
        List<String> labels = new ArrayList<>(CacheableQueryRow.getFixedColumnSet());
        Map<String,Integer> fieldMap = new HashMap<>();
        for (String column : List.of("X", "Y", "Z", "W")) {
            labels.add(column);
            fieldMap.put(column, labels.size());
        }

        String columnVisibilities = row.getColumnSecurityMarkingString(fieldMap);
        // the most common visibility is the default, the others list their column numbers
        assertEquals(3, columnVisibilities.split("\0\0").length);
        assertEquals("A\0" + "0", columnVisibilities.split("\0\0")[0]);

        Map<String,String> values = new HashMap<>();
        values.put("_markings_", row.getColumnVisibility());
        values.put("_column_markings_", columnVisibilities);
        values.put("_column_timestamps_", row.getColumnTimestampString(fieldMap));
        values.putAll(row.getColumnValues());

        CacheableQueryRow read = CacheableQueryRowReader.createRow(resultSet(labels, values), Collections.emptySet(), responseObjectFactory());

        assertEquals("A&B", read.getColumnVisibility());
        assertEquals("A", read.getColumnVisibility("X"));
        assertEquals("A", read.getColumnVisibility("Y"));
        assertEquals("A&B", read.getColumnVisibility("Z"));
        assertEquals("", read.getColumnVisibility("W"));
        assertEquals("z", read.getColumnValues().get("Z"));
    }

    private static ResultSet resultSet(List<String> labels, Map<String,String> values) throws Exception {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(labels.size());
        when(metadata.getColumnLabel(anyInt())).thenAnswer(invocation -> labels.get((int) invocation.getArgument(0) - 1));

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(resultSet.getString(anyInt())).thenAnswer(invocation -> values.get(labels.get((int) invocation.getArgument(0) - 1)));
        return resultSet;
    }

    private static ResponseObjectFactory responseObjectFactory() {
        ResponseObjectFactory factory = mock(ResponseObjectFactory.class);
        when(factory.getCacheableQueryRow()).thenAnswer(invocation -> new CacheableQueryRowImpl());
        return factory;
    }
}
