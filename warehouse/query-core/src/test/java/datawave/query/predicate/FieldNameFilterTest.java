package datawave.query.predicate;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.KeyToFieldName;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * An integration test for {@link KeyToFieldName}, {@link ValueToAttribute}, and {@link KeyProjection}
 */
public class FieldNameFilterTest {
    
    private final Value value = new Value();
    private final ColumnVisibility cv1 = new ColumnVisibility("PUBLIC");
    private final ColumnVisibility cv2 = new ColumnVisibility("PRIVATE");
    
    private final KeyToFieldName fieldNameFunction = new KeyToFieldName();
    private final ValueToAttribute fieldValueFunction = new ValueToAttribute(new TypeMetadata(), null);
    
    private final List<Entry<Key,Value>> data = new ArrayList<>();
    private final List<Entry<Key,Value>> expected = new ArrayList<>();
    
    @Before
    public void before() {
        data.clear();
        expected.clear();
    }
    
    @Test
    public void testIncludesFilterWithOneField() {
        data.add(createEntry("F1", "v1", cv1));
        data.add(createEntry("F1", "v2", cv2));
        data.add(createEntry("F2", "v22", cv1));
        data.add(createEntry("F3", "v33", cv2));
        
        expected.add(createEntry("F1", "v1", cv1));
        expected.add(createEntry("F1", "v2", cv2));
        
        KeyProjection filter = new KeyProjection(Collections.singleton("F1"), Projection.ProjectionType.INCLUDES);
        executeTest(filter);
    }
    
    @Test
    public void testIncludesFilterWithTwoFields() {
        data.add(createEntry("F1", "v1", cv1));
        data.add(createEntry("F1", "v2", cv2));
        data.add(createEntry("F2", "v22", cv1));
        data.add(createEntry("F3", "v33", cv2));
        
        expected.add(createEntry("F1", "v1", cv1));
        expected.add(createEntry("F1", "v2", cv2));
        expected.add(createEntry("F2", "v22", cv1));
        
        KeyProjection filter = new KeyProjection(Sets.newHashSet("F1", "F2"), Projection.ProjectionType.INCLUDES);
        executeTest(filter);
        ;
    }
    
    @Test
    public void testExcludesWithOneField() {
        data.add(createEntry("F1", "v1", cv1));
        data.add(createEntry("F1", "v2", cv2));
        data.add(createEntry("F2", "v22", cv1));
        data.add(createEntry("F3", "v33", cv2));
        
        expected.add(createEntry("F2", "v22", cv1));
        expected.add(createEntry("F3", "v33", cv2));
        
        KeyProjection filter = new KeyProjection(Collections.singleton("F1"), Projection.ProjectionType.EXCLUDES);
        executeTest(filter);
    }
    
    @Test
    public void testExcludesWithTwoFields() {
        data.add(createEntry("F1", "v1", cv1));
        data.add(createEntry("F1", "v2", cv2));
        data.add(createEntry("F2", "v22", cv1));
        data.add(createEntry("F3", "v33", cv2));
        
        expected.add(createEntry("F3", "v33", cv2));
        KeyProjection filter = new KeyProjection(Sets.newHashSet("F1", "F2"), Projection.ProjectionType.EXCLUDES);
        executeTest(filter);
    }
    
    private void executeTest(KeyProjection filter) {
        Iterator<Entry<Key,Value>> expectedIter = expected.iterator();
        Iterator<Entry<Key,Entry<String,Attribute<? extends Comparable<?>>>>> iter = transformAndFilter(data, filter);
        while (iter.hasNext()) {
            Entry<Key,Entry<String,Attribute<? extends Comparable<?>>>> next = iter.next();
            
            assertTrue(expectedIter.hasNext());
            Entry<Key,Value> entry = expectedIter.next();
            assertEquals(entry.getKey(), next.getKey());
            
            Type<?> type = ((TypeAttribute<?>) next.getValue().getValue()).getType();
            assertEquals(valueFromKey(entry.getKey()), type.getNormalizedValue());
        }
        assertFalse(expectedIter.hasNext());
    }
    
    private Entry<Key,Value> createEntry(String f, String v, ColumnVisibility cv) {
        Key key = new Key("row", "datatype\0uid", f + '\u0000' + v, cv, -1);
        return Maps.immutableEntry(key, value);
    }
    
    private String valueFromKey(Key k) {
        String cq = k.getColumnQualifier().toString();
        int index = cq.indexOf('\u0000');
        return cq.substring(index + 1);
    }
    
    private Iterator<Entry<Key,Entry<String,Attribute<? extends Comparable<?>>>>> transformAndFilter(List<Entry<Key,Value>> list, KeyProjection filter) {
        return Iterators.transform(Iterators.filter(Iterators.transform(list.iterator(), fieldNameFunction), filter), fieldValueFunction);
    }
}
