package datawave.webservice.query.configuration;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * 
 */
@Disabled
@ExtendWith(MockitoExtension.class)
public class QueryDataTest {
    @Mock
    QueryData copy;
    
    @Mock
    IteratorSetting setting;
    
    @Mock
    Range range;
    
    @Test
    public void testCopyConstructor() {
        // Set expectations
        when(this.copy.getQuery()).thenReturn("TEST");
        when(this.copy.getRanges()).thenReturn(Arrays.asList(this.range));
        when(this.copy.getSettings()).thenReturn(Arrays.asList(this.setting));
        
        // Run the test
        QueryData subject = new QueryData(this.copy);
        String result1 = subject.getQuery();
        Collection<Range> result2 = subject.getRanges();
        subject.addIterator(this.setting);
        Collection<IteratorSetting> result3 = subject.getSettings();
        String result4 = subject.toString();
        
        // Verify results
        assertNotNull(result1, "Query should not be null");
        assertNotNull(result2, "Ranges should not be null");
        assertTrue(!result2.isEmpty(), "Ranges should not be empty");
        assertNotNull(result3, "Settings should not be null");
        assertTrue(!result3.isEmpty(), "Settings should not be empty");
        assertEquals(2, result3.size(), "Settings should have a size of 2");
        assertNotNull("toString should not be null", result4);
    }
    
    @Test
    public void testCorrectReuse() {
        List<QueryData> queries = Lists.newArrayList();
        
        QueryData query1 = new QueryData();
        query1.setQuery("FOO == 'bar'");
        query1.setSettings(Lists.newArrayList(new IteratorSetting(20, "iter1", "iter1.class")));
        query1.setRanges(Collections.singleton(Range.prefix("1")));
        
        queries.add(query1);
        queries.add(new QueryData(query1, Collections.singleton(Range.prefix("2"))));
        queries.add(new QueryData(query1, Collections.singleton(Range.prefix("3"))));
        
        Integer count = 1;
        List<IteratorSetting> prevSettings = null;
        String prevQuery = null;
        for (QueryData qd : queries) {
            if (null == prevSettings) {
                prevSettings = qd.getSettings();
            } else {
                assertEquals(prevSettings, qd.getSettings());
            }
            
            if (null == prevQuery) {
                prevQuery = qd.getQuery();
            } else {
                assertEquals(prevQuery, qd.getQuery());
            }
            
            assertEquals(1, qd.getRanges().size());
            
            Range r = qd.getRanges().iterator().next();
            
            assertEquals(count.toString(), r.getStartKey().getRow().toString());
            
            count++;
        }
    }
    
    @Test
    public void testCorrectDownstreamReuse() {
        List<QueryData> queries = Lists.newArrayList();
        
        QueryData query1 = new QueryData();
        query1.setQuery("FOO == 'bar'");
        query1.setSettings(Lists.newArrayList(new IteratorSetting(20, "iter1", "iter1.class")));
        query1.setRanges(Collections.singleton(Range.prefix("1")));
        
        queries.add(query1);
        queries.add(new QueryData(query1, Collections.singleton(Range.prefix("2"))));
        queries.add(new QueryData(query1, Collections.singleton(Range.prefix("3"))));
        
        for (QueryData qd : queries) {
            qd.getSettings().add(new IteratorSetting(21, "iter2", "iter2.class"));
        }
        
        Integer count = 1;
        List<IteratorSetting> prevSettings = null;
        String prevQuery = null;
        for (QueryData qd : queries) {
            if (null == prevSettings) {
                prevSettings = qd.getSettings();
            } else {
                assertTrue(equals(prevSettings, qd.getSettings()));
            }
            
            if (null == prevQuery) {
                prevQuery = qd.getQuery();
            } else {
                assertEquals(prevQuery, qd.getQuery());
            }
            
            assertEquals(1, qd.getRanges().size());
            
            Range r = qd.getRanges().iterator().next();
            
            assertEquals(count.toString(), r.getStartKey().getRow().toString());
            
            count++;
        }
    }
    
    protected boolean equals(List<IteratorSetting> settings1, List<IteratorSetting> settings2) {
        if ((null == settings1 && null != settings2) || (null != settings1 && null == settings2)) {
            return false;
        }
        
        if (settings1.size() != settings2.size()) {
            return false;
        }
        
        for (int i = 0; i < settings1.size(); i++) {
            IteratorSetting s1 = settings1.get(i), s2 = settings2.get(i);
            if (!(s1.getIteratorClass().equals(s2.getIteratorClass()) && s1.getName().equals(s2.getName()) && s1.getPriority() == s2.getPriority() && s1
                            .getOptions().equals(s2.getOptions()))) {
                return false;
            }
        }
        
        return true;
    }
    
}
