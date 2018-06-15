package datawave.query.testframework;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generic resolver for simple JEXL query expressions where the types are Integer (e.g. a == b).
 */
public class IntegerResolver implements IQueryResolver {
    @Override
    public Set<IRawData> isEqual(String field, String value, Collection<IRawData> entries) {
        final Set<IRawData> match = new HashSet<>();
        final Integer intVal = Double.valueOf(value).intValue();
        for (final IRawData data : entries) {
            final Integer dataVal = Integer.valueOf(data.getValue(field.toLowerCase()));
            if (intVal.equals(dataVal)) {
                match.add(data);
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> notEqual(String field, String value, Collection<IRawData> entries) {
        final Set<IRawData> match = new HashSet<>();
        final Integer intVal = Double.valueOf(value).intValue();
        for (final IRawData data : entries) {
            final Integer dataVal = Integer.valueOf(data.getValue(field.toLowerCase()));
            if (!intVal.equals(dataVal)) {
                match.add(data);
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> regex(String field, String value, Collection<IRawData> entries) {
        throw new AssertionError("invalid regex test(field:" + field + " value:" + value + ")");
    }
    
    @Override
    public Set<IRawData> negRegex(String field, String value, Collection<IRawData> entries) {
        throw new AssertionError("invalid regex test(field:" + field + " value:" + value + ")");
    }
    
    @Override
    public Set<IRawData> greater(String field, String value, Collection<IRawData> entries) {
        final Set<IRawData> match = new HashSet<>();
        final int intVal = Double.valueOf(value).intValue();
        for (final IRawData data : entries) {
            final int val = Integer.valueOf(data.getValue(field.toLowerCase())).intValue();
            if (val > intVal) {
                match.add(data);
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> greaterEqual(String field, String value, Collection<IRawData> entries) {
        final Set<IRawData> match = new HashSet<>();
        final int intVal = Double.valueOf(value).intValue();
        for (final IRawData data : entries) {
            final int val = Integer.valueOf(data.getValue(field.toLowerCase())).intValue();
            if (val >= intVal) {
                match.add(data);
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> less(String field, String value, Collection<IRawData> entries) {
        final Set<IRawData> match = new HashSet<>();
        final int intVal = Double.valueOf(value).intValue();
        for (final IRawData data : entries) {
            final int val = Integer.valueOf(data.getValue(field.toLowerCase())).intValue();
            if (val < intVal) {
                match.add(data);
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> lessEqual(String field, String value, Collection<IRawData> entries) {
        final Set<IRawData> match = new HashSet<>();
        final int intVal = Double.valueOf(value).intValue();
        for (final IRawData data : entries) {
            final int val = Integer.valueOf(data.getValue(field.toLowerCase())).intValue();
            if (val <= intVal) {
                match.add(data);
            }
        }
        return match;
    }
    
    public static class TestIntegerResolver {
        private static final Logger log = LoggerFactory.getLogger(QueryAction.class);
        
        // unit test for this class
        @Test
        public void test() {
            Set<IRawData> s = new HashSet<>();
            String[] a = {"4", "10"};
            
            TestData raw = new TestData(a);
            s.add(raw);
            String[] b = {"0", "5"};
            raw = new TestData(b);
            s.add(raw);
            
            IQueryResolver qr = new IntegerResolver();
            QueryAction qa = new QueryAction("a", "==", "4", Integer.class, true);
            log.info("query({})", qa);
            Set<IRawData> resp = qr.isEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(1, resp.size());
            qa = new QueryAction("a", "!=", "5", Integer.class, true);
            log.info("query({})", qa);
            resp = qr.notEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(2, resp.size());
            qa = new QueryAction("a", ">", "3", Integer.class, true);
            log.info("query({})", qa);
            resp = qr.greater(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(1, resp.size());
            qa = new QueryAction("a", ">=", "4", Integer.class, true);
            log.info("query({})", qa);
            resp = qr.greaterEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(1, resp.size());
            qa = new QueryAction("a", "<", "6", Integer.class, true);
            log.info("query({})", qa);
            resp = qr.less(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(2, resp.size());
            qa = new QueryAction("a", "<=", "5", Integer.class, true);
            log.info("query({})", qa);
            resp = qr.lessEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(2, resp.size());
        }
    }
    
    private static class TestData extends BaseRawData {
        private static final List<String> headers;
        private static final Map<String,BaseRawData.RawMetaData> rawData = new HashMap<>();
        static {
            String[] hdrVals = {"a", "b"};
            headers = Arrays.asList(hdrVals);
            rawData.put("a", new BaseRawData.RawMetaData("a", Integer.class, false));
            rawData.put("b", new BaseRawData.RawMetaData("b", Integer.class, true));
        }
        
        TestData(final String[] fields) {
            super(fields);
        }
        
        @Override
        protected List<String> getHeaders() {
            return headers;
        }
        
        @Override
        protected boolean containsField(String field) {
            return headers.contains(field);
        }
        
        @Override
        public boolean isMultiValueField(String field) {
            return rawData.get(field.toLowerCase()).multiValue;
        }
        
        @Override
        public Type getFieldType(String field) {
            return rawData.get(field.toLowerCase()).type;
        }
    }
}
