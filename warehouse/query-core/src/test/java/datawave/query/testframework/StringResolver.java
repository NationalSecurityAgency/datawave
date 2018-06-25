package datawave.query.testframework;

import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generic resolver for simple JEXL query expressions where the types are String (e.g. a == b).
 */
public class StringResolver implements IQueryResolver {
    
    private final LcNoDiacriticsNormalizer normalizer = new LcNoDiacriticsNormalizer();
    
    @Override
    public Set<IRawData> isEqual(String field, String value, Collection<IRawData> entries) {
        Set<IRawData> match = new HashSet<>();
        final String norm = this.normalizer.normalize(value);
        for (final IRawData data : entries) {
            final String dataVal = data.getValue(field.toLowerCase());
            if (null != dataVal) {
                final String normVal = normalizer.normalize(dataVal);
                if (data.isMultiValueField(field)) {
                    for (final String multi : normVal.split(IRawDataManager.MULTIVALUE_SEP)) {
                        if (norm.equals(multi)) {
                            match.add(data);
                            break;
                        }
                    }
                } else {
                    if (norm.equals(normVal)) {
                        match.add(data);
                    }
                }
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> notEqual(String field, String value, Collection<IRawData> entries) {
        Set<IRawData> match = new HashSet<>();
        final String norm = this.normalizer.normalize(value);
        for (final IRawData data : entries) {
            final String dataVal = data.getValue(field.toLowerCase());
            if (null != dataVal) {
                final String normVal = normalizer.normalize(dataVal);
                if (data.isMultiValueField(field)) {
                    for (final String multi : normVal.split(IRawDataManager.MULTIVALUE_SEP)) {
                        if (!norm.equals(multi)) {
                            match.add(data);
                            break;
                        }
                    }
                } else {
                    if (!norm.equals(normVal)) {
                        match.add(data);
                    }
                }
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> regex(String field, String value, Collection<IRawData> entries) {
        Set<IRawData> match = new HashSet<>();
        final String norm = normalizer.normalizeRegex(value);
        final Pattern p = Pattern.compile(norm);
        for (final IRawData data : entries) {
            final String dataVal = data.getValue(field.toLowerCase());
            if (null != dataVal) {
                final String normVal = normalizer.normalize(dataVal);
                if (data.isMultiValueField(field)) {
                    for (final String multi : normVal.split(IRawDataManager.MULTIVALUE_SEP)) {
                        final Matcher m = p.matcher(multi);
                        if (m.matches()) {
                            match.add(data);
                            break;
                        }
                    }
                } else {
                    final Matcher m = p.matcher(normVal);
                    if (m.matches()) {
                        match.add(data);
                    }
                }
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> negRegex(String field, String value, Collection<IRawData> entries) {
        Set<IRawData> match = new HashSet<>();
        final String norm = normalizer.normalizeRegex(value);
        final Pattern p = Pattern.compile(norm);
        for (final IRawData data : entries) {
            final String dataVal = data.getValue(field.toLowerCase());
            if (null != dataVal) {
                final String normVal = normalizer.normalize(dataVal);
                if (data.isMultiValueField(field)) {
                    for (final String multi : normVal.split(IRawDataManager.MULTIVALUE_SEP)) {
                        final Matcher m = p.matcher(multi);
                        if (!m.matches()) {
                            match.add(data);
                            break;
                        }
                    }
                } else {
                    final Matcher m = p.matcher(normVal);
                    if (!m.matches()) {
                        match.add(data);
                    }
                }
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> greater(String field, String value, Collection<IRawData> entries) {
        Set<IRawData> match = new HashSet<>();
        final String norm = normalizer.normalize(value);
        for (final IRawData data : entries) {
            final String dataVal = data.getValue(field.toLowerCase());
            if (null != dataVal) {
                final String normVal = normalizer.normalize(dataVal);
                if (data.isMultiValueField(field)) {
                    for (final String multi : normVal.split(IRawDataManager.MULTIVALUE_SEP)) {
                        if (0 < multi.compareTo(norm)) {
                            match.add(data);
                            break;
                        }
                    }
                } else {
                    if (0 < normVal.compareTo(norm)) {
                        match.add(data);
                    }
                }
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> greaterEqual(String field, String value, Collection<IRawData> entries) {
        Set<IRawData> match = new HashSet<>();
        final String norm = normalizer.normalize(value);
        for (final IRawData data : entries) {
            final String dataVal = data.getValue(field.toLowerCase());
            if (null != dataVal) {
                final String normVal = normalizer.normalize(dataVal);
                if (data.isMultiValueField(field)) {
                    for (final String multi : normVal.split(IRawDataManager.MULTIVALUE_SEP)) {
                        if (0 <= multi.compareTo(norm)) {
                            match.add(data);
                            break;
                        }
                    }
                } else {
                    if (0 <= normVal.compareTo(norm)) {
                        match.add(data);
                    }
                }
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> less(String field, String value, Collection<IRawData> entries) {
        Set<IRawData> match = new HashSet<>();
        final String norm = normalizer.normalize(value);
        for (final IRawData data : entries) {
            final String dataVal = data.getValue(field.toLowerCase());
            if (null != dataVal) {
                final String normVal = normalizer.normalize(dataVal);
                if (data.isMultiValueField(field)) {
                    for (final String multi : normVal.split(IRawDataManager.MULTIVALUE_SEP)) {
                        if (0 > multi.compareTo(norm)) {
                            match.add(data);
                            break;
                        }
                    }
                } else {
                    if (0 > normVal.compareTo(norm)) {
                        match.add(data);
                    }
                }
            }
        }
        return match;
    }
    
    @Override
    public Set<IRawData> lessEqual(String field, String value, Collection<IRawData> entries) {
        Set<IRawData> match = new HashSet<>();
        final String norm = normalizer.normalize(value);
        for (final IRawData data : entries) {
            final String dataVal = data.getValue(field.toLowerCase());
            if (null != dataVal) {
                final String normVal = normalizer.normalize(dataVal);
                if (data.isMultiValueField(field)) {
                    for (final String multi : normVal.split(IRawDataManager.MULTIVALUE_SEP)) {
                        if (0 >= multi.compareTo(norm)) {
                            match.add(data);
                            break;
                        }
                    }
                } else {
                    if (0 >= normVal.compareTo(norm)) {
                        match.add(data);
                    }
                }
            }
        }
        return match;
    }
    
    public static class TestStringResolver {
        private static final Logger log = Logger.getLogger(QueryAction.class);
        
        // just for testing
        @Test
        public void test() {
            Set<IRawData> s = new HashSet<>();
            String[] a = {"a", "abc;abx;def;xyz;ccc"};
            TestData raw = new TestData(a);
            s.add(raw);
            String[] b = {"xyz", "cccc"};
            raw = new TestData(b);
            s.add(raw);
            
            StringResolver qr = new StringResolver();
            QueryAction qa = new QueryAction("a", "==", "'a'", String.class, true);
            log.info("query(" + qa + ")");
            Set<IRawData> resp = qr.isEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(1, resp.size());
            qa = new QueryAction("b", "==", "'xyz'", String.class, true);
            log.info("query(" + qa + ")");
            resp = qr.isEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(1, resp.size());
            qa = new QueryAction("a", "!=", "'b'", String.class, true);
            log.info("query(" + qa + ")");
            resp = qr.notEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(2, resp.size());
            qa = new QueryAction("b", "!=", "'abc'", String.class, true);
            log.info("query(" + qa + ")");
            resp = qr.notEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(2, resp.size());
            qa = new QueryAction("b", "<", "'abx'", String.class, true);
            log.info("query(" + qa + ")");
            // qa = new QueryAction("b < 'abx'");
            resp = qr.less(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(1, resp.size());
            qa = new QueryAction("a", "<=", "'A'", String.class, true);
            log.info("query(" + qa + ")");
            resp = qr.lessEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(1, resp.size());
            qa = new QueryAction("b", "<=", "'A'", String.class, true);
            log.info("query(" + qa + ")");
            resp = qr.lessEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(0, resp.size());
            qa = new QueryAction("A", ">", "'1'", String.class, true);
            log.info("query(" + qa + ")");
            resp = qr.greater(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(2, resp.size());
            qa = new QueryAction("b", ">", "'s'", String.class, true);
            log.info("query(" + qa + ")");
            resp = qr.greater(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(1, resp.size());
            qa = new QueryAction("a", ">=", "'a'", String.class, true);
            log.info("query(" + qa + ")");
            resp = qr.greaterEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(2, resp.size());
            qa = new QueryAction("b", ">=", "'c'", String.class, true);
            log.info("query(" + qa + ")");
            resp = qr.greaterEqual(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(2, resp.size());
            qa = new QueryAction("b", "=~", "'c.*c'", String.class, true);
            log.info("query(" + qa + ")");
            resp = qr.regex(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(2, resp.size());
            qa = new QueryAction("b", "!~", "'c.*c'", String.class, true);
            log.info("query(" + qa + ")");
            resp = qr.negRegex(qa.getKey(), qa.getValue(), s);
            Assert.assertEquals(1, resp.size());
        }
    }
    
    private static class TestData extends BaseRawData {
        private static final List<String> headers;
        private static final Map<String,BaseRawData.RawMetaData> rawData = new HashMap<>();
        static {
            String[] hdrVals = {"a", "b"};
            headers = Arrays.asList(hdrVals);
            rawData.put("a", new RawMetaData("a", String.class, false));
            rawData.put("b", new RawMetaData("b", String.class, true));
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
            return headers.contains(field.toLowerCase());
        }
        
        @Override
        public boolean isMultiValueField(String field) {
            return rawData.get(field.toLowerCase()).multiValue;
        }
        
        @Override
        public Type getFieldType(String field) {
            return rawData.get(field).type;
        }
    }
}
