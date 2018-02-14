package datawave.ingest.data.config.ingest;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.GeometryType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.BaseNormalizedContent;
import datawave.ingest.data.config.DataTypeHelper.Properties;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.policy.IngestPolicyEnforcer;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;

public class BaseIngestHelperTest {
    
    private static final String FIELD_NAME = "TEST_FIELD";
    private static final String[] MAPPED_FIELDS = new String[] {"THIS_FIELD", "THAT_FIELD", "OTHER_FIELD"};
    private static final String DATA_TYPE = "someType";
    
    private Configuration conf = null;
    
    public static class TestIngestHelper extends BaseIngestHelper {
        
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer value) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            NormalizedContentInterface nci = new BaseNormalizedContent();
            nci.setFieldName(FIELD_NAME);
            nci.setEventFieldValue(new String(value.getRawData()));
            nci.setIndexedFieldValue(new String(value.getRawData()));
            eventFields.put(FIELD_NAME, nci);
            return normalizeMap(eventFields);
        }
    }
    
    @Before
    public void setup() {
        conf = new Configuration();
        conf.set(Properties.DATA_NAME, DATA_TYPE);
        conf.set(DATA_TYPE + Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set(DATA_TYPE + TypeRegistry.INGEST_HELPER, TestIngestHelper.class.getName());
        conf.set(DATA_TYPE + BaseIngestHelper.DEFAULT_TYPE, NoOpType.class.getName());
        conf.set(DATA_TYPE + "." + FIELD_NAME + BaseIngestHelper.FIELD_TYPE, GeometryType.class.getName());
        conf.set(DATA_TYPE + "." + FIELD_NAME + BaseIngestHelper.FIELD_INDEX_FILTER_MAPPING, StringUtils.arrayToCommaDelimitedString(MAPPED_FIELDS));
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
    }
    
    @Test
    public void testConfiguration() {
        TestIngestHelper helper = new TestIngestHelper();
        helper.setup(conf);
        Collection<Type<?>> dataTypes = helper.getDataTypes(FIELD_NAME);
        Assert.assertEquals(1, dataTypes.size());
        Type<?> n = helper.getDataTypes(FIELD_NAME).iterator().next();
        Assert.assertEquals(GeometryType.class, n.getClass());
        n = helper.getDataTypes("foo").iterator().next();
        Assert.assertEquals(NoOpType.class, n.getClass());
    }
    
    @Test
    public void testFieldIndexFilterMapping() {
        TestIngestHelper helper = new TestIngestHelper();
        helper.setup(conf);
        Assert.assertTrue(helper.isFieldIndexFilterField(FIELD_NAME));
        Collection<String> fieldIndexFilterMapping = helper.getFieldIndexFilterMapping(FIELD_NAME);
        Assert.assertTrue(fieldIndexFilterMapping.containsAll(Arrays.asList(MAPPED_FIELDS)));
    }
}
