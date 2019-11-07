package datawave.configuration;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DataTypeConfigCompareTest {
    
    private static final String TYPE1 = "/type1-config.xml";
    private static final String TYPE2 = "/type2-config.xml";
    
    private DataTypeConfigCompare compare = new DataTypeConfigCompare();
    private Configuration type1Conf;
    private Configuration type2Conf;
    
    @Before
    public void setup() {
        type1Conf = new Configuration(false);
        type1Conf.addResource(this.getClass().getResourceAsStream(TYPE1));
        
        type2Conf = new Configuration(false);
        type2Conf.addResource(this.getClass().getResourceAsStream(TYPE2));
    }
    
    @Test
    public void shouldDetectSameSettings() {
        CompareResult result = compare.run(type1Conf, type2Conf);
        assertTrue(result.getSame().contains("file.input.format"));
        assertTrue(result.getSame().contains("handler.classes"));
    }
    
    @Test
    public void shouldDetectDifferentSettings() {
        CompareResult result = compare.run(type1Conf, type2Conf);
        assertTrue(result.getDiff().contains("ingest.helper.class"));
    }
    
    @Test
    public void shouldDetectLeftOnlySettings() {
        CompareResult result = compare.run(type1Conf, type2Conf);
        assertTrue(result.getLeftOnly().contains("only.type1.value"));
    }
    
    @Test
    public void shouldDetectRightOnlySettings() {
        CompareResult result = compare.run(type1Conf, type2Conf);
        assertTrue(result.getRightOnly().contains("only.type2.value"));
    }
}
