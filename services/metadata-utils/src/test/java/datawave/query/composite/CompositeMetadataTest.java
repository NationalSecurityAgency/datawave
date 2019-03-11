package datawave.query.composite;

import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class CompositeMetadataTest {
    
    private CompositeMetadata compositeMetadata;
    
    private String[] ingestTypes = new String[] {"EARTH", "FIRE", "WIND", "WATER", "HEART"};
    private String[][] compositeFields = new String[][] {new String[] {"CAPTAIN_PLANET", "HES", "A", "HERO"},
            new String[] {"GONNA_TAKE", "POLLUTION", "DOWN", "TO", "ZERO"},
            new String[] {"CAPTAIN_POLLUTION", "RADIATION", "DEFORESTATION", "SMOG", "TOXICS", "HATE"}};
    
    @Before
    public void setup() {
        compositeMetadata = new CompositeMetadata();
        
        for (String ingestType : ingestTypes) {
            for (String[] compFields : compositeFields)
                compositeMetadata.setCompositeFieldMappingByType(ingestType, compFields[0],
                                Arrays.asList(Arrays.copyOfRange(compFields, 1, compFields.length)));
            
            compositeMetadata.addCompositeTransitionDateByType(ingestType, "CAPTAIN_POLLUTION", new Date(0));
        }
    }
    
    @Test
    public void filterCompositeMetadataTest() {
        Set<String> compositeFields = new HashSet<>();
        compositeFields.add("CAPTAIN_PLANET");
        compositeFields.add("GONNA_TAKE");
        
        Set<String> componentFields = new HashSet<>();
        componentFields.add("HERO");
        componentFields.add("ZERO");
        
        Set<String> ingestTypes = new HashSet<>();
        ingestTypes.add("EARTH");
        ingestTypes.add("WIND");
        ingestTypes.add("HEART");
        
        // filter on specified composite fields
        CompositeMetadata fieldFilteredCompMetadata = compositeMetadata.filter(componentFields);
        Assert.assertTrue(fieldFilteredCompMetadata.compositeFieldMapByType.keySet().containsAll(Arrays.asList(this.ingestTypes)));
        for (Multimap<String,String> compFieldMap : fieldFilteredCompMetadata.compositeFieldMapByType.values()) {
            Assert.assertTrue(compFieldMap.keySet().containsAll(compositeFields));
            Assert.assertFalse(compFieldMap.keySet().contains("CAPTAIN_POLLUTION"));
        }
        Assert.assertTrue(fieldFilteredCompMetadata.compositeTransitionDatesByType.isEmpty());
        
        // filter on ingest types and composite fields
        CompositeMetadata filteredCompMetadata = compositeMetadata.filter(ingestTypes, componentFields);
        Assert.assertTrue(filteredCompMetadata.compositeFieldMapByType.keySet().containsAll(ingestTypes));
        Assert.assertFalse(filteredCompMetadata.compositeFieldMapByType.keySet().containsAll(Arrays.asList(this.ingestTypes)));
        for (Multimap<String,String> compFieldMap : filteredCompMetadata.compositeFieldMapByType.values()) {
            Assert.assertTrue(compFieldMap.keySet().containsAll(compositeFields));
            Assert.assertFalse(compFieldMap.keySet().contains("CAPTAIN_POLLUTION"));
        }
        Assert.assertTrue(filteredCompMetadata.compositeTransitionDatesByType.isEmpty());
    }
    
    @Test
    public void readWriteCompositeMetadataTest() {
        byte[] compMetadataBytes = CompositeMetadata.toBytes(compositeMetadata);
        CompositeMetadata destCompMetadata = CompositeMetadata.fromBytes(compMetadataBytes);
        
        for (String ingestType : compositeMetadata.compositeFieldMapByType.keySet()) {
            Assert.assertEquals(compositeMetadata.compositeFieldMapByType.get(ingestType), destCompMetadata.compositeFieldMapByType.get(ingestType));
            Assert.assertEquals(compositeMetadata.compositeTransitionDatesByType.get(ingestType),
                            destCompMetadata.compositeTransitionDatesByType.get(ingestType));
        }
    }
    
}
