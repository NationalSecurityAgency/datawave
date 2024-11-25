package datawave.query.composite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Multimap;

public class CompositeMetadataTest {
    
    private CompositeMetadata compositeMetadata;
    
    private String[] ingestTypes = new String[] {"EARTH", "FIRE", "WIND", "WATER", "HEART"};
    private String[][] compositeFields = new String[][] {new String[] {"CAPTAIN_PLANET", "HES", "A", "HERO"},
            new String[] {"GONNA_TAKE", "POLLUTION", "DOWN", "TO", "ZERO"},
            new String[] {"CAPTAIN_POLLUTION", "RADIATION", "DEFORESTATION", "SMOG", "TOXICS", "HATE"}};
    
    @BeforeEach
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
        assertTrue(fieldFilteredCompMetadata.compositeFieldMapByType.keySet().containsAll(Arrays.asList(this.ingestTypes)));
        for (Multimap<String,String> compFieldMap : fieldFilteredCompMetadata.compositeFieldMapByType.values()) {
            assertTrue(compFieldMap.keySet().containsAll(compositeFields));
            assertFalse(compFieldMap.keySet().contains("CAPTAIN_POLLUTION"));
        }
        assertTrue(fieldFilteredCompMetadata.compositeTransitionDatesByType.isEmpty());
        
        // filter on ingest types and composite fields
        CompositeMetadata filteredCompMetadata = compositeMetadata.filter(ingestTypes, componentFields);
        assertTrue(filteredCompMetadata.compositeFieldMapByType.keySet().containsAll(ingestTypes));
        assertFalse(filteredCompMetadata.compositeFieldMapByType.keySet().containsAll(Arrays.asList(this.ingestTypes)));
        for (Multimap<String,String> compFieldMap : filteredCompMetadata.compositeFieldMapByType.values()) {
            assertTrue(compFieldMap.keySet().containsAll(compositeFields));
            assertFalse(compFieldMap.keySet().contains("CAPTAIN_POLLUTION"));
        }
        assertTrue(filteredCompMetadata.compositeTransitionDatesByType.isEmpty());
    }
    
    @Test
    public void readWriteCompositeMetadataTest() {
        byte[] compMetadataBytes = CompositeMetadata.toBytes(compositeMetadata);
        CompositeMetadata destCompMetadata = CompositeMetadata.fromBytes(compMetadataBytes);
        
        for (String ingestType : compositeMetadata.compositeFieldMapByType.keySet()) {
            assertEquals(compositeMetadata.compositeFieldMapByType.get(ingestType), destCompMetadata.compositeFieldMapByType.get(ingestType));
            assertEquals(compositeMetadata.compositeTransitionDatesByType.get(ingestType), destCompMetadata.compositeTransitionDatesByType.get(ingestType));
        }
    }
    
    @Test
    public void serializeManyThreads() {
        final ExecutorService executor = Executors.newFixedThreadPool(5);
        try {
            for (int i = 0; i < 100; i++) {
                executor.submit(() -> {
                    byte[] compMetadataBytes = CompositeMetadata.toBytes(compositeMetadata);
                    assertNotNull(compMetadataBytes);
                });
            }
        } finally {
            // added protection
            executor.shutdown();
            try {
                // should not need this, but will perform for safety sake
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore and shutdownNow
            }
            executor.shutdownNow();
        }
    }
    
}
