/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datawave.query.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * 
  */
public class TypeMetadataTest {
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    private static final Logger log = Logger.getLogger(TypeMetadataTest.class);
    
    /**
     * Test of getDataType method, of class DatawaveShardedTableFieldIndexKeyParser.
     */
    @Test
    public void testRoundTrip() {
        if (log.isDebugEnabled()) {
            log.debug("testRoundTrip");
        }
        TypeMetadata typeMetadata = new TypeMetadata();
        
        typeMetadata.put("field1", "ingest1", "LcType");
        typeMetadata.put("field1", "ingest2", "DateType");
        
        typeMetadata.put("field2", "ingest1", "IntegerType");
        typeMetadata.put("field2", "ingest2", "LcType");
        
        String asString = typeMetadata.toString();
        
        TypeMetadata fromString = new TypeMetadata(asString);
        
        Assert.assertEquals(fromString, typeMetadata);
        Assert.assertEquals(fromString.getTypeMetadata("field1", "ingest1"), typeMetadata.getTypeMetadata("field1", "ingest1"));
        Assert.assertEquals(fromString.getTypeMetadata("field1", "ingest2"), typeMetadata.getTypeMetadata("field1", "ingest2"));
        Assert.assertEquals(fromString.getTypeMetadata("field2", "ingest1"), typeMetadata.getTypeMetadata("field2", "ingest1"));
        Assert.assertEquals(fromString.getTypeMetadata("field2", "ingest2"), typeMetadata.getTypeMetadata("field2", "ingest2"));
    }
    
    @Test
    public void testTypeMetadataFilter() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("field1", "ingest1", "LcType");
        typeMetadata.put("field1", "ingest2", "DateType");
        typeMetadata.put("field2", "ingest1", "IntegerType");
        typeMetadata.put("field2", "ingest2", "LcType");
        
        TypeMetadata typeMetadata1 = new TypeMetadata();
        typeMetadata1.put("field1", "ingest1", "LcType");
        typeMetadata1.put("field2", "ingest1", "IntegerType");
        
        TypeMetadata typeMetadata2 = new TypeMetadata();
        typeMetadata2.put("field1", "ingest2", "DateType");
        typeMetadata2.put("field2", "ingest2", "LcType");
        
        Assert.assertEquals(typeMetadata, typeMetadata.filter(Sets.newHashSet("ingest1", "ingest2")));
        Assert.assertEquals(typeMetadata1, typeMetadata.filter(Sets.newHashSet("ingest1")));
        Assert.assertEquals(typeMetadata2, typeMetadata.filter(Sets.newHashSet("ingest2")));
    }
    
    @Test
    public void testFileRoundTrip() throws Exception {
        Map<String,TypeMetadata> map = prepareTypeMetadataMap();
        final File tempDir = temporaryFolder.newFolder();
        final File tempFile = new File(tempDir, "type_metadata_roundtrip_test");
        FileOutputStream fos = new FileOutputStream(tempFile);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(map);
        oos.close();
        
        FileInputStream fis = new FileInputStream(tempFile);
        ObjectInputStream ois = new ObjectInputStream(fis);
        Object got = ois.readObject();
        Assert.assertEquals(got, map);
    }
    
    public Map<String,TypeMetadata> prepareTypeMetadataMap() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("field1", "ingest1", "LcType");
        typeMetadata.put("field1", "ingest2", "DateType");
        typeMetadata.put("field2", "ingest1", "IntegerType");
        typeMetadata.put("field2", "ingest2", "LcType");
        
        TypeMetadata typeMetadata1 = new TypeMetadata();
        typeMetadata1.put("field1", "ingest1", "LcType");
        typeMetadata1.put("field2", "ingest1", "IntegerType");
        
        TypeMetadata typeMetadata2 = new TypeMetadata();
        typeMetadata2.put("field1", "ingest2", "DateType");
        typeMetadata2.put("field2", "ingest2", "LcType");
        
        Map<String,TypeMetadata> map = Maps.newHashMap();
        map.put("A", typeMetadata);
        map.put("B", typeMetadata1);
        map.put("C", typeMetadata2);
        return map;
    }
    
}
