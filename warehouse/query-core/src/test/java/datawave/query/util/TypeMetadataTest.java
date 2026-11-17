/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 *
  */
public class TypeMetadataTest {

    @TempDir
    public File temporaryFolder;

    private static final Logger log = LoggerFactory.getLogger(TypeMetadataTest.class);

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

        assertEquals(fromString, typeMetadata);
        assertEquals(fromString.getTypeMetadata("field1", "ingest1"), typeMetadata.getTypeMetadata("field1", "ingest1"));
        assertEquals(fromString.getTypeMetadata("field1", "ingest2"), typeMetadata.getTypeMetadata("field1", "ingest2"));
        assertEquals(fromString.getTypeMetadata("field2", "ingest1"), typeMetadata.getTypeMetadata("field2", "ingest1"));
        assertEquals(fromString.getTypeMetadata("field2", "ingest2"), typeMetadata.getTypeMetadata("field2", "ingest2"));
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

        assertEquals(typeMetadata, typeMetadata.filter(Sets.newHashSet("ingest1", "ingest2")));
        assertEquals(typeMetadata1, typeMetadata.filter(Sets.newHashSet("ingest1")));
        assertEquals(typeMetadata2, typeMetadata.filter(Sets.newHashSet("ingest2")));
    }

    @Test
    public void testFileRoundTrip() throws Exception {
        Map<String,TypeMetadata> map = prepareTypeMetadataMap();
        final File tempDir = Files.createTempDirectory(temporaryFolder.toPath(), "util-").toFile();
        final File tempFile = new File(tempDir, "type_metadata_roundtrip_test");
        FileOutputStream fos = new FileOutputStream(tempFile);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(map);
        oos.close();

        FileInputStream fis = new FileInputStream(tempFile);
        ObjectInputStream ois = new ObjectInputStream(fis);
        Object got = ois.readObject();
        assertEquals(got, map);
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
