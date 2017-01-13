package nsa.datawave.query.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import nsa.datawave.common.test.integration.IntegrationTest;
import nsa.datawave.query.rewrite.QueryTestTableHelper;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:/TypeMetadataBridgeContext.xml", "classpath:/TypeMetadataProviderContext.xml", "classpath:/TypeMetadataWriterContext.xml",})
public class TypeMetadataProviderTest {
    
    private static final Logger log = Logger.getLogger(TypeMetadataProviderTest.class);
    
    // the test that uses the factory defers loading the spring context until during the test
    // it will use this temporary folder
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    private static List<TemporaryFolder> staticTempFolders = Lists.newArrayList();
    
    @Inject
    private TypeMetadataProvider typeMetadataProvider;
    
    @Inject
    private TypeMetadataWriter typeMetadataWriter;
    
    private Map<Set<String>,TypeMetadata> typeMetadataMap = Maps.newHashMap();
    
    // the injection test (ONLY!) will use this directory and it needs it in the beforeclass method
    // so that it will be substituted in the spring xml files at class loading time
    private static final String tempDirForInjectionTest = "/tmp/TempDirForInjectionTest";
    
    @BeforeClass
    public static void beforeClass() {
        // this will get property substituted into the TypeMetadataBridgeContext.xml file
        // for the injection test (when this unit test is first created)
        String val = tempDirForInjectionTest;
        System.setProperty("type.metadata.dir", val);
    }
    
    @AfterClass
    public static void teardown() {
        for (TemporaryFolder t : staticTempFolders) {
            if (t.getRoot().exists()) {
                try {
                    FileUtils.deleteDirectory(t.getRoot());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            log.debug("after class " + t.getRoot() + " exists? " + t.getRoot().exists());
        }
        // maybe delete the temp folder here
        File tempFolder = new File(tempDirForInjectionTest);
        if (tempFolder.exists()) {
            try {
                FileUtils.forceDelete(tempFolder);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
    
    @Before
    public void prepareTypeMetadataMap() {
        // This is for the factory test. It will use the tempFolder which will
        // be substituted into the TypeMetadataBridgeContext in time for its loading
        // by the Factory methods in TypeMetadataWriter and TypeMetadataProvider
        String val = tempFolder.getRoot().getAbsolutePath();
        System.setProperty("type.metadata.dir", val);
        log.info("using tempFolder " + tempFolder.getRoot());
        
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("field1", "ingest1", "LcType");
        typeMetadata.put("field1", "ingest2", "DateType");
        typeMetadataMap.put(Collections.singleton("AUTHA"), typeMetadata);
        
        typeMetadata = new TypeMetadata();
        typeMetadata.put("field2", "ingest1", "IntegerType");
        typeMetadata.put("field2", "ingest2", "LcType");
        typeMetadataMap.put(Collections.singleton("AUTHB"), typeMetadata);
        
        typeMetadata = new TypeMetadata();
        typeMetadata.put("field1", "ingest1", "LcType");
        typeMetadata.put("field1", "ingest2", "DateType");
        typeMetadata.put("field2", "ingest1", "IntegerType");
        typeMetadata.put("field2", "ingest2", "LcType");
        typeMetadataMap.put(Sets.newHashSet("AUTHA", "AUTHB"), typeMetadata);
    }
    
    @After
    public void after() {
        System.clearProperty("type.metadata.dir");
    }
    
    public void testThisThing(TypeMetadataProvider typeMetadataProvider, TypeMetadataWriter typeMetadataWriter) throws Exception {
        
        typeMetadataWriter.writeTypeMetadataMap(typeMetadataMap, QueryTestTableHelper.MODEL_TABLE_NAME);
        
        log.info("getting typeMetadata from " + this.typeMetadataProvider.getBridge().getUri() + this.typeMetadataProvider.getBridge().getDir() + "/"
                        + this.typeMetadataProvider.getBridge().getFileName());
        
        TypeMetadata typeMetadata = null;
        
        for (int i = 0; i < 10; i++) {
            try {
                typeMetadata = typeMetadataProvider.getTypeMetadata(QueryTestTableHelper.MODEL_TABLE_NAME, Sets.newHashSet("AUTHA", "AUTHB"));
                // stacking the deck for the assertion below
                if (typeMetadata.equals(this.typeMetadataMap.get(Sets.newHashSet("AUTHA", "AUTHB")))) {
                    break;
                } else {
                    log.warn("attempt[" + i + "] to access typeMetadata failed. Got " + typeMetadata + " from " + typeMetadataProvider);
                }
            } catch (Exception ex) {
                log.info("will ignore " + ex + " and try a few more times....");
            }
            try {
                Thread.currentThread().sleep(5000);
            } catch (InterruptedException iex) {
                // ignored
            }
            
        }
        Assert.assertEquals(typeMetadata, this.typeMetadataMap.get(Sets.newHashSet("AUTHA", "AUTHB")));
    }
    
    @Test
    public void showPowerSet() {
        
        Set<String> auths = Sets.newHashSet("AUTHA", "AUTHB", "BAR");
        Set<Set<String>> powerset = Sets.powerSet(auths);
        for (Set<String> s : powerset) {
            log.debug("powerset has " + s);
        }
        log.debug("got " + powerset);
        
        log.debug(Collections.singleton("AUTHA").hashCode());
        log.debug(Sets.newHashSet("AUTHA").hashCode());
        Set<Set<String>> stuff = Sets.newHashSet();
        stuff.add(Collections.singleton("AUTHA"));
        stuff.add(Sets.newHashSet("AUTHA"));
        log.debug("stuff:" + stuff);
    }
    
    @Test
    public void testWithInjectionOnly() throws Exception {
        testThisThing(this.typeMetadataProvider, this.typeMetadataWriter);
    }
    
    @Test
    public void testWithFactory() throws Exception {
        testThisThing(TypeMetadataProvider.Factory.createTypeMetadataProvider(), TypeMetadataWriter.Factory.createTypeMetadataWriter());
    }
}
