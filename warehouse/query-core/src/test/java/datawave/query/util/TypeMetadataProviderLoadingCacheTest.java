package datawave.query.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.query.QueryTestTableHelper;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:/TypeMetadataBridgeContext.xml", "classpath:/TypeMetadataProviderContext.xml", "classpath:/TypeMetadataWriterContext.xml",})
public class TypeMetadataProviderLoadingCacheTest {
    
    private static final Logger log = Logger.getLogger(TypeMetadataProviderLoadingCacheTest.class);
    
    @Inject
    private TypeMetadataProvider typeMetadataProvider;
    
    @Inject
    private TypeMetadataWriter typeMetadataWriter;
    
    // the injection test (ONLY!) will use this directory and it needs it in the beforeclass method
    // so that it will be substituted in the spring xml files at class loading time
    private static final String tempDirForThreadingTest = "/tmp/TempDirForThreadingTest";
    
    @BeforeClass
    public static void beforeClass() {
        // this will get property substituted into the TypeMetadataBridgeContext.xml file
        // for the injection test (when this unit test is first created)
        System.setProperty("type.metadata.dir", tempDirForThreadingTest);
    }
    
    @AfterClass
    public static void teardown() {
        // maybe delete the temp folder here
        File tempFolder = new File(tempDirForThreadingTest);
        if (tempFolder.exists()) {
            try {
                FileUtils.forceDelete(tempFolder);
            } catch (IOException ex) {
                log.error(ex);
            }
        }
    }
    
    /**
     * create a sample TypeMetadata map to be written to a (monitored) file
     * 
     * @return a Map of auth sets to typeMetadata
     */
    public Map<Set<String>,TypeMetadata> getTypeMetadataMap() {
        Map<Set<String>,TypeMetadata> typeMetadataMap = Maps.newHashMap();
        
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
        return typeMetadataMap;
    }
    
    /**
     * create another sample TypeMetadata map to write to the (monitored) file, and test that the singleton is updated when this data is written to the file and
     * the TypeMetadataProvider::fileChanged method is called, causing the LoadingCache to be refreshed.
     * 
     * @return
     */
    public Map<Set<String>,TypeMetadata> getChangedTypeMetadataMap() {
        Map<Set<String>,TypeMetadata> typeMetadataMap = Maps.newHashMap();
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("field2", "ingest2", "FooBarType");
        typeMetadataMap.put(Sets.newHashSet("AUTHA", "AUTHB"), typeMetadata);
        return typeMetadataMap;
    }
    
    @After
    public void after() {
        System.clearProperty("type.metadata.dir");
    }
    
    @Test
    public void testWithThreads() throws Exception {
        Map<Set<String>,TypeMetadata> typeMetadataMap = getTypeMetadataMap();
        typeMetadataWriter.writeTypeMetadataMap(typeMetadataMap, QueryTestTableHelper.MODEL_TABLE_NAME);
        
        try {
            Thread.sleep(5000);
        } // give the DefaultFileMonitor (poller) enough time (> 3 sec) to fire the event (TypeMetadataProvider::fileCreated)
        catch (InterruptedException ex) {} // ignore
        
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<Future<TypeMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            Future<TypeMetadata> result = executor.submit(() -> {
                Set<String> authSet = Sets.newHashSet("AUTHA", "AUTHB");
                try {
                    return typeMetadataProvider.getTypeMetadata(QueryTestTableHelper.MODEL_TABLE_NAME, authSet);
                } catch (Exception ex) {
                    log.warn("Could not get Typemetadata for " + QueryTestTableHelper.MODEL_TABLE_NAME + " and " + authSet);
                }
                return new TypeMetadata();
            });
            futures.add(result);
        }
        
        for (Future<TypeMetadata> future : futures) {
            log.debug("pass1 typeMetadata:" + future.get());
        }
        Set<String> resultStrings = Sets.newHashSet();
        for (Future<TypeMetadata> future : futures) {
            resultStrings.add(future.get().toString());
        }
        Assert.assertTrue(resultStrings.contains("field1:[ingest2:DateType;ingest1:LcType];field2:[ingest2:LcType;ingest1:IntegerType]"));
        futures.clear();
        
        // create a different TypeMetadataMap and write it to the file
        typeMetadataMap = getChangedTypeMetadataMap();
        typeMetadataWriter.writeTypeMetadataMap(typeMetadataMap, QueryTestTableHelper.MODEL_TABLE_NAME);
        try {
            Thread.sleep(5000);
        } // give the DefaultFileMonitor (poller) enough time (> 3 sec) to fire the event (TypeMetadataProvider::fileChanged)
        catch (InterruptedException ex) {} // ignore
        
        // let 15 workers read the typemetadata from the singleton. One will refresh the singleton,
        // some will read the previous data (while the refresh is happening) and others will see the new data
        for (int i = 0; i < 15; i++) {
            Future<TypeMetadata> result = executor.submit(() -> {
                Set<String> authSet = Sets.newHashSet("AUTHA", "AUTHB");
                try {
                    return typeMetadataProvider.getTypeMetadata(QueryTestTableHelper.MODEL_TABLE_NAME, authSet);
                } catch (Exception ex) {
                    log.warn("Could not get Typemetadata for " + QueryTestTableHelper.MODEL_TABLE_NAME + " and " + authSet);
                }
                return new TypeMetadata();
            });
            futures.add(result);
        }
        
        for (Future<TypeMetadata> future : futures) {
            log.debug("pass2 typeMetadata:" + future.get());
        }
        
        for (Future<TypeMetadata> future : futures) {
            resultStrings.add(future.get().toString());
        }
        // some threads should have read the old values, while others should have read the new values
        // test to see that we got at least one of each.
        Assert.assertTrue(resultStrings.contains("field1:[ingest2:DateType;ingest1:LcType];field2:[ingest2:LcType;ingest1:IntegerType]"));
        Assert.assertTrue(resultStrings.contains("field2:[ingest2:FooBarType]"));
    }
}
