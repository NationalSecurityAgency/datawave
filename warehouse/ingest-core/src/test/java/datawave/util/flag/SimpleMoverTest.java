/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datawave.util.flag;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import datawave.util.flag.InputFile.TrackedDir;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 *
 */
public class SimpleMoverTest extends AbstractFlagConfig {
    
    private static Cache<Path,Path> directoryCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES).concurrencyLevel(10)
                    .build();
    
    @Before
    public void before() throws Exception {
        this.fmc = getDefaultFMC();
        this.fmc.setBaseHDFSDir(this.fmc.getBaseHDFSDir().replace("target", "target/SimpleMoverTest"));
        
        // cleanup flagging and flagged directories
        cleanTestDirs();
    }
    
    /**
     * Test of call method, of class SimpleMover.
     */
    @Test
    public void testCall() throws Exception {
        
        FileSystem fs = FileSystem.getLocal(new Configuration());
        
        Path file = getTestFile(fs);
        InputFile inFile = new InputFile("foo", file, 0, 0, 0, fmc.getBaseHDFSDir());
        
        SimpleMover instance = new SimpleMover(directoryCache, inFile, TrackedDir.FLAGGED_DIR, fs);
        InputFile result = instance.call();
        assertTrue("Should have moved to flagged", result.isMoved());
        assertTrue(result.getCurrentDir().equals(result.getFlagged()));
    }
    
    /**
     * Test of checkParent method, of class SimpleMover.
     */
    @Test
    public void testFailCall() throws Exception {
        
        FileSystem fs = FileSystem.getLocal(new Configuration());
        
        Path file = getTestFile(fs);
        InputFile entry = new InputFile("foo", file, 0, 0, 0, fmc.getBaseHDFSDir());
        
        // copy to the move will fail
        fs.mkdirs(entry.getFlagging().getParent());
        fs.copyFromLocalFile(file, entry.getFlagging());
        
        SimpleMover instance = new SimpleMover(directoryCache, entry, TrackedDir.FLAGGING_DIR, fs);
        
        InputFile result = instance.call();
        assertFalse("should not have moved due to collision", result.isMoved());
        assertTrue(result.getCurrentDir().equals(result.getPath()));
    }
    
}
