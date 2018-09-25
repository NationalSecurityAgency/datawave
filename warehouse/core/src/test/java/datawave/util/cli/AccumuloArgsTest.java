package datawave.util.cli;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class AccumuloArgsTest {
    
    @Test
    public void testNewBuilder() {
        // @formatter:off
        AccumuloArgs args = AccumuloArgs.newBuilder()
            .withDefaultTable("defaultTableName")
            .build();
        String[] argv = {
                "-u", "Bob", 
                "--password", "zekret", 
                "-i", "instance", 
                "-z", "localhost:2181", 
                "-t", "testTable"
                };
        JCommander.newBuilder()
            .addObject(args)
            .build()
            .parse(argv);
        // @formatter:on
        
        assertThat(args.user(), is("Bob"));
        assertThat(args.password(), is("zekret"));
        assertThat(args.instance(), is("instance"));
        assertThat(args.zookeepers(), is("localhost:2181"));
        assertThat(args.table(), is("testTable"));
    }
    
    @Test
    public void testNewBuilder_WithExtraOpts() {
        // @formatter:off
        AccumuloArgs args = AccumuloArgs.newBuilder()
            .withDefaultTable("defaultTableName")
            .build();
        TestArg other = new TestArg();
        String[] argv = {
                "--user", "Steve", 
                "--password", "zekret", 
                "--instance", "instance", 
                "--zookeepers", "localhost:2181", 
                "--table", "testTable",
                "--color", "magenta"
                };
        JCommander.newBuilder()
            .addObject(args)
            .addObject(other)
            .build()
            .parse(argv);
        // @formatter:on
        
        assertThat(args.user(), is("Steve"));
        assertThat(args.password(), is("zekret"));
        assertThat(args.instance(), is("instance"));
        assertThat(args.zookeepers(), is("localhost:2181"));
        assertThat(args.table(), is("testTable"));
        // make sure extra args are available
        assertThat(other.color, is("magenta"));
    }
    
    private static class TestArg {
        @Parameter(names = {"-c", "--color"})
        String color;
    }
    
}
