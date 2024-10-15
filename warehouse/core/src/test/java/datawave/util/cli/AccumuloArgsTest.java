package datawave.util.cli;

import static org.junit.Assert.assertEquals;

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

        assertEquals(args.user(), "Bob");
        assertEquals(args.password(), "zekret");
        assertEquals(args.instance(), "instance");
        assertEquals(args.zookeepers(), "localhost:2181");
        assertEquals(args.table(), "testTable");
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

        assertEquals(args.user(), "Steve");
        assertEquals(args.password(), "zekret");
        assertEquals(args.instance(), "instance");
        assertEquals(args.zookeepers(), "localhost:2181");
        assertEquals(args.table(), "testTable");
        // make sure extra args are available
        assertEquals(other.color, "magenta");
    }

    private static class TestArg {
        @Parameter(names = {"-c", "--color"})
        String color;
    }

}
