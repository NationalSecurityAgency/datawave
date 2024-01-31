package datawave.util.cli;

import com.beust.jcommander.Parameter;

/**
 * Class for storing common args for use with interacting with Accumulo for use with jcommander command line parsing library. The arguments that are parsed
 * include:
 *
 * <pre>
 * -u --user
 * -p --password
 * -i --instance
 * -z --zookeepers
 * -t --table
 * </pre>
 *
 * All arguments are converted with the {@code PasswordConverter} so the env:ENVIRONMENT_VAR format can be used to specify parameters <br>
 * This is especially recommend to be used for password arguments <br>
 * Example usage:
 *
 * <pre>
 * AccumuloArgs args = AccumuloArgs.newBuilder()
 *     .withDefaultTable("myDefaultTable")
 *     .build();
 * // or
 * AccumuloArgs accumuloArgs = AccumuloArgs.defaults();
 * // for other info needed for task create separate argument storage object
 * DifferentExtraArgs otherArgs = new DifferentExtraArgs();
 *
 * JCommander.newBuilder()
 *   .addObject(accumuloArgs)
 *   .addObject(otherArgs)
 *   .build()
 *   .parse(argv);
 *
 * Instance instance = new ZooKeeperInstance(args.instance(), args.zookeepers());
 * Connector connector - instance.getConnector(args.user(), new PasswordToken(cli.password()))
 * Authorizations auths = connector.securityOperations.getUserAuthoriztions(connector.whoami());
 * BatchScanner scanner = connector.createBatchScanner(args.table(), auths, otherArgs.numQueryThreads());
 * </pre>
 */
public class AccumuloArgs {
    // @formatter:off
    @Parameter(names = {"-u", "--user"},
        description = "Accumulo user",
        converter = PasswordConverter.class,
        required = true)
    private String user;

    @Parameter(names = {"-p", "--password"},
        description = "Accumulo password",
        converter = PasswordConverter.class,
        required = true)
    private String password;

    @Parameter(names = {"-i", "--instance"},
        description = "Accumulo instance name",
        converter = PasswordConverter.class,
        required = true)
    private String instance;

    @Parameter(names = {"-z", "--zookeepers"},
        description = "Zookeepr connection URL",
        converter = PasswordConverter.class,
        required = true)
    private String zookeepers;

    @Parameter(names = {"-t", "--table"},
        description = "Optional arg to specify a specific table to interact with",
        converter = PasswordConverter.class,
        required = true)
    private String table;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help;
    // @formatter:on

    public String user() {
        return user;
    }

    public String password() {
        return password;
    }

    public String instance() {
        return instance;
    }

    public String zookeepers() {
        return zookeepers;
    }

    public String table() {
        return table;
    }

    /**
     * @return and AccumoloArgs container with no defaults changed
     */
    public static AccumuloArgs defaults() {
        return newBuilder().build();
    }

    /**
     * @return a new AccumuloArgs.Builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String defaultTable;

        public Builder withDefaultTable(String defaultTable) {
            this.defaultTable = defaultTable;
            return this;
        }

        public AccumuloArgs build() {
            AccumuloArgs args = new AccumuloArgs();
            args.table = defaultTable;
            return args;
        }
    }
}
