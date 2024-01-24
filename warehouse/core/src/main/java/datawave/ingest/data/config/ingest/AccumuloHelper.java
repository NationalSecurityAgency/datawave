package datawave.ingest.data.config.ingest;

import java.util.Base64;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.config.ConfigurationHelper;

/**
 * Helper class to validate configuration of Accumulo required parameters
 *
 *
 *
 */
public class AccumuloHelper {

    public static final String USERNAME = "accumulo.username";
    public static final String PASSWORD = "accumulo.password";
    public static final String INSTANCE_NAME = "accumulo.instance.name";
    public static final String ZOOKEEPERS = "accumulo.zookeepers";

    private String username = null;
    private PasswordToken password;
    private String instanceName = null;
    private String zooKeepers = null;

    public void setup(Configuration config) throws IllegalArgumentException {
        username = ConfigurationHelper.isNull(config, USERNAME, String.class);
        byte[] pw = Base64.getDecoder().decode(ConfigurationHelper.isNull(config, PASSWORD, String.class).getBytes());
        password = new PasswordToken(pw);
        instanceName = ConfigurationHelper.isNull(config, INSTANCE_NAME, String.class);
        zooKeepers = ConfigurationHelper.isNull(config, ZOOKEEPERS, String.class);
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getZooKeepers() {
        return zooKeepers;
    }

    public String getUsername() {
        return username;
    }

    public byte[] getPassword() {
        return password.getPassword();
    }

    /**
     * @return an {@link AccumuloClient} to Accumulo given this object's settings.
     */
    public AccumuloClient newClient() {
        return Accumulo.newClient().to(instanceName, zooKeepers).as(username, password).build();
    }

    public Properties newClientProperties() {
        return Accumulo.newClientProperties().to(instanceName, zooKeepers).as(username, password).build();
    }

    public static void setUsername(Configuration conf, String username) {
        conf.set(USERNAME, username);
    }

    public static void setPassword(Configuration conf, byte[] password) {
        conf.set(PASSWORD, new String(Base64.getEncoder().encode(password)));
    }

    public static void setInstanceName(Configuration conf, String instanceName) {
        conf.set(INSTANCE_NAME, instanceName);
    }

    public static void setZooKeepers(Configuration conf, String zooKeepers) {
        conf.set(ZOOKEEPERS, zooKeepers);
    }

    public static String getUsername(Configuration conf) {
        return conf.get(USERNAME);
    }

    public static byte[] getPassword(Configuration conf) {
        return Base64.getDecoder().decode(ConfigurationHelper.isNull(conf, PASSWORD, String.class).getBytes());
    }

    public static String getInstanceName(Configuration conf) {
        return conf.get(INSTANCE_NAME);
    }

    public static String getZooKeepers(Configuration conf) {
        return conf.get(ZOOKEEPERS);
    }
}
