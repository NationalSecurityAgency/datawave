package datawave.core.common.result;

import org.apache.log4j.Logger;

import datawave.webservice.common.connection.AccumuloClientConfiguration;

/**
 * The configuration for the connection pool clients of the form derived from properties as follows:
 *
 * dw.{pool}.client.{tableName}.consistency = IMMEDIATE|EVENTUAL dw.{pool}.client.{tableName}.{hintName} = {hintValue}
 *
 */
public class ConnectionPoolClientProperties {

    private static final Logger log = Logger.getLogger(ConnectionPoolClientProperties.class);
    protected AccumuloClientConfiguration config = new AccumuloClientConfiguration();

    public AccumuloClientConfiguration getConfiguration() {
        return config;
    }

    public void setConfiguration(AccumuloClientConfiguration config) {
        this.config = config;
    }
}
