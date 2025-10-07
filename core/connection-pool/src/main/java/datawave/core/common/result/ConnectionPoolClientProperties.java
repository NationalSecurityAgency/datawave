package datawave.core.common.result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.webservice.common.connection.AccumuloClientConfiguration;

/**
 * The configuration for the connection pool clients of the form derived from properties as follows:
 *
 * dw.{pool}.client.{tableName}.consistency = IMMEDIATE|EVENTUAL dw.{pool}.client.{tableName}.{hintName} = {hintValue}
 *
 */
public class ConnectionPoolClientProperties {

    private static final Logger log = LoggerFactory.getLogger(ConnectionPoolClientProperties.class);
    protected AccumuloClientConfiguration config = new AccumuloClientConfiguration();

    public AccumuloClientConfiguration getConfiguration() {
        return config;
    }

    public void setConfiguration(AccumuloClientConfiguration config) {
        this.config = config;
    }
}
