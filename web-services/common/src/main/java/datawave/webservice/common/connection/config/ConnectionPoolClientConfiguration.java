package datawave.webservice.common.connection.config;

import java.util.Map;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.commons.lang3.StringUtils;
import org.apache.deltaspike.core.api.config.ConfigResolver;
import org.apache.log4j.Logger;

import datawave.webservice.common.connection.AccumuloClientConfiguration;

/**
 * The configuration for the connection pool clients of the form derived from properties as follows:
 *
 * dw.{pool}.client.{tableName}.consistency = IMMEDIATE|EVENTUAL dw.{pool}.client.{tableName}.{hintName} = {hintValue}
 *
 */
public class ConnectionPoolClientConfiguration {

    private static final Logger log = Logger.getLogger(ConnectionPoolConfiguration.class);
    private AccumuloClientConfiguration config = new AccumuloClientConfiguration();

    public ConnectionPoolClientConfiguration(String poolName) {
        String prefix = "dw." + poolName + ".client";
        for (Map.Entry<String,String> property : ConfigResolver.getAllProperties().entrySet()) {
            if (property.getKey().startsWith(prefix)) {
                String[] tableAndHint = StringUtils.split(property.getKey().substring(prefix.length()), '.');
                if (tableAndHint.length == 2) {
                    if (tableAndHint[1].equals("consistency")) {
                        config.setConsistency(tableAndHint[0], ScannerBase.ConsistencyLevel.valueOf(property.getValue()));
                    } else {
                        config.addHint(tableAndHint[0], tableAndHint[1], property.getValue());
                    }
                } else {
                    log.error("Invalid client hint configuration property " + property.getKey());
                }
            }
        }
    }

    public AccumuloClientConfiguration getConfiguration() {
        return config;
    }
}
