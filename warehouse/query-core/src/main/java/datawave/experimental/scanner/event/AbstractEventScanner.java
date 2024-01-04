package datawave.experimental.scanner.event;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import datawave.query.attributes.AttributeFactory;

public abstract class AbstractEventScanner implements EventScanner {

    protected AccumuloClient client;
    protected Authorizations auths;
    protected String tableName;
    protected String scanId;

    protected final AttributeFactory attributeFactory;

    protected AbstractEventScanner(String tableName, Authorizations auths, AccumuloClient client, AttributeFactory attributeFactory) {
        this.tableName = tableName;
        this.auths = auths;
        this.client = client;
        this.attributeFactory = attributeFactory;
    }
}
