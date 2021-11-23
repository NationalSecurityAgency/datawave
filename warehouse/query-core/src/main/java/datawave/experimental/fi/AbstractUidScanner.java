package datawave.experimental.fi;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

public abstract class AbstractUidScanner implements UidScanner {

    protected AccumuloClient client;
    protected Authorizations auths;
    protected String tableName;
    protected String scanId;

    protected AbstractUidScanner(AccumuloClient client, Authorizations auths, String tableName, String scanId) {
        this.client = client;
        this.auths = auths;
        this.tableName = tableName;
        this.scanId = scanId;
    }
}
