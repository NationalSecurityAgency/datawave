package nsa.datawave.webservice.common.connection;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;

import java.lang.reflect.Proxy;

/**
 *
 */
public class ConnectionDelegates {
    
    public static WrappedConnector newWrappedConnector(Connector real, Connector mock) {
        return new WrappedConnector(real, mock);
    }
    
    public static BatchDeleter newBatchDeleter(BatchDeleter delegate) {
        return (BatchDeleter) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[] {BatchDeleter.class}, null);
    }
    
    public static BatchScanner newBatchScanner(BatchScanner delegate) {
        return (BatchScanner) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[] {BatchScanner.class}, null);
    }
    
    public static Scanner newBatchScanner(Scanner delegate) {
        return (Scanner) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[] {Scanner.class}, null);
    }
}
