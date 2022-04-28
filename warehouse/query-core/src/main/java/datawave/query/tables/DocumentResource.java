package datawave.query.tables;

import com.google.common.base.Preconditions;
import datawave.query.attributes.Document;
import datawave.query.config.DocumentQueryConfiguration;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * Purpose: Basic Iterable resource. Contains the connector from which we will create the scanners.
 * 
 * Justification: This class should contain all resources that will be used to identify a given scanner session. The batchScanner can be returned as a
 * BatchScanner resource. ScannerResource is an immutable resource. While we could make them mutable, their purpose is to maintain history of scan sessions so
 * that can do runtime analysis, if desired.
 * 
 * Design: Is closeable for obvious reasons, is an iterable so that instead of exposing the specific underlying accumulo resource, we return an iterator. While
 * this isn't entirely necessary, it does allow us to better inject test code.
 * 
 */
public class DocumentResource implements Closeable, Iterable<SerializedDocumentIfc> {
    private static final Logger log = Logger.getLogger(DocumentResource.class);
    /**
     * Our connector.
     */
    private AccumuloClient client;
    
    public DocumentResource(final AccumuloClient client) {
        Preconditions.checkNotNull(client);
        
        this.client = client;
    }
    
    public DocumentResource(final DocumentResource other) {
        // deep copy
    }
    
    protected AccumuloClient getClient() {
        return client;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        // nothing to close.
    }
    
    protected void init(final DocumentQueryConfiguration config, final String tableName, final Set<Authorizations> auths, Collection<Range> currentRange) throws TableNotFoundException {
        // do nothing.
    }
    
    /**
     * Sets the option on this currently running resource.
     * 
     * @param options
     * @return
     */
    public DocumentResource setOptions(SessionOptions options) {
        
        return this;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<SerializedDocumentIfc> iterator() {
        return Collections.emptyIterator();
    }
    
    public static final class ResourceFactory {
        
        /**
         * Initializes a resource after it was delegated.
         * 
         * @param clazz
         * @param baseResource
         * @param tableName
         * @param auths
         * @param currentRange
         * @return
         * @throws TableNotFoundException
         */
        public static <T> DocumentResource initializeResource(Class<T> clazz, DocumentResource baseResource, DocumentQueryConfiguration config, final String tableName,
                                                              final Set<Authorizations> auths, Range currentRange) throws TableNotFoundException {
            return initializeResource(clazz, baseResource,config,  tableName, auths, Collections.singleton(currentRange));
        }
        
        public static <T> DocumentResource initializeResource(Class<T> clazz, DocumentResource baseResource, DocumentQueryConfiguration config, final String tableName,
                                                              final Set<Authorizations> auths, Collection<Range> currentRange) throws TableNotFoundException {
            
            DocumentResource newResource = null;
            try {
                newResource = (DocumentResource) clazz.getConstructor(DocumentResource.class).newInstance(baseResource);
                newResource.init(config, tableName, auths, currentRange);
            } catch (IllegalArgumentException | NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException
                            | SecurityException e) {
                throw new RuntimeException(e);
            }
            
            return newResource;
        }
        
    }
    
}
