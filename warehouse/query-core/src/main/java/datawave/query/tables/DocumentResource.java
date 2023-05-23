package datawave.query.tables;

import com.google.common.base.Preconditions;
import datawave.query.attributes.Document;
import datawave.query.config.DocumentQueryConfiguration;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.pool.PoolableObjectFactory;
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
public class DocumentResource extends Resource<SerializedDocumentIfc> {
    private static final Logger log = Logger.getLogger(DocumentResource.class);

    public DocumentResource(final AccumuloClient client) {
        super(client);
    }

    protected void init(DocumentQueryConfiguration config,  String tableName, final Set<Authorizations> auths, Collection<Range> currentRange) throws TableNotFoundException {
        // do nothing.
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

    public static final class DocumentResourceFactory implements PoolableObjectFactory<DocumentResource> {

        private final AccumuloClient client;

        DocumentResourceFactory(AccumuloClient client) {
            this.client = client;
        }

        @Override
        public void activateObject(DocumentResource object) {
            /* no-op */
        }

        @Override
        public void destroyObject(DocumentResource object) {
            if (log.isTraceEnabled())
                log.trace("Removing " + object.hashCode());
        }

        @Override
        public DocumentResource makeObject() {
            DocumentResource scannerResource = new DocumentResource(client);
            if (log.isTraceEnabled())
                log.trace("Returning " + scannerResource.hashCode());
            return scannerResource;
        }

        @Override
        public void passivateObject(DocumentResource object) throws Exception {
            destroyObject(object);
        }

        @Override
        public boolean validateObject(DocumentResource object) {
            return true;
        }
    }


}
