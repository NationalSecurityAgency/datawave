package datawave.query.tables;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import com.google.common.base.Preconditions;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.log4j.Logger;

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
public class AccumuloResource extends Resource<Entry<Key,Value>> {

    private static final Logger log = Logger.getLogger(AccumuloResource.class);
    public AccumuloResource(final AccumuloClient client) {
        super(client);
    }


    public AccumuloResource(final AccumuloResource other) {
        super(other.client);
    }
    
    public static final class ResourceFactory {
        
        /**
         * Initializes a resource after it was delegated.
         * 
         * @param clazz
         *            a class
         * @param baseResource
         *            a base resource
         * @param tableName
         *            the table name
         * @param auths
         *            set of auths
         * @param currentRange
         *            a current range
         * @return the set resource
         * @throws TableNotFoundException
         *             if the table was not found
         * @param <T>
         *            type of the class
         */
        public static <T> AccumuloResource initializeResource(Class<T> clazz, AccumuloResource baseResource, final String tableName,
                        final Set<Authorizations> auths, Range currentRange) throws TableNotFoundException {
            return initializeResource(clazz, baseResource, tableName, auths, Collections.singleton(currentRange));
        }
        
        public static <T> AccumuloResource initializeResource(Class<T> clazz, AccumuloResource baseResource, final String tableName,
                        final Set<Authorizations> auths, Collection<Range> currentRange) throws TableNotFoundException {
            
            AccumuloResource newResource = null;
            try {
                newResource = (AccumuloResource) clazz.getConstructor(AccumuloResource.class).newInstance(baseResource);
                newResource.init(tableName, auths, currentRange);
            } catch (IllegalArgumentException | NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException
                            | SecurityException e) {
                throw new RuntimeException(e);
            }
            
            return newResource;
        }
        
    }

    public static final class AccumuloResourceFactory implements PoolableObjectFactory<AccumuloResource> {

        private final AccumuloClient client;

        AccumuloResourceFactory(AccumuloClient client) {
            this.client = client;
        }

        @Override
        public void activateObject(AccumuloResource object) {
            /* no-op */
        }

        @Override
        public void destroyObject(AccumuloResource object) {
            if (log.isTraceEnabled())
                log.trace("Removing " + object.hashCode());
        }

        @Override
        public AccumuloResource makeObject() {
            AccumuloResource scannerResource = new AccumuloResource(client);
            if (log.isTraceEnabled())
                log.trace("Returning " + scannerResource.hashCode());
            return scannerResource;
        }

        @Override
        public void passivateObject(AccumuloResource object) throws Exception {
            destroyObject(object);
        }

        @Override
        public boolean validateObject(AccumuloResource object) {
            return true;
        }
    }
    
}
