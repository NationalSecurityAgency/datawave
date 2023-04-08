package datawave.webservice.query.factory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.marking.SecurityMarking;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.QueryPersistence;
import datawave.query.iterator.QueriesTableAgeOffIterator;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.ScannerHelper;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactory.Priority;
import datawave.core.query.util.QueryUtil;
import datawave.webservice.query.Query;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.util.MapUtils;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJBContext;
import javax.ejb.EJBException;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.ws.rs.core.MultivaluedMap;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Object that creates and updates QueryImpl objects using a table structure:
 *
 * row = userDN cf = query name cq = query id vis = securitymarking ts = expiration date value = query object
 *
 * A table named 'Queries' is created with the structure above and an iterator is configured on it to remove persisted queries when they have expired.
 *
 */
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Stateless
@LocalBean
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class Persister {
    private static class QueryResultsTransform<Q extends Query> implements Function<Entry<Key,Value>,Q> {
        @SuppressWarnings("unchecked")
        @Override
        public Q apply(final Entry<Key,Value> entry) {
            try {
                
                return (Q) QueryUtil.deserialize(QueryUtil.getQueryImplClassName(entry.getKey()), entry.getKey().getColumnVisibility(), entry.getValue());
            } catch (InvalidProtocolBufferException | ClassNotFoundException ipbEx) {
                throw new EJBException("Error deserializing the Query", ipbEx);
            }
        }
    }
    
    private static final QueryResultsTransform<Query> resultsTransform = new QueryResultsTransform<>();
    private static final QueryResultsTransform<Query> implResultsTransform = new QueryResultsTransform<>();
    
    private Logger log = Logger.getLogger(Persister.class);
    
    private static final String TABLE_NAME = "Queries";
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    
    @Resource
    protected EJBContext ctx;
    
    @Inject
    private ResponseObjectFactory responseObjectFactory;
    
    public Query create(String userDN, List<String> dnList, SecurityMarking marking, String queryLogicName, QueryParameters qp,
                    MultivaluedMap<String,String> optionalQueryParameters) {
        Query q = responseObjectFactory.getQueryImpl();
        q.initialize(userDN, dnList, queryLogicName, qp, MapUtils.toMultiValueMap(optionalQueryParameters));
        q.setColumnVisibility(marking.toColumnVisibilityString());
        q.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
        Thread.currentThread().setUncaughtExceptionHandler(q.getUncaughtExceptionHandler());
        // Persist the query object if required
        if (qp.getPersistenceMode().equals(QueryPersistence.PERSISTENT)) {
            log.debug("Persisting query with id: " + q.getId());
            create(q);
        }
        return q;
    }
    
    private void tableCheck(AccumuloClient c) throws AccumuloException, AccumuloSecurityException, TableExistsException {
        if (!c.tableOperations().exists(TABLE_NAME)) {
            c.tableOperations().create(TABLE_NAME);
            try {
                IteratorSetting iteratorCfg = new IteratorSetting(19, "ageoff", QueriesTableAgeOffIterator.class);
                c.tableOperations().attachIterator(TABLE_NAME, iteratorCfg, EnumSet.allOf(IteratorScope.class));
            } catch (TableNotFoundException e) {
                throw new AccumuloException("We just created " + TABLE_NAME + " so this shouldn't have happened!", e);
            }
        }
    }
    
    /**
     * Persists a QueryImpl object
     *
     * @param query
     *
     */
    private void create(Query query) {
        AccumuloClient c = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            c = connectionFactory.getClient(null, null, Priority.ADMIN, trackingMap);
            tableCheck(c);
            try (BatchWriter writer = c.createBatchWriter(TABLE_NAME,
                            new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(10240L).setMaxWriteThreads(1))) {
                writer.addMutation(QueryUtil.toMutation(query, new ColumnVisibility(query.getColumnVisibility())));
            }
            
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            log.error("Error creating query", e);
            throw new EJBException("Error creating query", e);
        } finally {
            try {
                connectionFactory.returnClient(c);
            } catch (Exception e) {
                log.error("Error creating query", e);
                c = null;
            }
        }
    }
    
    /**
     * Removes existing query object with same id and inserts the updated object
     *
     * @param query
     */
    public void update(Query query) throws Exception {
        // TODO: decide the right thing to do here
        // Do we really need to remove first. Won't creating a record with the same key just overwrite with a new timestamp
        // The only time this wouldn't be the case is when the name and/or the visibility changes, which would cause a new row id
        remove(query);
        create(query);
    }
    
    /**
     * Removes the query object
     *
     * @param query
     */
    public void remove(Query query) throws Exception {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid = p.getName();
        Set<Authorizations> auths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            sid = dp.getShortName();
            for (Collection<String> cbAuths : dp.getAuthorizations())
                auths.add(new Authorizations(cbAuths.toArray(new String[cbAuths.size()])));
        }
        log.trace(sid + " has authorizations " + auths);
        
        AccumuloClient c = null;
        BatchDeleter deleter = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            c = connectionFactory.getClient(null, null, Priority.ADMIN, trackingMap);
            if (!c.tableOperations().exists(TABLE_NAME)) {
                return;
            }
            deleter = ScannerHelper.createBatchDeleter(c, TABLE_NAME, auths, 1, 10240L, 10000L, 1);
            Key skey = new Key(query.getOwner(), query.getQueryName(), query.getId().toString());
            Key ekey = new Key(query.getOwner(), query.getQueryName(), query.getId() + "\u0001");
            Range range = new Range(skey, ekey);
            log.info("Deleting query range: " + range);
            Collection<Range> ranges = Collections.singletonList(range);
            deleter.setRanges(ranges);
            deleter.delete();
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            log.error("Error deleting query", e);
            throw new EJBException("Error deleting query", e);
        } finally {
            if (null != deleter) {
                deleter.close();
            }
            try {
                connectionFactory.returnClient(c);
            } catch (Exception e) {
                log.error("Error deleting query", e);
                c = null;
            }
        }
    }
    
    /**
     *
     * Finds Query objects by the query id
     *
     * @param id
     * @return null if no results or list of query objects
     */
    @SuppressWarnings("unchecked")
    public List<Query> findById(String id) {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid = p.getName();
        Set<Authorizations> auths = new HashSet<>();
        
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            sid = dp.getShortName();
            for (Collection<String> cbAuths : dp.getAuthorizations())
                auths.add(new Authorizations(cbAuths.toArray(new String[cbAuths.size()])));
        }
        log.trace(sid + " has authorizations " + auths);
        
        AccumuloClient client = null;
        
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(null, null, Priority.ADMIN, trackingMap);
            tableCheck(client);
            
            IteratorSetting regex = new IteratorSetting(21, RegExFilter.class);
            regex.addOption(RegExFilter.COLQ_REGEX, id + "\0.*");
            
            try (Scanner scanner = ScannerHelper.createScanner(client, TABLE_NAME, auths)) {
                scanner.setRange(new Range(sid, sid));
                scanner.addScanIterator(regex);
                
                return Lists.newArrayList(Iterables.transform(scanner, resultsTransform));
            }
        } catch (Exception e) {
            log.error("Error creating query", e);
            throw new EJBException("Error creating query", e);
        } finally {
            try {
                connectionFactory.returnClient(client);
            } catch (Exception e) {
                log.error("Error creating query", e);
            }
        }
    }
    
    /**
     *
     * Finds Query objects by the query name
     *
     * @param name
     * @return null if no results or list of query objects
     */
    public List<Query> findByName(String name) {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String shortName = p.getName();
        Set<Authorizations> auths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            shortName = dp.getShortName();
            for (Collection<String> authCollection : dp.getAuthorizations())
                auths.add(new Authorizations(authCollection.toArray(new String[authCollection.size()])));
        }
        log.trace(shortName + " has authorizations " + auths);
        
        AccumuloClient c = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            c = connectionFactory.getClient(null, null, Priority.ADMIN, trackingMap);
            tableCheck(c);
            try (Scanner scanner = ScannerHelper.createScanner(c, TABLE_NAME, auths)) {
                Range range = new Range(shortName, shortName);
                scanner.setRange(range);
                scanner.fetchColumnFamily(new Text(name));
                List<Query> results = null;
                for (Entry<Key,Value> entry : scanner) {
                    if (null == results)
                        results = new ArrayList<>();
                    results.add(QueryUtil.deserialize(QueryUtil.getQueryImplClassName(entry.getKey()), entry.getKey().getColumnVisibility(), entry.getValue()));
                }
                
                return results;
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            log.error("Error creating query", e);
            throw new EJBException("Error creating query", e);
        } finally {
            try {
                connectionFactory.returnClient(c);
            } catch (Exception e) {
                log.error("Error creating query", e);
                c = null;
            }
        }
    }
    
    public List<Query> findByUser() {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid = p.getName();
        Set<Authorizations> auths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            sid = dp.getShortName();
            for (Collection<String> cbAuths : dp.getAuthorizations())
                auths.add(new Authorizations(cbAuths.toArray(new String[cbAuths.size()])));
        }
        log.trace(sid + " has authorizations " + auths);
        
        AccumuloClient c = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            c = connectionFactory.getClient(null, null, Priority.ADMIN, trackingMap);
            tableCheck(c);
            try (Scanner scanner = ScannerHelper.createScanner(c, TABLE_NAME, auths)) {
                Range range = new Range(sid, sid);
                scanner.setRange(range);
                List<Query> results = null;
                for (Entry<Key,Value> entry : scanner) {
                    if (null == results)
                        results = new ArrayList<>();
                    results.add(QueryUtil.deserialize(QueryUtil.getQueryImplClassName(entry.getKey()), entry.getKey().getColumnVisibility(), entry.getValue()));
                }
                return results;
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            log.error("Error creating query", e);
            throw new EJBException("Error creating query", e);
        } finally {
            try {
                connectionFactory.returnClient(c);
            } catch (Exception e) {
                log.error("Error creating query", e);
                c = null;
            }
        }
    }
    
    /**
     * Returns queries for the specified user with the credentials of the caller.
     *
     * @param user
     * @return list of specified users queries.
     */
    @RolesAllowed("Administrator")
    public List<Query> findByUser(String user) {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String sid = p.getName();
        Set<Authorizations> auths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            sid = dp.getShortName();
            for (Collection<String> cbAuths : dp.getAuthorizations())
                auths.add(new Authorizations(cbAuths.toArray(new String[cbAuths.size()])));
        }
        log.trace(sid + " has authorizations " + auths);
        
        AccumuloClient c = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            c = connectionFactory.getClient(null, null, Priority.ADMIN, trackingMap);
            tableCheck(c);
            try (Scanner scanner = ScannerHelper.createScanner(c, TABLE_NAME, auths)) {
                Range range = new Range(user, user);
                scanner.setRange(range);
                List<Query> results = null;
                for (Entry<Key,Value> entry : scanner) {
                    if (null == results)
                        results = new ArrayList<>();
                    results.add(QueryUtil.deserialize(QueryUtil.getQueryImplClassName(entry.getKey()), entry.getKey().getColumnVisibility(), entry.getValue()));
                }
                return results;
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            log.error("Error creating query", e);
            throw new EJBException("Error creating query", e);
        } finally {
            try {
                connectionFactory.returnClient(c);
            } catch (Exception e) {
                log.error("Error creating query", e);
                c = null;
            }
        }
    }
    
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    public List<Query> adminFindById(final String queryId) {
        AccumuloClient client = null;
        
        try {
            final Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(null, null, Priority.ADMIN, trackingMap);
            tableCheck(client);
            
            final IteratorSetting regex = new IteratorSetting(21, RegExFilter.class);
            regex.addOption(RegExFilter.COLQ_REGEX, queryId);
            
            final HashSet<Authorizations> auths = new HashSet<>();
            auths.add(client.securityOperations().getUserAuthorizations(client.whoami()));
            
            try (final Scanner scanner = ScannerHelper.createScanner(client, TABLE_NAME, auths)) {
                scanner.addScanIterator(regex);
                return Lists.newArrayList(Iterables.transform(scanner, implResultsTransform));
            }
        } catch (Exception ex) {
            log.error("Error finding query", ex);
            throw new EJBException("Error finding query", ex);
        } finally {
            try {
                if (client != null)
                    connectionFactory.returnClient(client);
            } catch (Exception ex) {
                log.error("Error creating query", ex);
            }
        }
    }
}
