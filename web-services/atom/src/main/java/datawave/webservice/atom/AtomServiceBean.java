package datawave.webservice.atom;

import datawave.annotation.Required;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.ScannerHelper;
import datawave.webservice.accumulo.iterator.MatchingKeySkippingIterator;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.NoResultsException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import org.apache.abdera.Abdera;
import org.apache.abdera.model.Categories;
import org.apache.abdera.model.Entry;
import org.apache.abdera.model.Feed;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;
import org.jboss.resteasy.util.Base64;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJB;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Path("/Atom")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class AtomServiceBean {
    
    private static final Abdera abdera = Abdera.getInstance();
    private static final String COLLECTION_LINK_FORMAT = "https://{0}:{1}/DataWave/Atom/{2}";
    // see configuration file for reference: datawave/atom/AtomConfiguration.xml
    
    private final Logger log = Logger.getLogger(this.getClass());
    
    @EJB
    private AccumuloConnectionFactory connectionFactory;
    
    @Resource
    private EJBContext ctx;
    
    @Inject
    @ConfigProperty(name = "cluster.name")
    private String clustername;
    
    @Inject
    @ConfigProperty(name = "dw.atom.tableName")
    private String tableName;
    
    @Inject
    @ConfigProperty(name = "dw.atom.externalHostName")
    private String host;
    
    @Inject
    @ConfigProperty(name = "dw.atom.externalPort")
    private String port;
    
    @Inject
    @ConfigProperty(name = "dw.atom.connectionPoolName")
    private String poolName;
    
    @PostConstruct
    public void setup() {}
    
    /**
     *
     * @return Atom Categories document that lists category names
     */
    @GET
    @GZIP
    @Produces("application/atomcat+xml")
    @Path("/categories")
    public Categories getCategories() {
        Principal p = ctx.getCallerPrincipal();
        Set<Authorizations> auths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            for (Collection<String> cbAuths : dp.getAuthorizations())
                auths.add(new Authorizations(cbAuths.toArray(new String[cbAuths.size()])));
        }
        Categories result;
        AccumuloClient client = null;
        try {
            result = abdera.newCategories();
            
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(poolName, Priority.NORMAL, trackingMap);
            try (Scanner scanner = ScannerHelper.createScanner(client, tableName + "Categories", auths)) {
                Map<String,String> props = new HashMap<>();
                props.put(MatchingKeySkippingIterator.ROW_DELIMITER_OPTION, "\0");
                props.put(MatchingKeySkippingIterator.NUM_SCANS_STRING_NAME, "5");
                IteratorSetting setting = new IteratorSetting(30, MatchingKeySkippingIterator.class, props);
                scanner.addScanIterator(setting);
                for (Map.Entry<Key,Value> entry : scanner) {
                    String collectionName = entry.getKey().getRow().toString();
                    result.addCategory(collectionName);
                }
                
            }
            if (result.getCategories().isEmpty())
                throw new NoResultsException(null);
            else
                return result;
            
        } catch (WebApplicationException web) {
            throw web;
        } catch (Exception e) {
            VoidResponse response = new VoidResponse();
            QueryException qe = new QueryException(DatawaveErrorCode.COLLECTION_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response);
        } finally {
            if (null != client) {
                try {
                    connectionFactory.returnClient(client);
                } catch (Exception e) {
                    log.error("Error returning connection to factory", e);
                }
            }
        }
    }
    
    /**
     *
     * @param category
     *            collection name
     * @param lastKey
     *            last key returned, page will begin with the next key
     * @param pagesize
     *            size of the page
     * @return Atom Feed document for a collection
     */
    @GET
    @GZIP
    @Produces("application/atom+xml")
    @Path("/{category}")
    public Feed getFeed(@Required("category") @PathParam("category") String category, @QueryParam("l") String lastKey,
                    @QueryParam("pagesize") @DefaultValue("30") int pagesize) {
        
        // Feed must contain
        // one atom:author
        // exactly one atom:id
        // SHOULD contain one atom:link element with a rel
        // attribute value of "self". This is the preferred URI for
        // retrieving Atom Feed Documents representing this Atom feed.
        // contain exactly one atom:title element.
        // contain exactly one atom:updated element.
        
        Principal p = ctx.getCallerPrincipal();
        Set<Authorizations> auths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            for (Collection<String> cbAuths : dp.getAuthorizations())
                auths.add(new Authorizations(cbAuths.toArray(new String[cbAuths.size()])));
        }
        
        Feed result;
        AccumuloClient client = null;
        Date maxDate = new Date(0);
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(poolName, Priority.NORMAL, trackingMap);
            
            result = abdera.newFeed();
            result.addAuthor(clustername);
            result.setTitle(category);
            
            Key nextLastKey = null;
            int count = 0;
            
            try (Scanner scanner = ScannerHelper.createScanner(client, tableName, auths)) {
                if (null != lastKey) {
                    Key lastSeenKey = deserializeKey(lastKey);
                    scanner.setRange(new Range(lastSeenKey, false, new Key(category + "\1"), false));
                } else {
                    scanner.setRange(new Range(category, true, category + "\1", false));
                }
                for (Map.Entry<Key,Value> entry : scanner) {
                    AtomKeyValueParser atom = AtomKeyValueParser.parse(entry.getKey(), entry.getValue());
                    if (atom.getUpdated().after(maxDate)) {
                        maxDate = atom.getUpdated();
                    }
                    nextLastKey = entry.getKey();
                    Entry e = atom.toEntry(abdera, this.host, this.port);
                    result.addEntry(e);
                    count++;
                    if (count >= pagesize)
                        break;
                }
            }
            
            String thisLastKey = "";
            if (null != nextLastKey)
                thisLastKey = serializeKey(nextLastKey);
            String id = MessageFormat.format(COLLECTION_LINK_FORMAT, this.host, this.port, category);
            result.setId(id);
            result.addLink(id + "?pagesize=" + pagesize, "first"); // need a link that contains the offset of null and current pagesize
            result.addLink(id + "?pagesize=" + pagesize + "&l=" + thisLastKey, "next"); // need a link that contains the next offset and current pagesize
            result.setUpdated(maxDate);
            
            if (count == 0)
                throw new NoResultsException(null);
            else
                return result;
            
        } catch (WebApplicationException web) {
            throw web;
        } catch (Exception e) {
            VoidResponse response = new VoidResponse();
            QueryException qe = new QueryException(DatawaveErrorCode.FEED_GET_ERROR, e, MessageFormat.format("collection: {0}", category));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response);
        } finally {
            if (null != client) {
                try {
                    connectionFactory.returnClient(client);
                } catch (Exception e) {
                    log.error("Error returning connection to factory", e);
                }
            }
        }
    }
    
    /**
     *
     * @param category
     *            collection name
     * @param id
     *            entry id
     * @return Atom Entry document for the id.
     */
    @GET
    @GZIP
    @Produces("application/atom+xml;type=entry")
    @Path("/{category}/{id}")
    public Entry getEntry(@Required("category") @PathParam("category") String category, @Required("id") @PathParam("id") String id) {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        Set<Authorizations> auths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            for (Collection<String> cbAuths : dp.getAuthorizations())
                auths.add(new Authorizations(cbAuths.toArray(new String[cbAuths.size()])));
        }
        
        Entry result = null;
        AccumuloClient client = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            client = connectionFactory.getClient(poolName, Priority.NORMAL, trackingMap);
            
            try (Scanner scanner = ScannerHelper.createScanner(client, tableName, auths)) {
                scanner.setRange(new Range(category, true, category + "\1", false));
                // ID is fieldValue\0UUID
                scanner.fetchColumnFamily(new Text(AtomKeyValueParser.decodeId(id)));
                for (Map.Entry<Key,Value> entry : scanner) {
                    result = AtomKeyValueParser.parse(entry.getKey(), entry.getValue()).toEntry(abdera, this.host, this.port);
                    break;
                }
            }
            if (null == result)
                throw new NoResultsException(null);
            else
                return result;
            
        } catch (WebApplicationException web) {
            throw web;
        } catch (Exception e) {
            VoidResponse response = new VoidResponse();
            QueryException qe = new QueryException(DatawaveErrorCode.ENTRY_RETRIEVAL_ERROR, e, MessageFormat.format("entry: {0} from collection: {1}", id,
                            category));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            throw new DatawaveWebApplicationException(qe, response);
        } finally {
            if (null != client) {
                try {
                    connectionFactory.returnClient(client);
                } catch (Exception e) {
                    log.error("Error returning connection to factory", e);
                }
            }
        }
    }
    
    private Key deserializeKey(String k) throws Exception {
        String key64 = URLDecoder.decode(k, "UTF-8");
        byte[] bKey = Base64.decode(key64);
        ByteArrayInputStream bais = new ByteArrayInputStream(bKey);
        DataInputStream in = new DataInputStream(bais);
        Key key = new Key();
        key.readFields(in);
        return key;
    }
    
    private String serializeKey(Key key) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        DataOutputStream out = new DataOutputStream(baos);
        key.write(out);
        out.close();
        String key64 = Base64.encodeBytes(baos.toByteArray());
        return URLEncoder.encode(key64, "UTF-8");
    }
    
}
