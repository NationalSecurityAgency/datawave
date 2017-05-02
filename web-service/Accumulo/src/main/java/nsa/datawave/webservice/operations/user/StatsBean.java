package nsa.datawave.webservice.operations.user;

import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.exception.AccumuloWebApplicationException;
import nsa.datawave.webservice.exception.UnauthorizedException;
import nsa.datawave.webservice.response.StatsProperties;
import nsa.datawave.webservice.response.StatsResponse;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.log4j.Logger;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.xml.sax.InputSource;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJB;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

@Path("/Accumulo")
@RolesAllowed({"InternalUser", "Administrator"})
@DeclareRoles({"InternalUser", "Administrator"})
@LocalBean
@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
public class StatsBean {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @EJB
    private AccumuloConnectionFactory connectionFactory;
    
    private ZooKeeperInstance instance = null;
    private String accumuloStatsURL = null;
    
    public StatsBean() {
        
    }
    
    @PermitAll
    public StatsResponse stats() {
        
        if (this.accumuloStatsURL == null) {
            Connector connection = null;
            AccumuloConnectionFactory.Priority priority = AccumuloConnectionFactory.Priority.ADMIN;
            try {
                Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
                connection = connectionFactory.getConnection(priority, trackingMap);
                this.instance = (ZooKeeperInstance) connection.getInstance();
                this.accumuloStatsURL = getStatsURL(instance.getMasterLocations());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                if (connection != null) {
                    try {
                        connectionFactory.returnConnection(connection);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
        
        StatsResponse response = new StatsResponse();
        Connector connection = null;
        AccumuloConnectionFactory.Priority priority = AccumuloConnectionFactory.Priority.ADMIN;
        
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            connection = connectionFactory.getConnection(priority, trackingMap);
            
            ClientRequest request = new ClientRequest(this.accumuloStatsURL);
            ClientResponse<String> clientResponse = null;
            
            clientResponse = request.get(String.class);
            Response.Status status = clientResponse.getResponseStatus();
            int httpStatusCode = status.getStatusCode();
            if (httpStatusCode == 200) {
                String entity = clientResponse.getEntity();
                entity = entity.replaceFirst("<stats>", "<stats xmlns=\"" + StatsProperties.NAMESPACE + "\">");
                // ByteArrayInputStream istream = new ByteArrayInputStream(entity.getBytes(Charset.forName("UTF-8")));
                
                SAXParserFactory spf = SAXParserFactory.newInstance();
                spf.setFeature("http://xml.org/sax/features/external-general-entities", false);
                spf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
                spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                
                Source xmlSource = new SAXSource(spf.newSAXParser().getXMLReader(), new InputSource(new StringReader(entity)));
                
                JAXBContext ctx = JAXBContext.newInstance(Object.class);
                Unmarshaller um = ctx.createUnmarshaller();
                response = (StatsResponse) um.unmarshal(xmlSource);
                
            } else {
                log.error("Error returned requesting stats from the cloud: " + httpStatusCode);
                response.addException(new RuntimeException("Error returned requesting stats from the cloud: " + httpStatusCode));
                
                // maybe the monitor has moved, re-resolve the stats location
                this.accumuloStatsURL = null;
            }
            return response;
            
        } catch (AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
            response.addException(e);
            throw new UnauthorizedException(e, response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            response.addException(e);
            throw new AccumuloWebApplicationException(e, response);
        } finally {
            try {
                connectionFactory.returnConnection(connection);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                response.addException(e);
            }
        }
    }
    
    /**
     * Retrieve statistics from the Accumulo monitor (Requires Administrator role)
     *
     * @HTTP 200 Success
     * @HTTP 500 Error while retrieving statistics
     * @return nsa.datawave.webservice.response.StatsResponse
     */
    @Path("/Stats")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml"})
    @GET
    public StatsResponse accumuloStats() {
        return stats();
    }
    
    private String getStatsURL(List<String> masterLocations) {
        String statsURL = null;
        
        if (masterLocations.size() == 1) {
            String s = masterLocations.get(0);
            int x = s.indexOf(":");
            if (x >= 0) {
                s = s.substring(0, x);
            }
            statsURL = "http://" + s + ":50095/xml";
        } else if (masterLocations.size() > 1) {
            for (String s : masterLocations) {
                int x = s.indexOf(":");
                if (x >= 0) {
                    s = s.substring(0, x);
                }
                statsURL = "http://" + s + ":50095/xml";
                
                ClientRequest request = new ClientRequest(statsURL);
                ClientResponse<String> clientResponse = null;
                
                int httpStatusCode = 0;
                try {
                    clientResponse = request.get(String.class);
                    Response.Status status = clientResponse.getResponseStatus();
                    httpStatusCode = status.getStatusCode();
                } catch (Exception e) {
                    statsURL = null;
                } finally {
                    if (clientResponse != null) {
                        clientResponse.releaseConnection();
                    }
                }
                
                if (httpStatusCode != 200) {
                    continue;
                }
            }
        }
        return statsURL;
    }
}
