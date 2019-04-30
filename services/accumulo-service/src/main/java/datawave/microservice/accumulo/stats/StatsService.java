package datawave.microservice.accumulo.stats;

import datawave.microservice.accumulo.stats.config.StatsConfiguration.JaxbProperties;
import datawave.microservice.accumulo.stats.util.AccumuloMonitorLocator;
import datawave.webservice.response.StatsResponse;
import org.apache.accumulo.core.client.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

import javax.annotation.PostConstruct;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.UnmarshallerHandler;
import javax.xml.parsers.SAXParserFactory;
import java.io.StringReader;

/**
 * This service implements Accumulo stats retrieval by using a {@link RestTemplate} client to fetch the Accumulo monitor's XML response, which is ultimately
 * returned as a {@link StatsResponse} instance
 *
 * <p>
 * Note that, in addition to the Accumulo monitor, the service also depends on an external ZK server, which it uses to automatically discover the monitor's
 * host:port
 */
@Service
@ConditionalOnProperty(name = "accumulo.stats.enabled", havingValue = "true", matchIfMissing = true)
public class StatsService {
    
    public static final String MONITOR_STATS_URI = "http://%s/xml";
    
    private String monitorStatsUri = null;
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final Instance warehouseInstance;
    private final RestTemplate restTemplate;
    private final JaxbProperties namespaceProperties;
    
    @Autowired
    //@formatter:off
    public StatsService(
            @Qualifier("warehouse") Instance warehouseInstance,
            RestTemplateBuilder restTemplateBuilder,
            JaxbProperties namespaceProperties) {
        this.warehouseInstance = warehouseInstance;
        this.restTemplate = restTemplateBuilder.build();
        this.namespaceProperties = namespaceProperties;
        //@formatter:on
    }
    
    @PostConstruct
    public synchronized void discoverAccumuloMonitor() {
        try {
            monitorStatsUri = String.format(MONITOR_STATS_URI, new AccumuloMonitorLocator().getHostPort(warehouseInstance));
        } catch (Exception e) {
            log.error("Failed to discover Accumulo monitor location", e);
        }
    }
    
    /**
     * Retrieves statistics from the Accumulo monitor
     *
     * @return {@link StatsResponse}
     */
    public synchronized StatsResponse getStats() {
        
        // Keep re-trying for the stats URL if we couldn't locate it at startup
        if (this.monitorStatsUri == null) {
            discoverAccumuloMonitor();
        }
        
        StatsResponse response = new StatsResponse();
        
        try {
            log.debug("Submitting Accumulo monitor request to {}", monitorStatsUri);
            
            ResponseEntity<String> monitorResponse = restTemplate.exchange(monitorStatsUri, HttpMethod.GET, null, String.class);
            
            if (monitorResponse.getStatusCode().value() == HttpStatus.OK.value()) {
                NamespaceFilter nsFilter = new NamespaceFilter(this.namespaceProperties.getNamespace());
                SAXParserFactory spf = SAXParserFactory.newInstance();
                spf.setFeature("http://xml.org/sax/features/external-general-entities", false);
                spf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
                spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                nsFilter.setParent(spf.newSAXParser().getXMLReader());
                
                JAXBContext ctx = JAXBContext.newInstance(StatsResponse.class);
                UnmarshallerHandler umHandler = ctx.createUnmarshaller().getUnmarshallerHandler();
                nsFilter.setContentHandler(umHandler);
                nsFilter.parse(new InputSource(new StringReader(monitorResponse.getBody())));
                response = (StatsResponse) umHandler.getResult();
            } else {
                String errorMessage = String.format("Monitor request failed. Http Status: (%s, %s)", monitorResponse.getStatusCodeValue(),
                                monitorResponse.getStatusCode().getReasonPhrase());
                log.error(errorMessage);
                response.addException(new RuntimeException(errorMessage));
                
                // maybe the monitor has moved, force rediscovery of its location
                this.monitorStatsUri = null;
            }
            
        } catch (Exception e) {
            log.error("Failed to retrieve stats from Accumulo", e);
            throw new RuntimeException(e);
        }
        
        return response;
    }
    
    /**
     * {@link XMLFilterImpl} that will inject the given namespace URI into XML retrieved from the Accumulo monitor
     */
    public static class NamespaceFilter extends XMLFilterImpl {
        private final String xmlNamespace;
        
        private NamespaceFilter(String xmlNamespace) {
            this.xmlNamespace = xmlNamespace;
        }
        
        @Override
        public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            super.startElement(xmlNamespace, localName, qName, atts);
        }
        
        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(xmlNamespace, localName, qName);
        }
    }
}
