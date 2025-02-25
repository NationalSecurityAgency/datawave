package datawave.microservice.accumulo.stats;

import java.io.StringReader;
import java.lang.annotation.Annotation;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.UnmarshallerHandler;
import javax.xml.bind.annotation.XmlSchema;
import javax.xml.parsers.SAXParserFactory;

import org.apache.accumulo.core.client.AccumuloClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
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

import datawave.microservice.accumulo.stats.config.StatsProperties;
import datawave.microservice.accumulo.stats.util.AccumuloMonitorLocator;
import datawave.webservice.response.StatsResponse;

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
public class StatsService implements InitializingBean {
    
    @Autowired
    private StatsProperties statsProperties;
    
    private String monitorStatsUri = null;
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final AccumuloClient warehouseClient;
    private final RestTemplate restTemplate;
    private final String namespace;
    
    @Autowired
    //@formatter:off
    public StatsService(
            @Qualifier("warehouse") AccumuloClient warehouseClient,
            RestTemplateBuilder restTemplateBuilder) {
        this.warehouseClient = warehouseClient;
        this.restTemplate = restTemplateBuilder.build();
        this.namespace = getNamespace();
        //@formatter:on
    }
    
    private String getNamespace() {
        String namespace = "";
        for (Annotation a : StatsResponse.class.getPackage().getAnnotations()) {
            if (a instanceof XmlSchema) {
                namespace = ((XmlSchema) a).namespace();
                break;
            }
        }
        return namespace;
    }
    
    public synchronized void discoverAccumuloMonitor() {
        try {
            monitorStatsUri = String.format(statsProperties.getMonitorStatsUriTemplate(), new AccumuloMonitorLocator().getHostPort(warehouseClient));
        } catch (Exception e) {
            log.error("Failed to discover Accumulo monitor location", e);
        }
    }
    
    @Override
    public void afterPropertiesSet() throws Exception {
        discoverAccumuloMonitor();
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
                NamespaceFilter nsFilter = new NamespaceFilter(this.namespace);
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
