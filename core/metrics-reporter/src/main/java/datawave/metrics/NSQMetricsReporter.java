package datawave.metrics;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

/**
 * Reports Dropwizard metrics to Timely via NSQ.
 */
public class NSQMetricsReporter extends TimelyMetricsReporter {
    private static final int MAX_MESSAGE_SIZE = 7 * 1024;
    
    private CloseableHttpClient client = HttpClients.createDefault();
    private URI endpoint;
    private DataOutputStream dos;
    private ByteArrayOutputStream baos;
    
    protected NSQMetricsReporter(String timelyHost, int timelyPort, MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
                    TimeUnit durationUnit) {
        super(timelyHost, timelyPort, registry, name, filter, rateUnit, durationUnit);
        try {
            endpoint = new URIBuilder().setScheme("http").setHost(timelyHost).setPort(timelyPort).setPath("/mpub").addParameter("topic", "metrics").build();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to construct NSQ URI: " + e.getMessage(), e);
        }
    }
    
    @Override
    protected synchronized boolean connect() {
        baos = new ByteArrayOutputStream();
        dos = new DataOutputStream(baos);
        return true;
    }
    
    @Override
    protected synchronized void reportMetric(String timelyMetric) {
        // Flush the message if adding the new metric will take us over the maximum message size.
        if (dos.size() + timelyMetric.length() > MAX_MESSAGE_SIZE) {
            flush();
            connect();
        }
        
        try {
            dos.writeBytes(timelyMetric);
        } catch (IOException e) {
            logger.error("Error sending metrics to NSQ: {}", e.getMessage(), e);
        }
    }
    
    @Override
    protected synchronized void flush() {
        try {
            dos.close();
            HttpPost post = new HttpPost(endpoint);
            post.setEntity(new ByteArrayEntity(baos.toByteArray(), ContentType.DEFAULT_BINARY));
            HttpResponse response = client.execute(post);
            int code = response.getStatusLine().getStatusCode();
            String msg = response.getStatusLine().getReasonPhrase();
            // Consume the entire response so the connection will be reused
            EntityUtils.consume(response.getEntity());
            if (code != HttpStatus.SC_OK) {
                logger.info("Error sending metrics to NSQ. Code: {}, msg: {}", code, msg);
            }
        } catch (IOException e) {
            logger.error("Error sending metrics to NSQ: {}", e.getMessage(), e);
        }
    }
    
    @Override
    public void stop() {
        try {
            super.stop();
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
