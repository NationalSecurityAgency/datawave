package nsa.datawave.ingest.poller.manager;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import nsa.datawave.ingest.data.RawRecordContainer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.log4j.Logger;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.FailoverEventListener;
import org.hornetq.api.core.client.FailoverEventType;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.api.core.client.loadbalance.RandomConnectionLoadBalancingPolicy;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;

public class HornetQEventProcessor extends BaseEventProcessor implements FailoverEventListener, SessionFailureListener, ClusterTopologyListener {
    
    private static final long serialVersionUID = 1L;
    private static final Logger log = Logger.getLogger(HornetQEventProcessor.class);
    private static final long DEFAULT_LATENCY = 50;
    
    public static final String QUEUE_NAME = "EventQueue";
    public static final String QUEUE_ADDRESS = "queue.EventQueue";
    
    private Option hornetQHostName, hornetQPort;
    private String host, port;
    private ClientSession session = null;
    private ClientProducer producer = null;
    private ClientSessionFactory factory = null;
    private ServerLocator locator = null;
    
    public class JmsSendThread extends SendThread {
        
        private volatile boolean shutdown = false;
        private List<RawRecordContainer> events = new ArrayList<>();
        
        public void shutdown() {
            this.shutdown = true;
        }
        
        @Override
        public void run() {
            while (!shutdown) {
                // wait until half full, then start sending data to NiFi
                try {
                    synchronized (lock) {
                        lock.wait(DEFAULT_LATENCY);
                    }
                } catch (InterruptedException e) {}
                
                if (shutdown)
                    break;
                
                if (size() == 0)
                    continue;
                
                try {
                    events.clear();
                    drainTo(events);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(256 * events.size());
                    GZIPOutputStream gzos = new GZIPOutputStream(baos, 1024 * events.size());
                    CountingOutputStream cos = new CountingOutputStream(gzos);
                    DataOutputStream out = new DataOutputStream(cos);
                    for (RawRecordContainer e : events) {
                        try {
                            e.write(out);
                        } catch (IOException e1) {
                            log.error("Error serializing event: " + e.toString(), e1);
                        }
                    }
                    out.close();
                    log.info("Sending event package of " + events.size() + " events of size: " + baos.size());
                    ClientMessage message = session.createMessage(false);
                    message.setExpiration(System.currentTimeMillis() + 86400000);
                    message.getBodyBuffer().writeBytes(baos.toByteArray());
                    message.putLongProperty("NUM_EVENTS", events.size());
                    message.putLongProperty("UNCOMPRESSED_BYTE_COUNT", cos.getByteCount());
                    message.putLongProperty("COMPRESSED_BYTE_COUNT", baos.size());
                    producer.send(message);
                    counter.addSent(events.size());
                    counter.incrementMessagesSent();
                    log.info(counter.toString());
                } catch (IOException e) {
                    log.error("Error writing event to output stream", e);
                } catch (HornetQException e) {
                    log.error("Error sending message to server", e);
                }
            }
        }
        
    }
    
    public HornetQEventProcessor() {
        super();
    }
    
    @Override
    public void _configure(CommandLine cl) throws Exception {
        
        if (!cl.hasOption(hornetQHostName.getOpt())) {
            throw new MissingOptionException(Collections.singletonList(hornetQHostName));
        }
        host = cl.getOptionValue(hornetQHostName.getOpt());
        
        if (!cl.hasOption(hornetQPort.getOpt())) {
            throw new MissingOptionException(Collections.singletonList(hornetQPort));
        }
        port = cl.getOptionValue(hornetQPort.getOpt());
        
        String[] hosts = host.split(":");
        List<String> hArray = Arrays.asList(hosts);
        Collections.shuffle(hArray);
        TransportConfiguration[] tconfigs = new TransportConfiguration[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            Map<String,Object> params = new HashMap<>();
            params.put(TransportConstants.USE_NIO_PROP_NAME, true);
            params.put(TransportConstants.HOST_PROP_NAME, hArray.get(i));
            params.put(TransportConstants.PORT_PROP_NAME, port);
            params.put(TransportConstants.BATCH_DELAY, 50);
            params.put(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME, 256 * 1024);
            TransportConfiguration tConfig = new TransportConfiguration(NettyConnectorFactory.class.getName(), params, UUID.randomUUID().toString());
            tconfigs[i] = tConfig;
        }
        
        locator = HornetQClient.createServerLocatorWithHA(tconfigs);
        locator.setBlockOnNonDurableSend(false);
        locator.setMinLargeMessageSize(10485760);
        locator.setConnectionTTL(-1);
        locator.setProducerWindowSize(-1);
        locator.setReconnectAttempts(-1);
        locator.setRetryIntervalMultiplier(1);
        locator.setConnectionLoadBalancingPolicyClassName(RandomConnectionLoadBalancingPolicy.class.getName());
        locator.addClusterTopologyListener(this);
        factory = locator.createSessionFactory();
        setupSession();
    }
    
    private void setupSession() throws HornetQException {
        session = factory.createSession();
        session.addFailoverListener(this);
        session.addFailureListener(this);
        producer = session.createProducer(QUEUE_ADDRESS);
    }
    
    @Override
    public Collection<Option> getConfigurationOptions() {
        Collection<Option> o = super.getConfigurationOptions();
        
        hornetQHostName = new Option("hqHost", "hornetQHost", true, "hostname of the HornetQ server");
        hornetQHostName.setRequired(true);
        hornetQHostName.setArgs(1);
        hornetQHostName.setType(String.class);
        o.add(hornetQHostName);
        
        hornetQPort = new Option("hqPort", "hornetQPort", true, "port for the HornetQ server");
        hornetQPort.setRequired(true);
        hornetQPort.setArgs(1);
        hornetQPort.setType(Integer.class);
        o.add(hornetQPort);
        
        return o;
    }
    
    @Override
    protected SendThread getSendThread() {
        return new JmsSendThread();
    }
    
    @Override
    public void close() {
        if (null != producer)
            try {
                producer.close();
            } catch (HornetQException e) {
                log.error("Error closing message producer", e);
            }
        if (null != session)
            try {
                session.close();
            } catch (HornetQException e) {
                log.error("Error closing message producer", e);
            }
        super.close();
    }
    
    @Override
    public void connectionFailed(HornetQException exception, boolean failedOver) {
        log.warn("Connection Failed. Failed Over: " + failedOver + ". Message: " + exception.getMessage());
    }
    
    @Override
    public void failoverEvent(FailoverEventType eventType) {
        log.warn("Failover: " + eventType.toString());
        if (FailoverEventType.FAILURE_DETECTED.equals(eventType)) {
            if (null != producer)
                try {
                    producer.close();
                } catch (HornetQException e) {
                    log.error("Error closing message producer", e);
                }
            if (null != session)
                try {
                    session.close();
                } catch (HornetQException e) {
                    log.error("Error closing message producer", e);
                }
            try {
                setupSession();
            } catch (HornetQException e) {
                log.error("Error trying to reconnect", e);
            }
        }
    }
    
    @Override
    public void beforeReconnect(HornetQException exception) {
        log.warn("Exception occurred, attempting to reconnect. Message: " + exception.getMessage());
    }
    
    @Override
    public void nodeUP(TopologyMember member, boolean last) {
        log.info("Node has joined the cluster: " + member.getNodeId());
    }
    
    @Override
    public void nodeDown(long eventUID, String nodeID) {
        log.info("Node has joined the cluster: " + nodeID);
    }
    
}
