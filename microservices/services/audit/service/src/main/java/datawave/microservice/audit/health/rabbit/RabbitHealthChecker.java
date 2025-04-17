package datawave.microservice.audit.health.rabbit;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;

import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.domain.BindingInfo;
import com.rabbitmq.http.client.domain.ExchangeInfo;
import com.rabbitmq.http.client.domain.NodeInfo;
import com.rabbitmq.http.client.domain.QueueInfo;

import datawave.microservice.audit.AuditController;
import datawave.microservice.audit.health.HealthChecker;
import datawave.microservice.audit.health.rabbit.config.RabbitHealthProperties;
import datawave.microservice.audit.health.rabbit.config.RabbitHealthProperties.BindingProperties;
import datawave.microservice.audit.health.rabbit.config.RabbitHealthProperties.ClusterProperties;
import datawave.microservice.audit.health.rabbit.config.RabbitHealthProperties.ExchangeProperties;
import datawave.microservice.audit.health.rabbit.config.RabbitHealthProperties.ManagementProperties;
import datawave.microservice.audit.health.rabbit.config.RabbitHealthProperties.QueueProperties;

/**
 * An implementation for {@link HealthChecker}, which can be used to monitor the RabbitMQ messaging infrastructure.
 * <p>
 * This health checker uses the RabbitMQ management api to check the status of the cluster, exchanges, queues, and bindings. The information returned by the
 * management API is compared with the configuration information supplied in the audit config.
 * <p>
 * The poll rate used by the health checker differs depending on whether RabbitMQ is healthy or not. While this is configurable, the default behavior is to use
 * a slow poll rate when healthy, and a faster poll rate when unhealthy.
 * <p>
 * Once a problem is detected, the isHealthy method will return false, which in turn will cause the {@link AuditController} to reject audit requests, and cause
 * the health endpoint to report the audit service as RABBITMQ_UNHEALTHY.
 * <p>
 * If properly configured, the RabbitHealthChecker can attempt to repair the RabbitMQ configuration if a problem is detected. Any exchanges, queues, or bindings
 * which are missing will be created, and any exchanges, or bindings which have an invalid configuration will be deleted, and recreated. Queues with invalid
 * configurations will only be deleted, and recreated if they are empty.
 * <p>
 * Regardless, health checks will continue to be performed. Once we determine that RabbitMQ is healthy, the {@link AuditController} rest endpoint will resume
 * processing messages, and the health endpoint will report the audit service as UP.
 *
 */
public class RabbitHealthChecker implements HealthChecker, HealthIndicator {
    private static Logger log = LoggerFactory.getLogger(RabbitHealthChecker.class);
    
    private static final String DEFAULT_VHOST = "/";
    
    static final Status RABBITMQ_UNHEALTHY = new Status("RABBITMQ_UNHEALTHY");
    
    private final RabbitHealthProperties rabbitHealthProperties;
    
    // rabbit cluster info
    private String host;
    private String username;
    private String password;
    
    private Client rabbitClient;
    
    private List<ExchangeInfo> exchanges;
    private List<QueueInfo> queues;
    private List<BindingInfo> bindings;
    
    // cluster health
    private boolean clusterHealthy = false;
    private int numNodesMissing = 0;
    private int numTimesMissingNode = 0;
    
    // exchanges health
    private boolean exchangesHealthy = false;
    private List<ExchangeInfo> missingExchanges;
    private List<InvalidPairing<ExchangeInfo>> invalidExchanges;
    
    // queues health
    private boolean queuesHealthy = false;
    private List<QueueInfo> missingQueues;
    private List<InvalidPairing<QueueInfo>> invalidQueues;
    
    // bindings health
    private boolean bindingsHealthy = false;
    private List<BindingInfo> missingBindings;
    private List<InvalidPairing<BindingInfo>> invalidBindings;
    
    // outage stats
    private Date lastSuccessfulHealthCheck;
    private RabbitOutageStats currentOutageStats;
    private TreeSet<RabbitOutageStats> outageStats = new TreeSet<>();
    
    /**
     * This is a convenience class used to link a desired configuration with a detected invalid configuration.
     *
     * @param <T>
     *            For our purposes, either {@link ExchangeInfo}, {@link QueueInfo}, or {@link BindingInfo}
     */
    private static class InvalidPairing<T> {
        public T desired;
        public T detected;
        
        public InvalidPairing(T desired, T detected) {
            this.desired = desired;
            this.detected = detected;
        }
    }
    
    public RabbitHealthChecker(RabbitHealthProperties rabbitHealthProperties, CachingConnectionFactory rabbitConnectionFactory) {
        this(rabbitHealthProperties, rabbitConnectionFactory.getHost(), rabbitConnectionFactory.getUsername(),
                        rabbitConnectionFactory.getRabbitConnectionFactory().getPassword());
    }
    
    public RabbitHealthChecker(RabbitHealthProperties rabbitHealthProperties, String host, String username, String password) {
        this.rabbitHealthProperties = rabbitHealthProperties;
        
        ManagementProperties mgmtProps = rabbitHealthProperties.getManagement();
        this.host = (!mgmtProps.getHost().isEmpty()) ? mgmtProps.getHost() : host;
        this.username = (!mgmtProps.getUsername().isEmpty()) ? mgmtProps.getUsername() : username;
        this.password = (!mgmtProps.getPassword().isEmpty()) ? mgmtProps.getPassword() : password;
        
        init();
    }
    
    private void init() {
        initRabbitClient();
        
        exchanges = rabbitHealthProperties.getExchanges().stream().map(RabbitHealthChecker::createExchange).collect(Collectors.toList());
        queues = rabbitHealthProperties.getQueues().stream().map(RabbitHealthChecker::createQueue).collect(Collectors.toList());
        bindings = rabbitHealthProperties.getBindings().stream().map(RabbitHealthChecker::createBinding).collect(Collectors.toList());
    }
    
    /**
     * This method is used to establish a {@link Client}. In the event that the RabbitMQ cluster is unavailable at startup, we will attempt to initialize the
     * client once per health check.
     */
    private void initRabbitClient() {
        if (rabbitClient == null) {
            try {
                ManagementProperties mgmtProps = rabbitHealthProperties.getManagement();
                URL rabbitMgmtUrl = new URIBuilder().setScheme(mgmtProps.getScheme()).setHost(host).setPort(mgmtProps.getPort()).setPath(mgmtProps.getUri())
                                .build().toURL();
                rabbitClient = new Client(rabbitMgmtUrl, username, password);
            } catch (MalformedURLException | URISyntaxException e) {
                log.warn("Unable to establish a rabbit client.", e);
            }
        }
    }
    
    /**
     * Creates an {@link ExchangeInfo} from the configured {@link ExchangeProperties}
     *
     * @param ep
     *            The exchange properties derived from the {@link RabbitHealthProperties}
     * @return An {@link ExchangeInfo}
     */
    private static ExchangeInfo createExchange(ExchangeProperties ep) {
        ExchangeInfo exchangeInfo = new ExchangeInfo(ep.getType(), ep.isDurable(), ep.isAutoDelete(), ep.isInternal(), null);
        exchangeInfo.setName(ep.getName());
        return exchangeInfo;
    }
    
    /**
     * Creates an {@link QueueInfo} from the configured {@link QueueProperties}
     *
     * @param qp
     *            The queue properties derived from the {@link RabbitHealthProperties}
     * @return A {@link QueueInfo}
     */
    private static QueueInfo createQueue(QueueProperties qp) {
        QueueInfo queue = new QueueInfo(qp.isDurable(), qp.isExclusive(), qp.isAutoDelete(), qp.getArguments());
        queue.setName(qp.getName());
        return queue;
    }
    
    /**
     * Creates an {@link BindingInfo} from the configured {@link BindingProperties}
     *
     * @param bp
     *            The binding properties derived from the {@link RabbitHealthProperties}
     * @return A {@link BindingInfo}
     */
    private static BindingInfo createBinding(BindingProperties bp) {
        BindingInfo bindingInfo = new BindingInfo();
        switch (bp.getDestinationType().toLowerCase()) {
            case "queue":
            case "exchange":
                bindingInfo.setDestinationType(bp.getDestinationType().toLowerCase());
                break;
            default:
                throw new RuntimeException("Unable to create binding given properties: [" + bp + "]");
        }
        bindingInfo.setDestination(bp.getDestination());
        bindingInfo.setSource(bp.getSource());
        bindingInfo.setRoutingKey(bp.getRoutingKey());
        bindingInfo.setArguments(bp.getArguments());
        return bindingInfo;
    }
    
    /**
     * Determines which poll interval should be used, based on the overall RabbitMQ health
     *
     * @return the poll interval to use in millis
     */
    @Override
    public long pollIntervalMillis() {
        if (isHealthy())
            return rabbitHealthProperties.getHealthyPollIntervalMillis();
        else
            return rabbitHealthProperties.getUnhealthyPollIntervalMillis();
    }
    
    /**
     * Performs a health check of RabbitMQ.
     * <p>
     * This method is synchronized with the isHealthy method to ensure that the health check is given time to complete before deciding whether to accept an
     * audit request via the {@link AuditController}
     * <p>
     * RabbitMQ is considered to be healthy if the cluster, exchanges, queues, and bindings are configured as specified in the {@link RabbitHealthProperties}.
     * If we are not able to determine the health of the cluster, exchanges, queues, and bindings (perhaps due to the management API being unavailable, or the
     * cluster being down), we will consider RabbitMQ to be unhealthy.
     */
    @Override
    public void runHealthCheck() {
        initRabbitClient();
        
        if (rabbitClient != null) {
            synchronized (this) {
                log.trace("RabbitMQ Health Check - Started");
                
                boolean wasHealthy = isHealthy();
                
                // check the cluster
                clusterHealthCheck();
                
                // check the exchanges
                exchangesHealthCheck();
                
                // check the queues
                queuesHealthCheck();
                
                // check the bindings
                bindingsHealthCheck();
                
                boolean isHealthy = isHealthy();
                
                if (wasHealthy) {
                    if (isHealthy) {
                        log.debug("RabbitMQ is still healthy.");
                        lastSuccessfulHealthCheck = new Date();
                    } else {
                        log.warn("RabbitMQ is not healthy.");
                        currentOutageStats = new RabbitOutageStats(lastSuccessfulHealthCheck);
                        outageStats.add(currentOutageStats);
                        updateOutageStats(currentOutageStats);
                        log.warn(currentOutageStats.toString());
                    }
                } else {
                    if (isHealthy) {
                        lastSuccessfulHealthCheck = new Date();
                        if (currentOutageStats != null) {
                            currentOutageStats.setStopDate(lastSuccessfulHealthCheck);
                            log.warn(currentOutageStats.toString());
                            currentOutageStats = null;
                            log.info("RabbitMQ has recovered.");
                        } else {
                            log.info("RabbitMQ is healthy");
                        }
                    } else {
                        log.debug("RabbitMQ is still unhealthy.");
                        if (currentOutageStats != null) {
                            updateOutageStats(currentOutageStats);
                        }
                    }
                }
                
                log.trace("RabbitMQ Health Check - Complete");
            }
        }
    }
    
    /**
     * Checks that the correct number of nodes are present in the RabbitMQ cluster.
     * <p>
     * If there are less nodes than specified in {@link RabbitHealthProperties}, then that could cause the cluster health check to fail.
     * <p>
     * The audit service can be configured to run indefinitely with less than the number of desired RabbitMQ nodes.
     * <p>
     * The default behavior is for the cluster health to be marked as unhealthy only once the specified number of failed health checks is reached.
     */
    private void clusterHealthCheck() {
        ClusterProperties clusterProps = rabbitHealthProperties.getCluster();
        
        List<NodeInfo> rabbitNodes = null;
        try {
            rabbitNodes = rabbitClient.getNodes();
        } catch (Exception e) {
            log.trace("Unable to get RabbitMQ node info.", e);
        }
        
        if (rabbitNodes != null) {
            // only count the running nodes
            rabbitNodes = rabbitNodes.stream().filter(NodeInfo::isRunning).collect(Collectors.toList());
            numNodesMissing = (rabbitNodes.size() < clusterProps.getExpectedNodes()) ? (clusterProps.getExpectedNodes() - rabbitNodes.size()) : 0;
        } else
            numNodesMissing = clusterProps.getExpectedNodes();
        
        if (numNodesMissing > 0) {
            numTimesMissingNode++;
            
            if (clusterProps.isFailIfNodeMissing() && numTimesMissingNode > clusterProps.getNumChecksBeforeFailure())
                clusterHealthy = false;
        } else {
            numTimesMissingNode = 0;
            clusterHealthy = true;
        }
        
        if (clusterHealthy)
            log.trace("RabbitMQ Cluster is healthy");
        else
            log.trace("RabbitMQ Cluster is unhealthy");
    }
    
    /**
     * Checks that the desired exchanges are present, and correctly configured.
     */
    private void exchangesHealthCheck() {
        List<ExchangeInfo> missingExchanges = new ArrayList<>();
        List<InvalidPairing<ExchangeInfo>> invalidExchanges = new ArrayList<>();
        
        List<ExchangeInfo> returnedExchanges = null;
        try {
            returnedExchanges = rabbitClient.getExchanges();
        } catch (Exception e) {
            log.trace("Unable to get RabbitMQ exchange info.", e);
        }
        
        if (returnedExchanges != null) {
            Map<String,ExchangeInfo> detectedExchanges = returnedExchanges.stream().collect(Collectors.toMap(ExchangeInfo::getName, e -> e));
            for (ExchangeInfo exchange : exchanges) {
                if (!detectedExchanges.containsKey(exchange.getName()))
                    missingExchanges.add(exchange);
                else if (!isExchangeValid(exchange, detectedExchanges.get(exchange.getName())))
                    invalidExchanges.add(new InvalidPairing<>(exchange, detectedExchanges.get(exchange.getName())));
            }
        } else {
            missingExchanges.addAll(exchanges);
        }
        
        exchangesHealthy = missingExchanges.isEmpty() && invalidExchanges.isEmpty();
        this.missingExchanges = missingExchanges;
        this.invalidExchanges = invalidExchanges;
        
        if (exchangesHealthy)
            log.trace("RabbitMQ Exchanges are healthy");
        else
            log.trace("RabbitMQ Exchanges are unhealthy");
    }
    
    /**
     * Checks that the desired queues are present, and correctly configured.
     */
    private void queuesHealthCheck() {
        List<QueueInfo> missingQueues = new ArrayList<>();
        List<InvalidPairing<QueueInfo>> invalidQueues = new ArrayList<>();
        
        List<QueueInfo> returnedQueues = null;
        try {
            returnedQueues = rabbitClient.getQueues();
        } catch (Exception e) {
            log.trace("Unable to get RabbitMQ queue info.", e);
        }
        
        if (returnedQueues != null) {
            Map<String,QueueInfo> detectedQueues = returnedQueues.stream().collect(Collectors.toMap(QueueInfo::getName, q -> q));
            for (QueueInfo queue : queues) {
                if (!detectedQueues.containsKey(queue.getName()))
                    missingQueues.add(queue);
                else if (!isQueueValid(queue, detectedQueues.get(queue.getName())))
                    invalidQueues.add(new InvalidPairing<>(queue, detectedQueues.get(queue.getName())));
            }
        } else {
            missingQueues.addAll(queues);
        }
        
        queuesHealthy = missingQueues.isEmpty() && invalidQueues.isEmpty();
        this.missingQueues = missingQueues;
        this.invalidQueues = invalidQueues;
        
        if (queuesHealthy)
            log.trace("RabbitMQ Queues are healthy");
        else
            log.trace("RabbitMQ Queues are unhealthy");
    }
    
    /**
     * Checks that the desired bindings are present, and correctly configured.
     */
    private void bindingsHealthCheck() {
        List<BindingInfo> missingBindings = new ArrayList<>();
        List<InvalidPairing<BindingInfo>> invalidBindings = new ArrayList<>();
        
        List<BindingInfo> returnedBindings = null;
        try {
            returnedBindings = rabbitClient.getBindings();
        } catch (Exception e) {
            log.trace("Unable to get RabbitMQ binding info.", e);
        }
        
        if (returnedBindings != null) {
            Map<String,BindingInfo> detectedBindings = returnedBindings.stream()
                            .collect(Collectors.toMap(b -> b.getSource() + "_" + b.getDestination(), b -> b));
            for (BindingInfo binding : bindings) {
                String bindingKey = binding.getSource() + "_" + binding.getDestination();
                if (!detectedBindings.containsKey(bindingKey))
                    missingBindings.add(binding);
                else if (!isBindingValid(binding, detectedBindings.get(bindingKey)))
                    invalidBindings.add(new InvalidPairing<>(binding, detectedBindings.get(bindingKey)));
            }
        } else {
            missingBindings.addAll(bindings);
        }
        
        bindingsHealthy = missingBindings.isEmpty() && invalidBindings.isEmpty();
        this.missingBindings = missingBindings;
        this.invalidBindings = invalidBindings;
        
        if (bindingsHealthy)
            log.trace("RabbitMQ Bindings are healthy");
        else
            log.trace("RabbitMQ Bindings are unhealthy");
    }
    
    /**
     * Attempts to restore the desired RabbitMQ configuration to the cluster.
     * <p>
     * Missing exchanges, queues, and bindings will be created. Invalid exchanges, and bindings will always be recreated. Invalid queues will only be recreated
     * if they are empty.
     */
    @Override
    public void recover() {
        initRabbitClient();
        
        if (rabbitClient != null && rabbitHealthProperties.isAttemptRecovery() && !isHealthy()) {
            log.trace("RabbitMQ Recovery - Started");
            
            // create missing exchanges
            if (rabbitHealthProperties.isFixMissing()) {
                for (ExchangeInfo exchange : missingExchanges) {
                    log.trace("Creating missing exchange: [{}]", exchange);
                    try {
                        rabbitClient.declareExchange(DEFAULT_VHOST, exchange.getName(), exchange);
                    } catch (Exception e) {
                        log.trace("Unable to create missing exchange", e);
                    }
                }
            }
            
            // recreate invalid exchanges
            if (rabbitHealthProperties.isFixInvalid()) {
                for (InvalidPairing<ExchangeInfo> exchanges : invalidExchanges) {
                    log.trace("Fixing invalid exchange: [" + exchanges.desired + "]");
                    try {
                        rabbitClient.deleteExchange(DEFAULT_VHOST, exchanges.detected.getName());
                        rabbitClient.declareExchange(DEFAULT_VHOST, exchanges.desired.getName(), exchanges.desired);
                    } catch (Exception e) {
                        log.trace("Unable to recreate invalid exchange", e);
                    }
                }
            }
            
            // create missing queues
            if (rabbitHealthProperties.isFixMissing()) {
                for (QueueInfo queue : missingQueues) {
                    log.trace("Creating missing queue: [" + queue + "]");
                    try {
                        rabbitClient.declareQueue(DEFAULT_VHOST, queue.getName(), queue);
                    } catch (Exception e) {
                        log.trace("Unable to create missing queue", e);
                    }
                }
            }
            
            // recreate invalid queues
            if (rabbitHealthProperties.isFixInvalid()) {
                for (InvalidPairing<QueueInfo> queues : invalidQueues) {
                    log.trace("Fixing invalid queue: [{}]", queues.desired);
                    
                    QueueInfo queueInfo = null;
                    try {
                        queueInfo = rabbitClient.getQueue(DEFAULT_VHOST, queues.detected.getName());
                    } catch (Exception e) {
                        log.trace("Unable to get queue info", e);
                    }
                    
                    if (queueInfo != null) {
                        if (queueInfo.getTotalMessages() == 0) {
                            try {
                                rabbitClient.deleteQueue(DEFAULT_VHOST, queues.detected.getName());
                                rabbitClient.declareQueue(DEFAULT_VHOST, queues.desired.getName(), queues.desired);
                            } catch (Exception e) {
                                log.trace("Unable to recreate invalid queue", e);
                            }
                        } else
                            log.warn("Cannot fix invalid queue [{}] containing {} messages", queueInfo.getName(), queueInfo.getTotalMessages());
                    }
                }
            }
            
            // create missing bindings
            if (rabbitHealthProperties.isFixMissing()) {
                for (BindingInfo binding : missingBindings) {
                    log.trace("Creating missing binding: [{}]", binding);
                    try {
                        if (binding.getDestinationType().equalsIgnoreCase(Binding.DestinationType.EXCHANGE.name()))
                            rabbitClient.bindExchange(DEFAULT_VHOST, binding.getDestination(), binding.getSource(), binding.getRoutingKey(),
                                            binding.getArguments());
                        else if (binding.getDestinationType().equalsIgnoreCase(Binding.DestinationType.QUEUE.name()))
                            rabbitClient.bindQueue(DEFAULT_VHOST, binding.getDestination(), binding.getSource(), binding.getRoutingKey(),
                                            binding.getArguments());
                    } catch (Exception e) {
                        log.trace("Unable to create missing binding", e);
                    }
                }
            }
            
            // recreate invalid bindings
            if (rabbitHealthProperties.isFixInvalid()) {
                for (InvalidPairing<BindingInfo> bindings : invalidBindings) {
                    log.trace("Fixing invalid binding: [{}]", bindings.desired);
                    try {
                        // remove invalid bindings
                        if (bindings.detected.getDestinationType().equalsIgnoreCase(Binding.DestinationType.EXCHANGE.name()))
                            rabbitClient.unbindExchange(DEFAULT_VHOST, bindings.detected.getDestination(), bindings.detected.getSource(),
                                            bindings.detected.getRoutingKey());
                        else if (bindings.desired.getDestinationType().equalsIgnoreCase(Binding.DestinationType.QUEUE.name()))
                            rabbitClient.unbindQueue(DEFAULT_VHOST, bindings.detected.getDestination(), bindings.detected.getSource(),
                                            bindings.detected.getRoutingKey());
                        
                        // add correct bindings
                        if (bindings.desired.getDestinationType().equalsIgnoreCase(Binding.DestinationType.EXCHANGE.name()))
                            rabbitClient.bindExchange(DEFAULT_VHOST, bindings.desired.getDestination(), bindings.desired.getSource(),
                                            bindings.desired.getRoutingKey(), bindings.desired.getArguments());
                        else if (bindings.desired.getDestinationType().equalsIgnoreCase(Binding.DestinationType.QUEUE.name()))
                            rabbitClient.bindQueue(DEFAULT_VHOST, bindings.desired.getDestination(), bindings.desired.getSource(),
                                            bindings.desired.getRoutingKey(), bindings.desired.getArguments());
                    } catch (Exception e) {
                        log.trace("Unable to recreate invalid binding", e);
                    }
                }
            }
            log.trace("RabbitMQ Recovery - Complete");
        }
    }
    
    /**
     * Used to determine whether RabbitMQ is healthy.
     * <p>
     * This method is synchronized with the runHealthCheck method to ensure that the health check is given time to complete before deciding whether to accept an
     * audit request via the {@link AuditController}
     *
     * @return true if RabbitMQ is healthy, false if RabbitMQ is unhealthy
     */
    @Override
    public boolean isHealthy() {
        synchronized (this) {
            return rabbitClient != null && clusterHealthy && exchangesHealthy && queuesHealthy && bindingsHealthy;
        }
    }
    
    private void updateOutageStats(RabbitOutageStats outageStats) {
        if (numNodesMissing > 0)
            outageStats.setNumNodesMissing(Math.max(outageStats.getNumNodesMissing(), numNodesMissing));
        
        missingExchanges.forEach(e -> outageStats.getMissingExchanges().add(e.getName()));
        missingQueues.forEach(q -> outageStats.getMissingQueues().add(q.getName()));
        missingBindings.forEach(b -> outageStats.getMissingBindings().put(b.getSource(), b.getDestination()));
        
        invalidExchanges.forEach(e -> outageStats.getInvalidExchanges().add(e.desired.getName()));
        invalidQueues.forEach(q -> outageStats.getInvalidQueues().add(q.desired.getName()));
        invalidBindings.forEach(b -> outageStats.getInvalidBindings().put(b.desired.getSource(), b.desired.getDestination()));
    }
    
    /**
     * Collects a list of stats for outages experienced by the audit service.
     *
     * @return a list of RabbitMQ outages experienced by the audit service
     */
    @Override
    public List<Map<String,Object>> getOutageStats() {
        if (!outageStats.isEmpty())
            return outageStats.stream().map(RabbitOutageStats::getOutageParams).collect(Collectors.toList());
        return Collections.emptyList();
    }
    
    /**
     * Provides information for the audit service health endpoint.
     * <p>
     * If available, the health status will also include information about the number of messages contained in each queue.
     *
     * @return health status and information for the audit service
     */
    @Override
    public Health health() {
        Map<String,Object> queueSizeStats = null;
        if (rabbitHealthProperties.isIncludeQueueSizeStats()) {
            queueSizeStats = new LinkedHashMap<>();
            for (QueueInfo queue : queues) {
                QueueInfo queueInfo = null;
                if (rabbitClient != null) {
                    try {
                        queueInfo = rabbitClient.getQueue(DEFAULT_VHOST, queue.getName());
                    } catch (Exception e) {
                        log.trace("Unable to get queue info", e);
                    }
                }
                
                if (queueInfo != null) {
                    Map<String,Object> sizeStats = new LinkedHashMap<>();
                    sizeStats.put("ready", queueInfo.getMessagesReady());
                    sizeStats.put("unacknowledged", queueInfo.getMessagesUnacknowledged());
                    sizeStats.put("total", queueInfo.getTotalMessages());
                    queueSizeStats.put(queue.getName(), sizeStats);
                } else {
                    queueSizeStats.put(queue.getName(), "unknown");
                }
            }
        }
        
        if (!isHealthy()) {
            Health.Builder builder = Health.status(RABBITMQ_UNHEALTHY);
            if (currentOutageStats != null)
                builder.withDetail("outage", currentOutageStats.getOutageParams());
            if (queueSizeStats != null)
                builder.withDetail("queueStats", queueSizeStats);
            return builder.build();
        } else {
            Health.Builder builder = Health.up();
            if (queueSizeStats != null)
                builder.withDetail("queueStats", queueSizeStats);
            return builder.build();
        }
    }
    
    private boolean isExchangeValid(ExchangeInfo desiredExchange, ExchangeInfo detectedExchange) {
        return desiredExchange.getName().equals(detectedExchange.getName()) && desiredExchange.getType().equals(detectedExchange.getType())
                        && desiredExchange.isDurable() == detectedExchange.isDurable() && desiredExchange.isAutoDelete() == detectedExchange.isAutoDelete()
                        && desiredExchange.isInternal() == detectedExchange.isInternal();
    }
    
    private boolean isQueueValid(QueueInfo desiredQueue, QueueInfo detectedQueue) {
        return desiredQueue.getName().equals(detectedQueue.getName()) && desiredQueue.isDurable() == detectedQueue.isDurable()
                        && desiredQueue.isExclusive() == detectedQueue.isExclusive() && desiredQueue.isAutoDelete() == detectedQueue.isAutoDelete()
                        && areArgumentsValid(desiredQueue.getArguments(), detectedQueue.getArguments());
    }
    
    private boolean isBindingValid(BindingInfo desiredBinding, BindingInfo detectedBinding) {
        return desiredBinding.getDestination().equals(detectedBinding.getDestination())
                        && desiredBinding.getDestinationType().equals(detectedBinding.getDestinationType())
                        && desiredBinding.getSource().equals(detectedBinding.getSource())
                        && desiredBinding.getRoutingKey().equals(detectedBinding.getRoutingKey())
                        && areArgumentsValid(desiredBinding.getArguments(), detectedBinding.getArguments());
    }
    
    private boolean areArgumentsValid(Map<String,Object> desiredArguments, Map<String,Object> detectedArguments) {
        if (desiredArguments != null && detectedArguments != null)
            for (String arg : desiredArguments.keySet())
                if (!detectedArguments.containsKey(arg) || !detectedArguments.get(arg).equals(desiredArguments.get(arg)))
                    return false;
        return true;
    }
}
