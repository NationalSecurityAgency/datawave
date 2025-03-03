package datawave.microservice.query.messaging.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;

import datawave.microservice.query.messaging.ClaimCheck;
import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.config.MessagingProperties;

public class RabbitMQQueryResultsManager implements QueryResultsManager {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String RABBITMQ = "rabbitmq";
    
    static final String QUERY_RESULTS_EXCHANGE = "queryResults";
    static final String QUERY_QUEUE_PREFIX = QUERY_RESULTS_EXCHANGE + ".";
    
    private final MessagingProperties messagingProperties;
    private final RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry;
    private final CachingConnectionFactory connectionFactory;
    private final ClaimCheck claimCheck;
    
    private final RabbitAdmin rabbitAdmin;
    private final DirectRabbitListenerContainerFactory listenerContainerFactory;
    
    public RabbitMQQueryResultsManager(MessagingProperties messagingProperties, RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry,
                    CachingConnectionFactory cachingConnectionFactory, ClaimCheck claimCheck) {
        this.messagingProperties = messagingProperties;
        this.rabbitListenerEndpointRegistry = rabbitListenerEndpointRegistry;
        this.connectionFactory = cachingConnectionFactory;
        this.claimCheck = claimCheck;
        
        rabbitAdmin = new RabbitAdmin(cachingConnectionFactory);
        listenerContainerFactory = new DirectRabbitListenerContainerFactory();
        listenerContainerFactory.setConnectionFactory(cachingConnectionFactory);
        listenerContainerFactory.setConsumersPerQueue(messagingProperties.getConcurrency());
    }
    
    /**
     * Create a listener for a specified listener id
     *
     * @param listenerId
     *            The listener id
     * @param queryId
     *            The query ID to listen to
     * @return a query result listener
     */
    @Override
    public QueryResultsListener createListener(String listenerId, String queryId) {
        ensureQueueCreated(queryId);
        return new RabbitMQQueryResultsListener(listenerContainerFactory, rabbitListenerEndpointRegistry, claimCheck, listenerId, queryId);
    }
    
    /**
     * Create a publisher for a specific query id.
     * 
     * @param queryId
     *            The query ID to publish to
     * @return a query result publisher
     */
    @Override
    public QueryResultsPublisher createPublisher(String queryId) {
        ensureQueueCreated(queryId);
        return new RabbitMQQueryResultsPublisher(messagingProperties.getRabbitmq(), new RabbitTemplate(connectionFactory), claimCheck, queryId);
    }
    
    /**
     * Ensure a queue is created for a given pool
     *
     * @param queryId
     *            The query id to use to create the exchange and queue
     */
    private void ensureQueueCreated(String queryId) {
        QueueInformation queueInfo = rabbitAdmin.getQueueInfo(queryId);
        if (queueInfo == null) {
            if (log.isInfoEnabled()) {
                log.debug("Creating exchange/queue " + queryId);
            }
            
            TopicExchange exchange = new TopicExchange(QUERY_RESULTS_EXCHANGE, messagingProperties.getRabbitmq().isDurable(), false);
            Queue queue = new Queue(QUERY_QUEUE_PREFIX + queryId, messagingProperties.getRabbitmq().isDurable(), false, false);
            Binding binding = BindingBuilder.bind(queue).to(exchange).with(queryId);
            
            rabbitAdmin.declareExchange(exchange);
            rabbitAdmin.declareQueue(queue);
            rabbitAdmin.declareBinding(binding);
        }
    }
    
    @Override
    public void deleteQuery(String queryId) {
        try {
            if (rabbitAdmin.getQueueInfo(QUERY_QUEUE_PREFIX + queryId) != null) {
                rabbitAdmin.deleteQueue(QUERY_QUEUE_PREFIX + queryId);
            }
        } catch (AmqpIOException e) {
            log.error("Failed to delete queue " + queryId, e);
        }
        
        if (claimCheck != null) {
            claimCheck.delete(queryId);
        }
    }
    
    @Override
    public void emptyQuery(String queryId) {
        try {
            rabbitAdmin.purgeQueue(QUERY_QUEUE_PREFIX + queryId);
        } catch (AmqpIOException e) {
            // log an continue
            log.error("Failed to empty queue " + queryId, e);
        }
        
        if (claimCheck != null) {
            claimCheck.empty(queryId);
        }
    }
    
    @Override
    public int getNumResultsRemaining(String queryId) {
        QueueInformation queueInfo = rabbitAdmin.getQueueInfo(QUERY_QUEUE_PREFIX + queryId);
        if (queueInfo != null) {
            return queueInfo.getMessageCount();
        }
        return 0;
    }
}
