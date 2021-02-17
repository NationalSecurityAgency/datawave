package datawave.microservice.common.storage;

import datawave.microservice.common.storage.config.QueryStorageProperties;
import org.apache.log4j.Logger;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Service
public class QueryQueueManager {
    private static final Logger log = Logger.getLogger(QueryQueueManager.class);
    
    @Autowired
    private RabbitAdmin rabbitAdmin;
    
    @Autowired
    private RabbitTemplate rabbitTemplate;
    
    @Autowired
    private RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry;
    
    @Autowired
    private ConnectionFactory factory;
    
    // A mapping of query pools to routing keys
    private Map<QueryPool,String> exchanges = new HashMap<>();
    
    @Autowired
    QueryStorageProperties properties;
    
    /**
     * Passes task notifications to the messaging infrastructure.
     *
     * @param taskNotification
     *            The task notification to be sent
     */
    public void sendMessage(QueryTaskNotification taskNotification) {
        TaskKey taskKey = taskNotification.getTaskKey();
        QueryPool queryPool = taskKey.getQueryPool();
        if (exchanges.get(queryPool) == null) {
            synchronized (exchanges) {
                if (exchanges.get(queryPool) == null) {
                    TopicExchange exchange = new TopicExchange(queryPool.getName(), properties.isSynchStorage(), false);
                    Queue queue = new Queue(queryPool.getName(), properties.isSynchStorage(), false, false);
                    Binding binding = BindingBuilder.bind(queue).to(exchange).with(taskKey.toRoutingKey());
                    // Binding binding = new Binding(queryPool.getName(), Binding.DestinationType.QUEUE, queryPool.getName(), taskKey.toRoutingKey(), null);
                    rabbitAdmin.declareExchange(exchange);
                    rabbitAdmin.declareQueue(queue);
                    rabbitAdmin.declareBinding(binding);
                    exchanges.put(queryPool, taskKey.toRoutingKey());
                }
            }
        }
        
        rabbitTemplate.convertAndSend(taskNotification.getTaskKey().toKey(), taskNotification);
    }
    
    public AbstractMessageListenerContainer createListener(String listenerId) {
        log.info("Creating listener for " + listenerId);
        AbstractMessageListenerContainer listener = this.getMessageListenerContainerById(listenerId);
        if (listener == null) {
            // TODO
        }
        return listener;
    }
    
    public void addQueueToListener(String listenerId, String queueName) {
        log.info("adding queue : " + queueName + " to listener with id : " + listenerId);
        if (!checkQueueExistOnListener(listenerId, queueName)) {
            this.getMessageListenerContainerById(listenerId).addQueueNames(queueName);
            log.info("queue ");
        } else {
            log.info("given queue name : " + queueName + " not exist on given listener id : " + listenerId);
        }
    }
    
    public void removeQueueFromListener(String listenerId, String queueName) {
        log.info("removing queue : " + queueName + " from listener : " + listenerId);
        if (checkQueueExistOnListener(listenerId, queueName)) {
            this.getMessageListenerContainerById(listenerId).removeQueueNames(queueName);
            log.info("deleting queue from rabbit management");
            this.rabbitAdmin.deleteQueue(queueName);
        } else {
            log.info("given queue name : " + queueName + " not exist on given listener id : " + listenerId);
        }
    }
    
    public Boolean checkQueueExistOnListener(String listenerId, String queueName) {
        try {
            log.info("checking queueName : " + queueName + " exist on listener id : " + listenerId);
            log.info("getting queueNames");
            AbstractMessageListenerContainer container = this.getMessageListenerContainerById(listenerId);
            String[] queueNames = (container == null ? null : container.getQueueNames());
            log.info("queueNames : " + Arrays.toString(queueNames));
            if (queueNames != null) {
                log.info("checking " + queueName + " exist on active queues");
                for (String name : queueNames) {
                    log.info("name : " + name + " with checking name : " + queueName);
                    if (name.equals(queueName)) {
                        log.info("queue name exist on listener, returning true");
                        return Boolean.TRUE;
                    }
                }
                return Boolean.FALSE;
            } else {
                log.info("there is no queue exist on listener");
                return Boolean.FALSE;
            }
        } catch (Exception e) {
            log.error("Error on checking queue exist on listener", e);
            return Boolean.FALSE;
        }
    }
    
    private AbstractMessageListenerContainer getMessageListenerContainerById(String listenerId) {
        log.info("getting message listener container by id : " + listenerId);
        return ((AbstractMessageListenerContainer) this.rabbitListenerEndpointRegistry.getListenerContainer(listenerId));
    }
}
