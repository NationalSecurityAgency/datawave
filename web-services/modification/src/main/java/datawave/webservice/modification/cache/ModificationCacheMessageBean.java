package datawave.webservice.modification.cache;

import datawave.configuration.spring.SpringBean;
import datawave.modification.MutableMetadataHandler;
import datawave.modification.configuration.ModificationConfiguration;
import datawave.modification.configuration.ModificationServiceConfiguration;
import org.apache.log4j.Logger;

import javax.annotation.security.RunAs;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

@RunAs("InternalUser")
@MessageDriven(name = "ModificationCacheMessageBean",
                activationConfig = {@ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Topic"),
                        @ActivationConfigProperty(propertyName = "destination", propertyValue = "topic/AccumuloTableCache"),
                        @ActivationConfigProperty(propertyName = "useLocalTx", propertyValue = "true"),
                        @ActivationConfigProperty(propertyName = "subscriptionDurability", propertyValue = "Durable"),
                        @ActivationConfigProperty(propertyName = "subscriptionName", propertyValue = "ModificationCacheMessageBean"),
                        @ActivationConfigProperty(propertyName = "user", propertyValue = "${dw.hornetq.system.userName}"),
                        @ActivationConfigProperty(propertyName = "password", propertyValue = "${dw.hornetq.system.password}"),
                        @ActivationConfigProperty(propertyName = "maxSession", propertyValue = "${dw.modification.cache.mdb.pool.size}")})
@TransactionManagement(value = TransactionManagementType.CONTAINER)
@TransactionAttribute(value = TransactionAttributeType.NOT_SUPPORTED)
public class ModificationCacheMessageBean implements MessageListener {
    private Logger log = Logger.getLogger(this.getClass());

    @Inject
    private ModificationCacheBean modificationCacheBean;

    @Inject
    @SpringBean(refreshable = true)
    private ModificationConfiguration modificationConfiguration;

    @Override
    public void onMessage(Message message) {
        String tableName;
        try {
            tableName = ((TextMessage) message).getText();
            log.info("tablename = " + tableName);
            ModificationServiceConfiguration modificationServiceConfiguration = modificationConfiguration.getConfiguration("MutableMetadataService");
            if (modificationServiceConfiguration instanceof MutableMetadataHandler) {
                MutableMetadataHandler mutableMetadataHandler = (MutableMetadataHandler) modificationServiceConfiguration;
                String metadataTableName = mutableMetadataHandler.getMetadataTableName();
                if (metadataTableName != null && metadataTableName.equals(tableName)) {
                    if (log.isTraceEnabled()) {
                        log.info("cache was:" + modificationCacheBean.listMutableFields());
                    }
                    modificationCacheBean.reloadMutableFieldCache();
                    if (log.isTraceEnabled()) {
                        log.info("reloaded ModificationCache for " + tableName + ", after reload, cache is " + modificationCacheBean.listMutableFields());
                    }
                }
            }
        } catch (JMSException e) {
            log.warn("Could not reload ModificationCache for message :" + message);
        }
    }
}
