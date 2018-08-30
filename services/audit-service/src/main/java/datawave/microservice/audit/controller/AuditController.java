package datawave.microservice.audit.controller;

import datawave.microservice.audit.common.AuditMessage;
import datawave.microservice.audit.config.AuditServiceConfig;
import datawave.webservice.common.audit.AuditParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;

@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class AuditController {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private AuditParameters restAuditParams;
    
    private MessageChannel messageChannel;
    
    public AuditController(@Qualifier("restAuditParams") AuditParameters restAuditParams,
                    @Qualifier(AuditServiceConfig.AuditSourceBinding.NAME) MessageChannel messageChannel) {
        this.restAuditParams = restAuditParams;
        this.messageChannel = messageChannel;
    }
    
    private void sendMessage(AuditParameters parameters) {
        messageChannel.send(MessageBuilder.withPayload(AuditMessage.fromParams(parameters)).build());
    }
    
    @RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/audit", method = RequestMethod.POST)
    public String audit(@RequestParam MultiValueMap<String,String> parameters) {
        log.trace("Received audit request with parameters {}", parameters);
        restAuditParams.clear();
        restAuditParams.validate(parameters);
        sendMessage(restAuditParams);
        return restAuditParams.getAuditId();
    }
}
