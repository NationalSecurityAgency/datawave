package datawave.microservice.audit.controller;

import datawave.microservice.audit.common.AuditParameters;
import datawave.microservice.audit.config.AuditProperties;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.annotation.security.RolesAllowed;

@RestController
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public class AuditController {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private AuditProperties auditProperties;
    
    private AuditParameters auditParameters;
    
    private RabbitTemplate rabbitTemplate;
    
    public AuditController(AuditProperties auditProperties, AuditParameters auditParameters, RabbitTemplate rabbitTemplate) {
        this.auditProperties = auditProperties;
        this.auditParameters = auditParameters;
        this.rabbitTemplate = rabbitTemplate;
        init();
    }
    
    private void init() {
        rabbitTemplate.setExchange(auditProperties.getExchangeName());
    }
    
    protected void sendMessage(AuditParameters parameters) throws Exception {
        try {
            rabbitTemplate.convertAndSend(parameters.toMap());
        } catch (Exception e) {
            log.error("Error sending audit message to " + auditProperties.getExchangeName() + ": " + parameters.toString(), e);
            throw e;
        }
    }
    
    @RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/audit", method = RequestMethod.POST)
    public VoidResponse audit(@RequestParam MultiValueMap<String,String> parameters) {
        VoidResponse response = new VoidResponse();
        try {
            auditParameters.clear();
            auditParameters.validate(parameters);
            sendMessage(auditParameters);
            return response;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.AUDITING_ERROR, e);
            log.error(qe.toString());
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
}
