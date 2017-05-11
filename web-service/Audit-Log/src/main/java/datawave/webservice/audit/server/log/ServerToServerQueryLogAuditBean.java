package datawave.webservice.audit.server.log;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;

import datawave.security.util.DnUtils;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.apache.log4j.Logger;

@LocalBean
@Stateless
@RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
public class ServerToServerQueryLogAuditBean implements Auditor {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Override
    public void audit(AuditParameters msg) throws Exception {
        
        if (!msg.getAuditType().equals(AuditType.NONE)) {
            if (DnUtils.isServerDN(msg.getUserDn())) {
                log.info(msg.toString());
            }
        }
    }
    
}
