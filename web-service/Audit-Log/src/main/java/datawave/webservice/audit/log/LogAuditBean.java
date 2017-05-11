package datawave.webservice.audit.log;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;

import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.apache.log4j.Logger;

@LocalBean
@Stateless
@RolesAllowed({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
public class LogAuditBean implements Auditor {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Override
    public void audit(AuditParameters am) throws Exception {
        if (!am.getAuditType().equals(AuditType.NONE)) {
            log.info(am.toString());
        }
    }
    
}
