package datawave.webservice.util;

import org.apache.log4j.Logger;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ByteArraySet;

public class MergeAuthorizationsHelper {
    private static Logger log = Logger.getLogger(MergeAuthorizationsHelper.class);
    
    public static Authorizations mergeAuthorizations(Connector connector, String cbUserName, Authorizations userAuthorizations) {
        Authorizations mergedAuthorizations = null;
        Authorizations cbUserAuthorizations;
        try {
            cbUserAuthorizations = connector.securityOperations().getUserAuthorizations(cbUserName);
            mergedAuthorizations = mergeAuthorizations(cbUserAuthorizations, userAuthorizations);
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.error(e.getMessage(), e);
        }
        
        return mergedAuthorizations;
    }
    
    public static Authorizations mergeAuthorizations(Authorizations auth1, Authorizations auth2) {
        ByteArraySet newAuthList = new ByteArraySet();
        ByteArraySet removedAuthList = new ByteArraySet();
        for (byte[] b : auth1) {
            if (auth2.contains(b)) {
                newAuthList.add(b);
            } else {
                if (log.isTraceEnabled()) {
                    removedAuthList.add(b);
                }
            }
        }
        Authorizations mergedAuthorizations = new Authorizations(newAuthList);
        
        if (log.isTraceEnabled()) {
            Authorizations removedAuths = new Authorizations(removedAuthList);
            log.trace("\nAuth1: " + auth1 + "\ncbAuth2: " + auth2 + "\nUsing authorizations: " + mergedAuthorizations + "\nMasking authorizations: "
                            + removedAuths);
        }
        return mergedAuthorizations;
    }
}
