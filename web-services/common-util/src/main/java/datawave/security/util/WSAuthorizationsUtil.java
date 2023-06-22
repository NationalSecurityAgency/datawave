package datawave.security.util;

import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.security.authorization.DatawavePrincipal;

/**
 * Several of these methods refer to different types of principals:
 *
 * overallPrincipal: This is the principal that represents all of the possible auths that the calling user is allowed to have. The requested auths must always
 * be a subset of these. This will be a combination of the local principal (the one for this webserver) and the principal for and remote user operations that
 * may be applicable. queryPrincipal: This is the principal that represents all of the auths that are valid for the query being made. The requested auths will
 * be reduced by this set of auths.
 */
public class WSAuthorizationsUtil extends AuthorizationsUtil {

    /**
     * Merge principals. This can be used to create a composite view of a principal when including remote systems
     *
     * @param principals
     * @return The merge principal
     */
    public static DatawavePrincipal mergePrincipals(DatawavePrincipal... principals) {
        return datawave.microservice.authorization.util.AuthorizationsUtil.mergeProxiedUserDetails(principals);
    }
}
