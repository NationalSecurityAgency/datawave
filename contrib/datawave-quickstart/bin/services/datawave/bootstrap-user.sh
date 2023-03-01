
## Sourced by bootstrap.sh

# Set to anything other than false to enable the remote authorization microservice (i.e., RemoteDatawaveUserService class).
# If enabled, it is assumed that you have properly configured and launched the necessary microservices and that you have
# configured users and PKI materials accordingly. If 'false', then the TestDatawaveUserService class will be configured
# automatically here with the user details defined below

DW_ENABLE_REMOTE_AUTH_SERVICE=${DW_ENABLE_REMOTE_AUTH_SERVICE:-false}

# Comma-delimited list of DataWave Web application roles to grant the test user. This list will be used to auto-generate
# the appropriate TestDatawaveUserServiceConfiguration.xml entries for the web tier config. Override the default list as
# needed for whatever your user-based testing requires...

DW_DATAWAVE_USER_ROLES="${DW_DATAWAVE_USER_ROLES:-AuthorizedUser,Administrator,JBossAdministrator}"
DW_DATAWAVE_SERVER_ROLES="${DW_DATAWAVE_SERVER_ROLES:-AuthorizedServer}"

# Comma-delimited list of Accumulo authorizations to grant the DataWave Web test user. This list will be used to
# auto-generate the appropriate TestDatawaveUserServiceConfiguration.xml entries for the web tier config. Override the
# default list as needed for whatever your user-based testing requires. Defaults to DW_DATAWAVE_ACCUMULO_AUTHS, since
# you'll most likely want your test user to be able to see all data in Accumulo.

# Note that users may opt to query with only a subset of the auths in this list, on a per-query basis. E.g.,...
# datawaveQuery --auths BAR,PUBLIC --logic EdgeQuery --syntax JEXL --query "SOURCE == 'kevin bacon' && TYPE == 'TV_COSTARS'"

DW_DATAWAVE_USER_AUTHS="${DW_DATAWAVE_USER_AUTHS:-${DW_DATAWAVE_ACCUMULO_AUTHS}}"
DW_DATAWAVE_SERVER_AUTHS="${DW_DATAWAVE_SERVER_AUTHS:-${DW_DATAWAVE_ACCUMULO_AUTHS}}"

# Test user's client cert DN's (subject and issuer). Defaults to DN's from testUser.p12 cert. These will be used to
# auto-generate the appropriate TestDatawaveUserServiceConfiguration.xml entries for the web tier config...

DW_DATAWAVE_USER_DN="${DW_DATAWAVE_USER_DN:-cn=test a. user, ou=example developers, o=example corp, c=us}"
DW_DATAWAVE_USER_ISSUER_DN="${DW_DATAWAVE_USER_ISSUER_DN:-cn=example corp ca, o=example corp, c=us}"

DW_DATAWAVE_SERVER_DN="${DW_DATAWAVE_SERVER_DN:-cn=testserver.example.com, ou=servers, o=example corp, c=us}"
DW_DATAWAVE_SERVER_ISSUER_DN="${DW_DATAWAVE_SERVER_ISSUER_DN:-cn=example corp ca, o=example corp, c=us}"

function generateTestDatawaveUserServiceConfig() {

   # The goal here is to write to BUILD_PROPERTIES_FILE all the props required to configure TestDatawaveUserService
   # for the 'testUser.p12' cert/user, or whichever cert/user is currently configured.

   # TestDatawaveUserService implements datawave.microservice.authorization.AuthorizationService and retrieves DataWave
   # user roles/auths from a local file, i.e., Spring bean context 'TestDatawaveUserServiceConfiguration.xml', rather
   # than from an external service

   [ -z "${BUILD_PROPERTIES_FILE}" ] && error "BUILD_PROPERTIES_FILE variable not defined" && return 1
   [ ! -f ${BUILD_PROPERTIES_FILE} ] && error "File does not exist: ${BUILD_PROPERTIES_FILE}" && return 1

   if [ "${DW_ENABLE_REMOTE_AUTH_SERVICE}" != false ] ; then
      echo "security.use.testauthservice=false" >> ${BUILD_PROPERTIES_FILE}
      echo "security.use.remoteauthservice=true" >> ${BUILD_PROPERTIES_FILE}
      return 0
   fi

   echo "security.use.testauthservice=true" >> ${BUILD_PROPERTIES_FILE}
   echo "security.testauthservice.context.entry=<value>classpath*:datawave/security/TestDatawaveUserServiceConfiguration.xml</value>" >> ${BUILD_PROPERTIES_FILE}

   # Format auth and role lists as json array elements

   OLD_IFS="$IFS"
   IFS=","
   local userAuths=( ${DW_DATAWAVE_USER_AUTHS} )
   local userRoles=( ${DW_DATAWAVE_USER_ROLES} )
   IFS="$OLD_IFS"

   local authEntries="\"${userAuths[0]}\""
   for (( i=1 ; i<${#userAuths[@]} ; i++ )) ; do
      authEntries="${authEntries}, \"${userAuths[i]}\""
   done

   local roleEntries="\"${userRoles[0]}\""
   for (( i=1 ; i<${#userRoles[@]} ; i++ )) ; do
      roleEntries="${roleEntries}, \"${userRoles[i]}\""
   done

   echo "security.testauthservice.users= \\"                                 >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        <value><![CDATA[ \\"                                     >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        { \\"                                                    >> ${BUILD_PROPERTIES_FILE}
   echo "\\n           \"dn\": { \\"                                         >> ${BUILD_PROPERTIES_FILE}
   echo "\\n             \"subjectDN\": \"${DW_DATAWAVE_USER_DN}\", \\"      >> ${BUILD_PROPERTIES_FILE}
   echo "\\n             \"issuerDN\": \"${DW_DATAWAVE_USER_ISSUER_DN}\" \\" >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        }, \\"                                                   >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        \"userType\": \"USER\",\\"                               >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        \"auths\": [ ${authEntries} ], \\"                       >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        \"roles\": [ ${roleEntries} ], \\"                       >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        \"creationTime\": -1, \\"                                >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        \"expirationTime\": -1 \\"                               >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        } \\"                                                    >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        ]]></value> \\"                                          >> ${BUILD_PROPERTIES_FILE}

   OLD_IFS="$IFS"
   IFS=","
   local serverAuths=( ${DW_DATAWAVE_SERVER_AUTHS} )
   local serverRoles=( ${DW_DATAWAVE_SERVER_ROLES} )
   IFS="$OLD_IFS"

   local authEntries="\"${serverAuths[0]}\""
   for (( i=1 ; i<${#serverAuths[@]} ; i++ )) ; do
      authEntries="${authEntries}, \"${serverAuths[i]}\""
   done

   local roleEntries="\"${serverRoles[0]}\""
   for (( i=1 ; i<${#serverRoles[@]} ; i++ )) ; do
      roleEntries="${roleEntries}, \"${serverRoles[i]}\""
   done

   echo "\\n        <value><![CDATA[ \\"                                            >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        { \\"                                                           >> ${BUILD_PROPERTIES_FILE}
   echo "\\n            \"dn\": { \\"                                               >> ${BUILD_PROPERTIES_FILE}
   echo "\\n                 \"subjectDN\": \"${DW_DATAWAVE_SERVER_DN}\", \\"       >> ${BUILD_PROPERTIES_FILE}
   echo "\\n                 \"issuerDN\": \"${DW_DATAWAVE_SERVER_ISSUER_DN}\" \\"  >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        }, \\"                                                          >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        \"userType\": \"SERVER\",\\"                                    >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        \"auths\": [ ${authEntries} ], \\"                              >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        \"roles\": [ ${roleEntries} ], \\"                              >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        \"creationTime\": -1, \\"                                       >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        \"expirationTime\": -1 \\"                                      >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        } \\"                                                           >> ${BUILD_PROPERTIES_FILE}
   echo "\\n        ]]></value>"                                                    >> ${BUILD_PROPERTIES_FILE}


   # add the server dn as a trusted entity
   echo "trusted.proxied.entities=$DW_DATAWAVE_SERVER_DN" >> ${BUILD_PROPERTIES_FILE}
}
