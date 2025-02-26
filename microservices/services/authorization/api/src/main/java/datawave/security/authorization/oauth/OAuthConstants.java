package datawave.security.authorization.oauth;

public class OAuthConstants {
    
    /*
     * Use this grant type in the token API call after receiving an authorization code from the authorize API call
     */
    public static final String GRANT_AUTHORIZATION_CODE = "authorization_code";
    
    /*
     * Use this grant type in the token API call to get a new token using the (longer lived) refresh token which is passed as a parameter
     */
    public static final String GRANT_REFRESH_TOKEN = "refresh_token";
    
    /*
     * Expected response type of code for the authorization operation. Required by specification and the only allowed value for code flow
     */
    public static final String RESPONSE_TYPE_CODE = "code";
    
}
