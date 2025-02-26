package datawave.security.authorization;

import java.util.Collection;
import java.util.List;

public interface ProxiedUserDetails {
    
    Collection<? extends DatawaveUser> getProxiedUsers();
    
    String getName();
    
    DatawaveUser getPrimaryUser();
    
    Collection<? extends Collection<String>> getAuthorizations();
    
    String[] getDNs();
    
    String getShortName();
    
    List<String> getProxyServers();
    
    <T extends ProxiedUserDetails> T newInstance(List<DatawaveUser> proxiedUsers);
}
