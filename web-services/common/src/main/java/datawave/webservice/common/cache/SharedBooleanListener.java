/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datawave.webservice.common.cache;

import org.apache.curator.framework.state.ConnectionStateListener;

public interface SharedBooleanListener extends ConnectionStateListener {
    
    void booleanHasChanged(SharedBooleanReader var1, boolean var2) throws Exception;
}
