/**********************************************************************
Copyright (c) 2010 Todd Nine. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
 ***********************************************************************/
package com.spidertracks.datanucleus.basic.model;

import java.util.Date;

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Index;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

import com.spidertracks.datanucleus.model.BaseEntity;

/**
 * @author Todd Nine
 *
 */
@PersistenceCapable(table = "Person", identityType = IdentityType.APPLICATION)
@Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
public class Person extends BaseEntity {
    
    //all 3 fields and persistent and secondary indexed
    @Persistent
    @Index
    private String firstName;
    
    @Persistent
    @Index
    private String lastName;
    
    //should auto generate secondary index name with name "Person_email"
    @Persistent
    @Index
    private String email;
    
    
    //data that's persisted but not indexed
    @Persistent
    private int loginCount;
    
    @Persistent
    @Index
    private Date lastLogin;
    /**
     * @return the firstName
     */
    public String getFirstName() {
        return firstName;
    }
    /**
     * @param firstName the firstName to set
     */
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }
    /**
     * @return the lastName
     */
    public String getLastName() {
        return lastName;
    }
    /**
     * @param lastName the lastName to set
     */
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
    /**
     * @return the email
     */
    public String getEmail() {
        return email;
    }
    /**
     * @param email the email to set
     */
    public void setEmail(String email) {
        this.email = email;
    }
    /**
     * @return the loginCount
     */
    public int getLoginCount() {
        return loginCount;
    }
    /**
     * @param loginCount the loginCount to set
     */
    public void setLoginCount(int loginCount) {
        this.loginCount = loginCount;
    }
    /**
     * @return the lastLogin
     */
    public Date getLastLogin() {
        return lastLogin;
    }
    /**
     * @param lastLogin the lastLogin to set
     */
    public void setLastLogin(Date lastLogin) {
        this.lastLogin = lastLogin;
    }
    
    
    

}
