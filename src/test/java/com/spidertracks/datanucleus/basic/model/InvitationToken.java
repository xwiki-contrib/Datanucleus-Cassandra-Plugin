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

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Index;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Todd Nine
 *
 */
@PersistenceCapable(table="InvitationToken", identityType = IdentityType.APPLICATION)
public class InvitationToken {

    @PrimaryKey
    @Persistent
    private String token;

    @Persistent(dependent="true", defaultFetchGroup="true")
    private InvitedPerson person;
    
    @Index(name="InvitationToken_TestString")
    @Persistent
    private String testString;

    /**
     * @return the token
     */
    public String getToken() {
        return token;
    }

    /**
     * @param token the token to set
     */
    public void setToken(String token) {
        this.token = token;
    }

    /**
     * @return the person
     */
    public InvitedPerson getPerson() {
        return person;
    }

    /**
     * @param person the person to set
     */
    public void setPerson(InvitedPerson person) {
        this.person = person;
    }

    /**
     * @return the testString
     */
    public String getTestString() {
        return testString;
    }

    /**
     * @param testString the testString to set
     */
    public void setTestString(String testString) {
        this.testString = testString;
    }
}
