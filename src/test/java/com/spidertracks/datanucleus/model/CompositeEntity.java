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
package com.spidertracks.datanucleus.model;

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Todd Nine
 *
 */
@PersistenceCapable(identityType= IdentityType.APPLICATION, table="CompositeEntity")
public class CompositeEntity {

    @PrimaryKey
    @Persistent
    private CompositeKey id;
    
    @Persistent
    private String testVal;
    
    

    
    /**
     * @param id the id to set
     */
    public void setId(CompositeKey id) {
        this.id = id;
    }

    public CompositeKey getId() {
        return id;
    }

    /**
     * @return the testVal
     */
    public String getTestVal() {
        return testVal;
    }

    /**
     * @param testVal the testVal to set
     */
    public void setTestVal(String testVal) {
        this.testVal = testVal;
    }
    

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CompositeEntity other = (CompositeEntity) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }
    
    
    
    

}
