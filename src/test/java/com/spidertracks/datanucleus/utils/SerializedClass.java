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
package com.spidertracks.datanucleus.utils;

import java.io.Serializable;

/**
 * @author Todd Nine
 *
 */

public class SerializedClass implements Serializable{
    
    /**
     * 
     */
    private static final long serialVersionUID = 5416370150512344462L;

    int testVal = 10;
    
    boolean testBool = true;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (testBool ? 1231 : 1237);
        result = prime * result + testVal;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SerializedClass other = (SerializedClass) obj;
        if (testBool != other.testBool)
            return false;
        if (testVal != other.testVal)
            return false;
        return true;
    }

    
}
