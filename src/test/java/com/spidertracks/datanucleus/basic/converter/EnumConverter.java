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
package com.spidertracks.datanucleus.basic.converter;

import java.util.HashMap;
import java.util.Map;

import org.datanucleus.store.types.ObjectStringConverter;

import com.spidertracks.datanucleus.basic.model.EnumValues;

/**
 * @author Todd Nine
 *
 */
public class EnumConverter implements ObjectStringConverter {

    private static int fromCount = 0;
    private static int toCount = 0;
    
    private Map<Integer, EnumValues> indexed = new HashMap<Integer, EnumValues>();
    
    
    public EnumConverter(){
        for(EnumValues current: EnumValues.values()){
            indexed.put(current.getValue(), current);
        }
    }
    /* (non-Javadoc)
     * @see org.datanucleus.store.types.ObjectStringConverter#toObject(java.lang.String)
     */
    @Override
    public Object toObject(String str) {
        toCount++;
        return indexed.get(Integer.valueOf(str));
        
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.types.ObjectStringConverter#toString(java.lang.Object)
     */
    @Override
    public String toString(Object obj) {
        fromCount++;
        return String.valueOf(((EnumValues)obj).getValue());
    }

    public static int getFromCount(){
        return fromCount;
    }
    
    public static int getToCount(){
        return toCount;
    }
}
