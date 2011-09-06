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

import java.nio.ByteBuffer;

import org.scale7.cassandra.pelops.ColumnFamilyManager;

import com.spidertracks.datanucleus.convert.ByteConverter;
import com.spidertracks.datanucleus.convert.ByteConverterContext;
import com.spidertracks.datanucleus.convert.ConverterUtils;

/**
 * @author Todd Nine
 * 
 */
public class CompositeKeyConverter implements ByteConverter {

    private static final int SIZE = (Long.SIZE*2+Integer.SIZE)/Byte.SIZE;
    
    @Override
    public Object getObject(ByteBuffer buffer,  ByteConverterContext context) {
        CompositeKey key = new CompositeKey();

        key.setFirst(buffer.getLong());
        key.setSecond(buffer.getLong());
        key.setThird(buffer.getInt());
        return key;
    }

    @Override
    public ByteBuffer writeBytes(Object value, ByteBuffer buffer, ByteConverterContext context) {

        ByteBuffer checked = ConverterUtils.check(buffer, SIZE);
        
        CompositeKey key = (CompositeKey) value;
        checked.putLong(key.getFirst());
        checked.putLong(key.getSecond());
        return checked.putInt(key.getThird());

    }

    @Override
    public String getComparatorType() {
        return ColumnFamilyManager.CFDEF_COMPARATOR_BYTES;
    }

    
}
