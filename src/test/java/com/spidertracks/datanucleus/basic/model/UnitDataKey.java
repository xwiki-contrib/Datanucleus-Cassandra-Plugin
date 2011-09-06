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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Date;

import org.scale7.cassandra.pelops.ColumnFamilyManager;

import com.spidertracks.datanucleus.convert.ByteConverterContext;
import com.spidertracks.datanucleus.convert.ConverterUtils;
import com.spidertracks.datanucleus.identity.ByteAware;

/**
 * @author Todd Nine
 * 
 */
public class UnitDataKey implements Serializable, ByteAware {

    /**
         * 
         */
    private static final long serialVersionUID = 1507634768434382394L;
    public Date createdDate;
    public String unitId;

    public UnitDataKey() {
    }

    public UnitDataKey(Date createdDate, String unitId) {
        this.createdDate = createdDate;
        this.unitId = unitId;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public String getUnitId() {
        return unitId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof UnitDataKey)) {
            return false;
        }
        UnitDataKey c = (UnitDataKey) obj;

        return unitId.equals(c.unitId) && createdDate.equals(c.createdDate);
    }

    @Override
    public int hashCode() {
        return this.unitId.hashCode() ^ this.createdDate.hashCode();
    }

    public String toString() {
        // Give output expected by String constructor
        return this.unitId + "::" + this.createdDate.getTime();
    }

    @Override
    public ByteBuffer writeBytes(ByteBuffer buffer, ByteConverterContext context) {
        
        ByteBuffer checked = ConverterUtils.check(buffer, 8+this.unitId.length());
        
        checked.putLong(this.createdDate.getTime());
        try {
            return checked.put(this.unitId.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            // should never happen
            throw new RuntimeException(e);
        }

    }

    @Override
    public void parseBytes(ByteBuffer buffer, ByteConverterContext context) {
        this.createdDate = new Date(buffer.getLong());
        try {
            this.unitId = new String(buffer.array(), buffer.position(),
                    buffer.remaining(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // should never happen
            throw new RuntimeException(e);
        }

    }

    @Override
    public String getComparatorType() {
        return ColumnFamilyManager.CFDEF_COMPARATOR_BYTES;
    }


}
