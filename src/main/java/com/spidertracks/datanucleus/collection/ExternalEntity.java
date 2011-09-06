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
package com.spidertracks.datanucleus.collection;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.client.Consistency;
import com.spidertracks.datanucleus.convert.ByteConverterContext;

/**
 * A collection that represents a relationship. All relations ships are
 * bi-directional even if not mapped as one this allows us to keep our relations
 * clean when the object on the non owned side is deleted. This cleanup happens
 * by loading all owners with the given ID and removing them.
 * 
 * @author Todd Nine
 * 
 */
public class ExternalEntity {

    protected static final byte DELIM_MIN = 0;
    protected static final byte DELIM_MAX = 1;

    protected ByteConverterContext context;

    protected String ownerColumnFamily;

    protected Bytes ownerColumn;

    protected Bytes rowKey;

    protected Selector selector;

    /**
     * 
     * @param context
     *            The Byte converter context
     * @param ownerColumnFamily
     *            The owning column family
     * @param rowKey
     *            The row key
     * @param ownerColumn
     *            The bytes of the column
     */
    public ExternalEntity(Selector selector, ByteConverterContext context,
            String ownerColumnFamily, Bytes rowKey, Bytes ownerColumn) {
        this.selector = selector;
        this.context = context;
        this.ownerColumnFamily = ownerColumnFamily;
        this.ownerColumn = ownerColumn;
        this.rowKey = rowKey;
    }

    

}
