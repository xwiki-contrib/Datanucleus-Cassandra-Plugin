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

import org.apache.cassandra.thrift.Column;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.convert.ByteConverterContext;

/**
 * Object for writing collection columns
 * @author Todd Nine
 *
 */
public class WriteMap extends ExternalEntityWriter {


	public WriteMap(Selector selector,ByteConverterContext context,
			String ownerColumnFamily, Bytes rowKey, Bytes ownerColumn) {
		super(selector, context, ownerColumnFamily, rowKey, ownerColumn);
	}

	

	/**
	 * Write the relationship column
	 * @param mutator
	 */
	public void writeRelationship(Mutator mutator, Object key, Object entityKey) {
		// a DRE, take the property +256 bytes so the buffer hopefully won't need to be re-allocated and copied.
		ByteBuffer buffer = ByteBuffer.allocate(ownerColumn.length() + 256);

		buffer.mark();
		buffer.put(ownerColumn.toByteArray());
		buffer.put(DELIM_MIN);
		buffer = context.getRowKeyForId(key, buffer);
		buffer.limit(buffer.position());
		buffer.reset();
		
		super.addStoredColumn(buffer);
		
		ByteBuffer value = context.getRowKeyForId(entityKey, null);
		value.reset();

		Column keyColumn = mutator.newColumn(Bytes.fromByteBuffer(buffer),
				Bytes.fromByteBuffer(value));

		mutator.writeColumn(ownerColumnFamily, rowKey, keyColumn, true);

	}
}
