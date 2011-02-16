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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.datanucleus.store.ExecutionContext;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.client.Consistency;
import com.spidertracks.datanucleus.convert.ByteConverterContext;

/**
 * Object for writing collection columns
 * 
 * @author Todd Nine
 * 
 */
public class ReadCollection extends ExternalEntity implements
		Iterable<Object>, Iterator<Object> {
	

	private Class<?> targetClass;

	private int index = -1;

	private List<Column> columns;
	
	private ExecutionContext ec;

	public ReadCollection(Selector selector, ByteConverterContext context,
			String ownerColumnFamily, Bytes rowKey, Bytes ownerColumn, ExecutionContext ec, Class<?> targetClass) {
		super(selector, context, ownerColumnFamily, rowKey, ownerColumn);
		this.ec = ec;
		this.targetClass = targetClass;
	}

	/**
	 * Get the slice predicate for selecting all the columns from the given
	 * start key (inclusive). If the key is null, the range reads from the
	 * beginning. Reads up to the max for the given column from start using
	 * count.
	 * 
	 * @param count
	 * @param startKey
	 * @return
	 */
	public void fetchColumns(int count, Bytes startKey) {

		SliceRange range = new SliceRange();

		int length = ownerColumn.length() + 1;

		if (startKey != null) {
			length += startKey.length();
		}

		ByteBuffer startBuff = ByteBuffer.allocate(length);
		startBuff.mark();
		startBuff.put(ownerColumn.toByteArray());
		startBuff.put(DELIM_MIN);

		if (startKey != null) {
			startBuff.put(startKey.toByteArray());
		}

		startBuff.reset();

		range.setStart(startBuff);

		ByteBuffer endBuff = ByteBuffer.allocate(ownerColumn.length() + 1);
		endBuff.mark();
		endBuff.put(ownerColumn.toByteArray());
		endBuff.put(DELIM_MAX);
		endBuff.reset();

		range.setFinish(endBuff);
		range.setCount(count);

		SlicePredicate predicate = new SlicePredicate();

		predicate.setSlice_range(range);

		columns = selector.getColumnsFromRow(ownerColumnFamily, rowKey,
				predicate, Consistency.get());

	}

	@Override
	public Iterator<Object> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return columns != null && index + 1 < columns.size();
	}

	/**
	 * Returns the bytes as an object identity
	 */
	@Override
	public Object next() {
		if (++index > columns.size()) {
			throw new NoSuchElementException("No elements left");
		}
		
		ByteBuffer buffer = columns.get(index).name;
		buffer.position(buffer.position()+ownerColumn.length()+1);

		return context.getObjectIdentity(ec, targetClass,
				Bytes.fromByteBuffer(buffer));

	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove isn't supported");
	}
}
