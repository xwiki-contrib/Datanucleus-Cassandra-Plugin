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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.client.Consistency;
import com.spidertracks.datanucleus.convert.ByteConverterContext;

/**
 * Class that allows us to perform basic I/O ops on external entities
 * 
 * @author Todd Nine
 * 
 */
public class ExternalEntityWriter extends ExternalEntity {

	private static final int ITERATION_SIZE = 100;

	private Set<Bytes> savedColumns;

	public ExternalEntityWriter(Selector selector,
			ByteConverterContext context, String ownerColumnFamily,
			Bytes rowKey, Bytes ownerColumn) {
		super(selector, context, ownerColumnFamily, rowKey, ownerColumn);
		savedColumns = new HashSet<Bytes>();
	}

	/**
	 * Add the stored column to our internal queue
	 * 
	 * @param column
	 */
	protected void addStoredColumn(ByteBuffer column) {
		savedColumns.add(Bytes.fromByteBuffer(column));
	}

	/**
	 * Remove all columns from the collection/map. Useful for if a collection is
	 * set to null
	 */
	public void removeAllColumns(Mutator mutator) {

		byte[] columnBytes = ownerColumn.toByteArray();

		SliceRange range = new SliceRange();
		range.setCount(ITERATION_SIZE);
		range.setFinish(createBuffer(columnBytes, DELIM_MAX));

		SlicePredicate predicate = new SlicePredicate();
		predicate.setSlice_range(range);

		List<Column> results = null;

		do {

			range.setStart(createBuffer(columnBytes, DELIM_MIN));

			results = selector.getColumnsFromRow(ownerColumnFamily, rowKey,
					predicate, Consistency.get());

			// remove all columns that are presisted
			for (Column col : results) {
				mutator.deleteColumn(ownerColumnFamily, rowKey,
						Bytes.fromByteArray(col.getName()));
			}

			// advance our start key if required
			if (results.size() == ITERATION_SIZE) {
				columnBytes = results.get(ITERATION_SIZE - 1).getName();
			}

		} while (results != null && results.size() == ITERATION_SIZE);

	}

	/**
	 * Removes all columns that have not been marked as persisted.
	 */
	public void removeRemaining(Mutator mutator) {
		byte[] columnBytes = ownerColumn.toByteArray();

		SliceRange range = new SliceRange();
		range.setCount(ITERATION_SIZE);
		range.setFinish(createBuffer(columnBytes, DELIM_MAX));

		SlicePredicate predicate = new SlicePredicate();
		predicate.setSlice_range(range);

		List<Column> results = null;

		do {

			range.setStart(createBuffer(columnBytes, DELIM_MIN));

			results = selector.getColumnsFromRow(ownerColumnFamily, rowKey,
					predicate, Consistency.get());

			// remove all columns that are presisted
			for (Column col : results) {

				//not in our already saved columns, remove it
				if (!savedColumns.contains(Bytes.fromByteBuffer(col.name))) {
					mutator.deleteColumn(ownerColumnFamily, rowKey,
							Bytes.fromByteArray(col.getName()));
				}
			}

			// advance our start key if required
			if (results.size() == ITERATION_SIZE) {
				columnBytes = results.get(ITERATION_SIZE - 1).getName();
			}

		} while (results.size() == ITERATION_SIZE);

	}

	protected ByteBuffer createBuffer(byte[] columnBytes, byte delimByte) {
		ByteBuffer buffer = ByteBuffer.allocate(columnBytes.length + 1);
		buffer.mark();
		buffer.put(columnBytes);
		buffer.put(delimByte);
		buffer.reset();

		return buffer;
	}

}
