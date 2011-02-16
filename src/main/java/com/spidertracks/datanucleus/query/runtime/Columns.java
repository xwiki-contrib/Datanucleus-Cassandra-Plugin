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
package com.spidertracks.datanucleus.query.runtime;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.scale7.cassandra.pelops.Bytes;

/**
 * Class that holds the serialized bytes of the key and the descriminator value if present
 * 
 * @author Todd Nine
 *
 */
public class Columns {
	
	private Bytes rowKey;
	private Map<Bytes, Bytes> values;

	
	public Columns(Bytes rowKey){
		this.rowKey = rowKey;
		values = new LinkedHashMap<Bytes,Bytes>();
	}
	
	public Bytes getColumnValue(Bytes key) {
		return values.get(key);
	}

	/**
	 * Add the result
	 * @param column
	 */
	public void addResult(Column column){
		values.put(new Bytes(column.getName()), new Bytes(column.getValue()));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((rowKey == null) ? 0 : rowKey.hashCode());
		result = prime * result + ((values == null) ? 0 : values.hashCode());
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
		Columns other = (Columns) obj;
		if (rowKey == null) {
			if (other.rowKey != null)
				return false;
		} else if (!rowKey.equals(other.rowKey))
			return false;
		if (values == null) {
			if (other.values != null)
				return false;
		} else if (!values.equals(other.values))
			return false;
		return true;
	}



	
	
}
