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

Contributors : Todd Nine
 ***********************************************************************/
package com.spidertracks.datanucleus.mutate;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Stack;

import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.RowDeletor;

import com.spidertracks.datanucleus.client.Consistency;

/**
 * Holds all mutations for the current execution context
 * 
 * @author Todd Nine
 * 
 */
public class ExecutionContextDelete extends ExecutionContextOp {



	//operations of mutations to perform
	//our reference to visited objects so we don't get stuck in a recursive delete
	private IdentityHashMap<ObjectProvider, Object> visited = new IdentityHashMap<ObjectProvider, Object>();
	private List<Deletion> mutations = new Stack<Deletion>();
	private RowDeletor deletor;

	
	public ExecutionContextDelete(ExecutionContext ctx, RowDeletor deletor) {
		super(ctx);
		this.deletor = deletor;
	}




	/**
	 * Add the deleting if the op hasn't already been visited
	 * @param op The object we're deleting
	 * @param key The key to delete
	 * @param columnFamily The CF to dele
	 * @return True if this is the first visit to the object.  False otherwise
	 */
	public boolean addDeletion(ObjectProvider op, Bytes key, String columnFamily) {
		
		if(visited.containsKey(op)){
			return false;
		}
		
		visited.put(op, null);
		
		mutations.add(new Deletion(key, columnFamily));
		
		return true;
	}



	public void execute() throws Exception {
		for (Deletion deletion : mutations) {
			deletor.deleteRow(deletion.columnFamily, deletion.rowKey, Consistency.get());
		}
	}
	
	private class Deletion{
		
		public Deletion(Bytes rowKey, String columnFamily) {
			super();
			this.rowKey = rowKey;
			this.columnFamily = columnFamily;
		}
		
		private Bytes rowKey;
		private String columnFamily;
	}

}
