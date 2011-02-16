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

import java.util.HashMap;
import java.util.Map;

import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.scale7.cassandra.pelops.Pelops;

import com.spidertracks.datanucleus.CassandraStoreManager;

/**
 * Internalises all pending operations for a given Execution context.
 * 
 * @author Todd Nine
 * 
 */
public class BatchMutationManager {

	private Map<ExecutionContext, ExecutionContextMutate> contextMutations = new HashMap<ExecutionContext, ExecutionContextMutate>();
	private Map<ExecutionContext, ExecutionContextDelete> contextDeletions = new HashMap<ExecutionContext, ExecutionContextDelete>();

	private CassandraStoreManager manager;

	public BatchMutationManager(CassandraStoreManager manager) {
		this.manager = manager;
	}

	public ExecutionContextDelete beginDelete(ExecutionContext context,
			ObjectProvider op) {
		ExecutionContextDelete deleteContext = getDeletions(context);
		deleteContext.pushInstance();
		return deleteContext;

	}

	public ExecutionContextMutate beginWrite(ExecutionContext context) {
		ExecutionContextMutate mutationContext = getMutations(context);
		mutationContext.pushInstance();
		return mutationContext;
	}

	/**
	 * Returns true if this is the last object to end writing. This mean the
	 * mutation for this context should be saved
	 * 
	 * @param context
	 * @param sm
	 * @return
	 * @throws Exception
	 */
	public void endDelete(ExecutionContext context)
			throws Exception {
		// not our root instance, don't create a batch mutation
		if (!getDeletions(context).popInstance()) {
			return;
		}

		// it is our root instance, create the batch mutation.

		getDeletions(context).execute();
		contextDeletions.remove(context);

	}

	/**
	 * Returns true if this is the last object to end writing. This mean the
	 * mutation for this context should be saved
	 * 
	 * @param context
	 * @param sm
	 * @return
	 * @throws Exception
	 */
	public void endWrite(ExecutionContext context)
			throws Exception {
		// not our root instance, don't create a batch mutation
		if (!getMutations(context).popInstance()) {
			return;
		}

		// it is our root instance, create the batch mutation.

		getMutations(context).execute();
		contextMutations.remove(context);

	}

	/**
	 * Get the mutations for this execution context
	 * 
	 * @param context
	 * @return
	 */
	private ExecutionContextMutate getMutations(ExecutionContext context) {
		ExecutionContextMutate operations = contextMutations.get(context);

		if (operations == null) {
			operations = new ExecutionContextMutate(context,
					Pelops.createMutator(manager.getPoolName(), System.nanoTime()/1000));
			contextMutations.put(context, operations);
		}

		return operations;
	}

	/**
	 * Get the mutations for this execution context
	 * 
	 * @param context
	 * @return
	 */
	private ExecutionContextDelete getDeletions(ExecutionContext context) {
		ExecutionContextDelete operations = contextDeletions.get(context);

		if (operations == null) {
			operations = new ExecutionContextDelete(context, Pelops
					.createRowDeletor(manager.getPoolName(), System.nanoTime()/1000));
			contextDeletions.put(context, operations);
		}

		return operations;
	}

}
