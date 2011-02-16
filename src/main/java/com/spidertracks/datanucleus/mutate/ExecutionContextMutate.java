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

import org.datanucleus.store.ExecutionContext;
import org.scale7.cassandra.pelops.Mutator;

import com.spidertracks.datanucleus.client.Consistency;

/**
 * Holds all mutations for the current execution context
 * 
 * @author Todd Nine
 * 
 */
public class ExecutionContextMutate extends ExecutionContextOp {

	// operations of mutations to perform
	private Mutator mutator;

	public ExecutionContextMutate(ExecutionContext ctx, Mutator mutator) {
		super(ctx);
		this.mutator = mutator;
	}

	public void execute() throws Exception {
		mutator.execute(Consistency.get());
	}

	/**
	 * @return the mutator
	 */
	public Mutator getMutator() {
		return mutator;
	}

}
