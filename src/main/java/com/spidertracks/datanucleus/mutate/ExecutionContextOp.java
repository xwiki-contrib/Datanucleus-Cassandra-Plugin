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
package com.spidertracks.datanucleus.mutate;

import org.datanucleus.store.ExecutionContext;

/**
 * @author Todd Nine
 *
 */
public abstract class ExecutionContextOp {
	
	private ExecutionContext ctx;
	private int count;

	public ExecutionContextOp(ExecutionContext ctx) {
		this.ctx = ctx;
		this.count = 0;
	}

	/**
	 * Get the execution context this mutate belongs to
	 * 
	 * @return
	 */
	public ExecutionContext getExecutionContext() {
		return ctx;
	}
	/**
	 * Push the current on to our stack for this execution context
	 * 
	 * @param instance
	 */
	public void pushInstance() {
		count++;
	}

	/**
	 * Pop the current instance from our execution context
	 * 
	 * @param instnace
	 * @return
	 */
	public boolean popInstance() {
		count--;
		return count == 0;

	}
}
