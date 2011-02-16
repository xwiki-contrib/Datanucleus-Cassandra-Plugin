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

import java.util.List;

import org.scale7.cassandra.pelops.Bytes;

/**
 * @author Todd Nine
 *
 */
public class OrOperand extends Operand {

	/* (non-Javadoc)
	 * @see com.spidertracks.datanucleus.query.QueryResult#complete(com.spidertracks.datanucleus.query.QueryResult)
	 */
	@Override
	public synchronized void complete(Operand child) {
		//by default && should union the results from left and right
		
		//first child to call
		if(candidateKeys == null){
			candidateKeys = child.getCandidateKeys();
			return;
		}
		
		//second child to call
		candidateKeys.addAll(child.getCandidateKeys());
		
		if(parent != null){
			parent.complete(this);
		}
	
	}

	@Override
	public void performQuery(String poolName, String cfName, Bytes[] columns) {
		
		left.performQuery(poolName, cfName, columns);
		right.performQuery(poolName, cfName, columns);
		
	}

	@Override
	public Operand optimizeDescriminator(Bytes descriminatorColumnValue,
			List<Bytes> possibleValues) {
		setLeft(left.optimizeDescriminator(descriminatorColumnValue, possibleValues));
		setRight(right.optimizeDescriminator(descriminatorColumnValue, possibleValues));
		
		return this;
		
	}

}
