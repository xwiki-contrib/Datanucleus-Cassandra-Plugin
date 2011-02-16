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

import org.apache.cassandra.thrift.IndexClause;

/**
 * Interface to mark operands that can be combined.  Primarily used to compress && and equality operands in the tree
 * into a single secondary index search
 * @author Todd Nine
 *
 */
public interface CompressableOperand {

	/**
	 * Return the index clause of this node and all it's compressible children
	 */
	public IndexClause getIndexClause();
}
