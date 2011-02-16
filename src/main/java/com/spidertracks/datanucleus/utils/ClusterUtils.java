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
package com.spidertracks.datanucleus.utils;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Cluster.Node;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Todd Nine
 *
 */
public class ClusterUtils {
	
	private static Logger logger = LoggerFactory.getLogger(ClusterUtils.class);

	/**
	 * Get the first node that is online listed in our cluster.  This performs all migrations
	 * on the same node to avoid migration lag between nodes
	 * 
	 * @return A cluster with only a single node
	 * @throws a NucleusDataStoreExeption is no nodes can be reached
	 */
	public static Cluster getFirstAvailableNode(Cluster cluster) {

		Node[] nodes = cluster.getNodes();

		for (Node node : nodes) {
	
			Cluster  firstNodeOnly = getClusterForNode(node);
			
			if(firstNodeOnly == null){
				continue;
			}
			
			return firstNodeOnly;
		}

		throw new NucleusDataStoreException("Could not connect to any node to perform migrations %s");
	}
	
	/**
	 * Get a cluster with only the given node
	 * @param node
	 * @return
	 */
	public static Cluster getClusterForNode(Node node){
		Cluster  firstNodeOnly = new Cluster(node.getAddress(), node.getConfig(), false);
		KeyspaceManager manager = new KeyspaceManager(firstNodeOnly);
		try {
			manager.getKeyspaceNames();
		} catch (Exception e) {
			logger.error("Unable to connect to node", e);
			// swallow and try the next one
			return null;
		}
		
		return firstNodeOnly;
	}
}
