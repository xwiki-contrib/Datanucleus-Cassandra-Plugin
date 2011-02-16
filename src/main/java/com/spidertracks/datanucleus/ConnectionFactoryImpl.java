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
package com.spidertracks.datanucleus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.datanucleus.OMFContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IConnection.Config;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.OperandPolicy;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.pool.CommonsBackedPool.Policy;

import com.spidertracks.datanucleus.utils.ClusterUtils;

/**
 * Implementation of a ConnectionFactory for HBase.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory {

	// matches the pattern
	// cassandra:<poolname>:<keyspace>:<connectionport>:host1, host2, host3...
	// etc
	private static final Pattern URL = Pattern
			.compile("cassandra:(\\w+):(true|false):(true|false):(\\d+):(\\w+):(\\d+):(\\s*\\S+[.\\S+]*[\\s*,\\s*\\S+[.\\S+]*]*)");

	private Cluster cluster;

	private String keyspace;

	private String poolName;

	private CassandraStoreManager manager;

	/**
	 * Constructor.
	 * 
	 * @param omfContext
	 *            The OMF context
	 * @param resourceType
	 *            Type of resource (tx, nontx)
	 */
	public ConnectionFactoryImpl(OMFContext omfContext, String resourceType) {
		super(omfContext, resourceType);

		String connectionString = omfContext.getPersistenceConfiguration()
				.getStringProperty("datanucleus.ConnectionURL");

		// now strip off the cassandra: at the beginning and make sure our
		// format is correct

		Matcher hostMatcher = URL.matcher(connectionString);

		if (!hostMatcher.matches()) {
			throw new UnsupportedOperationException(
					"Your URL must be in the format of cassandra:poolname:<true|false, framed transport>:<true|false, dynamic node discovery>:<timeout in ms>:keyspace:port:host1[,hostN]");
		}

		// pool name
		poolName = hostMatcher.group(1);

		// set framed
		boolean framed = Boolean.parseBoolean(hostMatcher.group(2));
		
		boolean discover = Boolean.parseBoolean(hostMatcher.group(3));
		
		int timeout = Integer.parseInt(hostMatcher.group(4));

		// set our keyspace
		keyspace = hostMatcher.group(5);

		// grab our port
		int defaultPort = Integer.parseInt(hostMatcher.group(6));

		String hosts = hostMatcher.group(7);

		// by default we won't discover other nodes we're not explicitly
		// connected to. May change in future

		Config config = new Config(defaultPort, framed, timeout);

		cluster = new Cluster(hosts, config, discover);

		manager = (CassandraStoreManager) omfContext.getStoreManager();

		manager.setConnectionFactory(this);

	}

	/**
	 * Setup the keyspace if the schema should be created
	 * 
	 * @param createSchema
	 */
	public void keyspaceComplete(boolean createSchema) {
		if (!createSchema) {
			return;
		}

		KeyspaceManager keyspaceManager = new KeyspaceManager(
				ClusterUtils.getFirstAvailableNode(cluster));

		List<KsDef> keyspaces;
		try {
			keyspaces = keyspaceManager.getKeyspaceNames();
		} catch (Exception e) {
			throw new NucleusDataStoreException("Unable to scan for keyspace");
		}

		boolean found = false;

		for (KsDef ksDef : keyspaces) {
			if (ksDef.name.equals(keyspace)) {
				found = true;
				break;
			}
		}

		if (!found) {
			KsDef keyspaceDefinition = new KsDef(keyspace,
					KeyspaceManager.KSDEF_STRATEGY_SIMPLE, 1,
					new ArrayList<CfDef>());

			try {
				keyspaceManager.addKeyspace(keyspaceDefinition);
			} catch (Exception e) {
				throw new NucleusDataStoreException("Not supported", e);
			}

		}

		if (Pelops.getDbConnPool(poolName) == null) {
			OperandPolicy opPolicy = new OperandPolicy();
			opPolicy.setMaxOpRetries(3);
			opPolicy.setDeleteIfNull(true);
			
			Policy policy = new Policy();
			
			Pelops.addPool(poolName, cluster, keyspace, policy, opPolicy);
		}
	}

	/**
	 * Setup the keyspace if the schema should be created
	 * 
	 * @param createSchema
	 */
	public void cfComplete(boolean createColumnFamilies, boolean createColumns) {
		if (createColumnFamilies) {
			manager.getMetaDataManager().registerListener(
					new ColumnFamilyCreator(manager, cluster, keyspace,
							createColumnFamilies, createColumns));
		}
	}

	/**
	 * Obtain a connection from the Factory. The connection will be enlisted
	 * within the {@link org.datanucleus.Transaction} associated to the
	 * <code>poolKey</code> if "enlist" is set to true.
	 * 
	 * @param poolKey
	 *            the pool that is bound the connection during its lifecycle (or
	 *            null)
	 * @param options
	 *            Any options for then creating the connection
	 * @return the {@link org.datanucleus.store.connection.ManagedConnection}
	 */
	@SuppressWarnings("rawtypes")
	public ManagedConnection createManagedConnection(Object poolKey,
			Map transactionOptions) {

		throw new NucleusDataStoreException("Not supported");

	}

	/**
	 * @return the cluster
	 */
	public Cluster getCluster() {
		return cluster;
	}

	/**
	 * @return the keyspace
	 */
	public String getKeyspace() {
		return keyspace;
	}

	/**
	 * @return the poolName
	 */
	public String getPoolName() {
		return poolName;
	}

}