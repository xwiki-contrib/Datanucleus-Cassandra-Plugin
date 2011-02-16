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


Contributors : Pedro Gomes and Universidade do Minho.
    		 : Todd Nine
 ***********************************************************************/
package com.spidertracks.datanucleus;

import static com.spidertracks.datanucleus.utils.MetaDataUtils.getColumnFamily;
import static com.spidertracks.datanucleus.utils.MetaDataUtils.getDescriminatorColumn;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SlicePredicate;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.OMFContext;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.NucleusConnection;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.client.Consistency;
import com.spidertracks.datanucleus.convert.ByteConverterContext;
import com.spidertracks.datanucleus.serialization.JavaSerializer;
import com.spidertracks.datanucleus.serialization.Serializer;

public class CassandraStoreManager extends AbstractStoreManager {

	// MetaDataListener metadataListener;

	private static final String EVICTION_IDLE = "datanucleus.connectionPool.minEvictableIdleTimeMillis";
	private static final String EVICTION_TIME = "datanucleus.connectionPool.timeBetweenEvictionRunsMillis";
	private static final String CREATE_COLUMNS = "datanucleus.autoCreateColumns";
	private static final String CREATE_TABLES = "datanucleus.autoCreateTables";
	private static final String CREATE_SCHEMA = "datanucleus.autoCreateSchema";
	private static final String SERIALIZER = "com.spidertracks.cassandra.serializer";
	private static final String BYTEMAPPER = "com.spidertracks.cassandra.bytemapper";
	private static final String CHECKSLEEPTIME = "com.spidertracks.cassandra.checksleep";
	
	
	private boolean autoCreateSchema = false;
	private boolean autoCreateTables = false;
	private boolean autoCreateColumns = false;

	private int poolTimeBetweenEvictionRunsMillis;
	private int poolMinEvictableIdleTimeMillis;
	private long checkSleepTime;

	private ConnectionFactoryImpl connectionFactory;

	private ByteConverterContext byteConverterContext;

	/**
	 * Constructor.
	 * 
	 * @param clr
	 *            ClassLoader resolver
	 * @param omfContext
	 *            ObjectManagerFactory context
	 */
	public CassandraStoreManager(ClassLoaderResolver clr, OMFContext omfContext) {
		super("cassandra", clr, omfContext);

		PersistenceConfiguration conf = omfContext
				.getPersistenceConfiguration();

		String serializerClass = conf
				.getStringProperty(SERIALIZER);

		Serializer serializer = null;
		
		if (serializerClass == null) {
			serializer = new JavaSerializer();
		} else {
			try {
				serializer = (Serializer) Class.forName(serializerClass)
						.newInstance();
			} catch (Exception e) {
				throw new NucleusDataStoreException(String.format(
						"Could not create serializer for class name %s",
						serializerClass));
			}
		}
		
		String byteMapperFile = conf.getStringProperty(BYTEMAPPER);
		
		byteConverterContext = new ByteConverterContext(byteMapperFile, serializer, getOMFContext().getTypeManager(), getApiAdapter());
		
		autoCreateSchema = conf
				.getBooleanProperty(CREATE_SCHEMA);
		
		checkSleepTime = conf.getLongProperty(CHECKSLEEPTIME);
		
		

		if (autoCreateSchema) {
			autoCreateTables = true;
			autoCreateColumns = true;

		} else {
			autoCreateTables = conf
					.getBooleanProperty(CREATE_TABLES);
			autoCreateColumns = conf
					.getBooleanProperty(CREATE_COLUMNS);
		}
		// how often should the evictor run
		poolTimeBetweenEvictionRunsMillis = conf
				.getIntProperty(EVICTION_TIME);

		if (poolTimeBetweenEvictionRunsMillis == 0) {
			poolTimeBetweenEvictionRunsMillis = 15 * 1000; // default, 15 secs
		}

		// how long may a connection sit idle in the pool before it may be
		// evicted
		poolMinEvictableIdleTimeMillis = conf
				.getIntProperty(EVICTION_IDLE);

		if (poolMinEvictableIdleTimeMillis == 0) {
			poolMinEvictableIdleTimeMillis = 30 * 1000; // default, 30 secs
		}

		connectionFactory.keyspaceComplete(autoCreateSchema);

		if (autoCreateTables || autoCreateColumns) {
			connectionFactory.cfComplete(autoCreateTables, autoCreateColumns);
		}
		
		
		// Handler for persistence process
		persistenceHandler2 = new CassandraPersistenceHandler(this);

		
		logConfiguration();

	}



	protected void registerConnectionMgr() {
		super.registerConnectionMgr();
		this.connectionMgr.disableConnectionPool();
	}

	/**
	 * Release of resources
	 */
	public void close() {
		// omfContext.getMetaDataManager().deregisterListener(metadataListener);
		super.close();
	}

	public NucleusConnection getNucleusConnection(ExecutionContext om) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Accessor for the supported options in string form
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Collection getSupportedOptions() {
		Set set = new HashSet();
		set.add("ApplicationIdentity");
		set.add("TransactionIsolationLevel.read-committed");
		// could happen if writing to "one" or reading from "one" node
		set.add("TransactionIsolationLevel.read-uncommitted");
		return set;
	}

	public boolean isAutoCreateColumns() {
		return autoCreateColumns;
	}

	public boolean isAutoCreateTables() {
		return autoCreateTables;
	}

	public int getPoolMinEvictableIdleTimeMillis() {
		return poolMinEvictableIdleTimeMillis;
	}

	public int getPoolTimeBetweenEvictionRunsMillis() {
		return poolTimeBetweenEvictionRunsMillis;
	}

	/**
	 * @return the defaultKeyspace
	 */
	public String getKeyspace() {
		return connectionFactory.getKeyspace();
	}


	/**
	 * @return the poolName
	 */
	public String getPoolName() {
		return connectionFactory.getPoolName();
	}

	/**
	 * @return the pauseOnCreateTime
	 */
	public long getCheckSleepTime() {
		return checkSleepTime;
	}



	/**
	 * DO NOT CALL OUTSIDE OF FRAMEWORK. This is a callback for the connection
	 * factory to signal to the store manager that it has finished configuring
	 * itself.
	 * 
	 * @param poolName
	 *            the poolName to set
	 */
	public void setConnectionFactory(ConnectionFactoryImpl connectionFactory) {
		this.connectionFactory = connectionFactory;

	}

	/**
	 * @return the byteConverterContext
	 */
	public ByteConverterContext getByteConverterContext() {
		return byteConverterContext;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.datanucleus.store.AbstractStoreManager#getClassNameForObjectID(java
	 * .lang.Object, org.datanucleus.ClassLoaderResolver,
	 * org.datanucleus.store.ExecutionContext)
	 */
	@Override
	public String getClassNameForObjectID(Object id, ClassLoaderResolver clr,
			ExecutionContext ec) {

		String pcClassName = super.getClassNameForObjectID(id, clr, ec);

		AbstractClassMetaData metaData = ec.getMetaDataManager()
				.getMetaDataForClass(pcClassName, clr);

		SlicePredicate descriminator = getDescriminatorColumn(metaData);

		// We only support discriminator. Even in a subclass per table scheme
		// for clarity of the columns within Cassandra.
		if (descriminator == null) {
			return pcClassName;
		}

		Bytes key = byteConverterContext.getRowKeyForId(id);

		return findObject(key, metaData, clr, ec, id);

	}

	private String findObject(Bytes key, AbstractClassMetaData metaData,
			ClassLoaderResolver clr, ExecutionContext ec, Object id) {

		Selector selector = Pelops.createSelector(getPoolName());

		// if we have a discriminator, fetch the discriminator column only
		// and see if it's equal
		// to the class provided by the op

		List<Column> columns = null;

		try {

			columns = selector.getColumnsFromRow(getColumnFamily(metaData),
					key, getDescriminatorColumn(metaData), Consistency.get());

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}

		// what do we do if no descriminator is found and one should be
		// present?
		if (columns == null || columns.size() != 1) {

			// now check if we have subclasses from the given metaData, if we do
			// recurse to a child class and search for the object
			String[] decendents = ec.getMetaDataManager()
					.getSubclassesForClass(metaData.getFullClassName(), true);

			// it has decendents, only recurse to them if their inheritance
			// strategy is a new table
			if (decendents == null || decendents.length == 0) {
				return null;
			}

			AbstractClassMetaData decendentMetaData = null;

			for (String decendent : decendents) {
				decendentMetaData = ec.getMetaDataManager()
						.getMetaDataForClass(decendent, clr);

				InheritanceStrategy strategy = decendentMetaData
						.getInheritanceMetaData().getStrategy();

				// either the subclass has it's own table, or one if it's
				// children may, recurse to find the object
				if (InheritanceStrategy.NEW_TABLE.equals(strategy)
						|| InheritanceStrategy.SUBCLASS_TABLE.equals(strategy)) {
					String result = findObject(key, decendentMetaData, clr, ec,
							id);

					// we found a subclass with the descriminator stored, return
					// it
					if (result != null) {
						return result;
					}
				}
			}

			// nothing found in this class or it's children return null
			return null;

		}

		String descriminatorValue = Bytes.toUTF8(columns.get(0).getValue());

		String className = org.datanucleus.metadata.MetaDataUtils
				.getClassNameFromDiscriminatorValue(descriminatorValue,
						metaData.getDiscriminatorMetaData(), ec);

		// now recursively load the search for our class

		return className;
	}
}
