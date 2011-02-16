package com.spidertracks.datanucleus;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.KsDef;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataListener;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Cluster.Node;
import org.scale7.cassandra.pelops.ColumnFamilyManager;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Pelops;

import com.spidertracks.datanucleus.convert.ByteConverterContext;
import com.spidertracks.datanucleus.utils.ClusterUtils;
import com.spidertracks.datanucleus.utils.MetaDataUtils;

/**
 * Create column families and index columns if they do not exist when a class is
 * loaded the first time.
 * 
 * @author Todd Nine
 * 
 */
public class ColumnFamilyCreator implements MetaDataListener {

	private Cluster cluster = null;
	private String keyspace;

	private Set<String> visitedCfs = new HashSet<String>();

	private CassandraStoreManager storeManager;

	private Object mutex = new Object();
	private boolean createColumns;
	private boolean createColumnFamilies;

	public ColumnFamilyCreator(CassandraStoreManager storeManager,
			Cluster cluster, String keyspace, boolean createColumnFamilies, boolean createColumns) {
		this.storeManager = storeManager;
		this.cluster = cluster;
		this.keyspace = keyspace;
		this.createColumnFamilies = createColumnFamilies;
		this.createColumns = createColumns;

	}

	@Override
	public void loaded(AbstractClassMetaData cmd) {

		String cfName = MetaDataUtils.getColumnFamily(cmd);

		// shouldn't check the keyspace
		if (cfName == null) {
			return;
		}

		if (visitedCfs.contains(cfName)) {
			return;
		}

		synchronized (mutex) {
			// could be the second into the mutex
			if (visitedCfs.contains(cfName)) {
				return;
			}

			Cluster migrationCluster = ClusterUtils.getFirstAvailableNode(cluster);
			
			
			// now go through the corresponding fields and create our indexes

			boolean schemaChanged = false;
			
			if (createColumnFamilies) {
				schemaChanged = createColumnFamily(migrationCluster, cfName);
			}
			
			if(createColumns){
				List<ColumnDef> changed = getNewIndexes(migrationCluster, cmd, cfName);
				
				if(changed.size() > 0){
					CfDef columnFamily = getCf(migrationCluster, cfName);
					
					
					columnFamily.getColumn_metadata().addAll(changed);
					
					ColumnFamilyManager manager = Pelops.createColumnFamilyManager(
							migrationCluster, keyspace);
					
					try {
						manager.updateColumnFamily(columnFamily);
					} catch (Exception e) {
						throw new NucleusDataStoreException("Unable to migrate column families");
					}
					
					schemaChanged = true;
				}
				
				
			}
			
			//wait for every online node to ack the column family and indexes.  If one fails sleep and try it again
			if(schemaChanged){
				for(Node node: cluster.getNodes()){
					validateNode(node, cmd, cfName);
				}
			}
			
			
		}

	}
	
	/**
	 * Validate a node.  Blocks until the column family and indexes are properly created
	 * @param node
	 * @param cmd
	 * @param cfName
	 */
	private void validateNode(Node node, AbstractClassMetaData cmd, String cfName){
		
		Cluster validationCluster = ClusterUtils.getClusterForNode(node);
			
		//loop until it's created
		while(true){
			
			CfDef cf = getCf(validationCluster, cfName);
			
			if(cf == null){
				try {
					Thread.sleep(storeManager.getCheckSleepTime());
				} catch (InterruptedException e) {
					break;
				}
				continue;
			}
			
			List<ColumnDef> columns = getNewIndexes(validationCluster, cmd, cfName);
			
			if(columns.size() > 0){
				try {
					Thread.sleep(storeManager.getCheckSleepTime());
				} catch (InterruptedException e) {
					break;
				}
				continue;
			}
			
			break;
			
		}
			
			
			
			
		
	}

	/**
	 * Get the column family and return it.  Creating it if required
	 * @param keyspaceManager
	 * @param cfName
	 * @return
	 */
	private boolean createColumnFamily(Cluster migrationCluster, String cfName) {

		KeyspaceManager keyspaceManager = Pelops.createKeyspaceManager(migrationCluster);
		
	
		KsDef schema = null;

		try {
			schema = keyspaceManager.getKeyspaceSchema(keyspace);
		} catch (Exception e) {
			throw new NucleusDataStoreException(
					"Could not retrieve schema meta data", e);
		}

		CfDef columnFamily = getDefinition(cfName, schema);

		// no column family, define one
		if (columnFamily == null && createColumnFamilies) {
			

			columnFamily = new CfDef(keyspace, cfName);
			columnFamily
					.setComparator_type(ColumnFamilyManager.CFDEF_COMPARATOR_BYTES);
		
			ColumnFamilyManager manager = Pelops.createColumnFamilyManager(
					migrationCluster, keyspace);

			
			try {
				manager.addColumnFamily(columnFamily);
			} catch (Exception e) {
				throw new NucleusDataStoreException(String.format("Unable to add column family %s", cfName));
			}
			
			return true;
		}

		return false;
	}
	
	
	/**
	 * Returns the CF if it exists, null otherwise
	 * @param keyspaceManager
	 * @param cfName
	 * @return
	 */
	private CfDef getCf(Cluster cluster, String cfName){

		KeyspaceManager keyspaceManager = Pelops.createKeyspaceManager(cluster);
		
		KsDef schema = null;

		try {
			schema = keyspaceManager.getKeyspaceSchema(keyspace);
		} catch (Exception e) {
			throw new NucleusDataStoreException(
					"Could not retrieve schema meta data", e);
		}

		return getDefinition(cfName, schema);
	}

	/**
	 * Return a list of all new indexes that should be created on this columnFamily
	 * @param migrationCluster
	 * @param cmd
	 * @param columnFamily
	 * @return
	 */
	private List<ColumnDef> getNewIndexes(Cluster migrationCluster, AbstractClassMetaData cmd,  String cfName) {

		
		CfDef columnFamily = getCf(migrationCluster, cfName);
		
		if(columnFamily == null){
			throw new NucleusDataStoreException(String.format("Unable to locate column family %s", cfName));
		}
		
		List<ColumnDef> indexColumns = new ArrayList<ColumnDef>();

		ByteConverterContext context = ((CassandraStoreManager) storeManager)
				.getByteConverterContext();

		for (int field : cmd.getAllMemberPositions()) {
			AbstractMemberMetaData memberData = cmd.getMetaDataForManagedMemberAtAbsolutePosition(field);

			String indexName = MetaDataUtils.getIndexName(cmd, memberData);

			if (indexName == null) {
				continue;
			}

			Bytes columnName = MetaDataUtils.getColumnName(cmd, field);

			// already defined
			if (hasColumn(columnName, columnFamily)) {
				continue;
			}

			String validationClass = context.getValidationClass(memberData
					.getType());

			ColumnDef def = new ColumnDef();

			def.setName(columnName.toByteArray());
			def.setValidation_class(validationClass);

			def.setIndex_name(indexName);
			def.setIndex_type(IndexType.KEYS);

			indexColumns.add(def);
		}

		// if we have a discriminator we should index it as we'll be
		// referencing it in queries
		Bytes discriminatorColumn = MetaDataUtils
				.getDiscriminatorColumnName(cmd);

		if (discriminatorColumn != null	&& !hasColumn(discriminatorColumn, columnFamily)) {
			ColumnDef def = new ColumnDef();

			def.setName(discriminatorColumn.toByteArray());
			def.setValidation_class(ColumnFamilyManager.CFDEF_COMPARATOR_UTF8);

			def.setIndex_name(discriminatorColumn.toUTF8() + "_index");
			def.setIndex_type(IndexType.KEYS);
			indexColumns.add(def);
		}

		return indexColumns;

	}

	/**
	 * Return the cf definition if it exists. If not null is returned
	 * 
	 * @param cfName
	 * @param keyspace
	 * @return
	 */
	private CfDef getDefinition(String cfName, KsDef keyspace) {
		for (CfDef cf : keyspace.getCf_defs()) {
			if (cfName.equals(cf.getName())) {
				return cf;
			}
		}

		return null;
	}

	/**
	 * Returns true if the column exists, false otherwise
	 * 
	 * @param cfName
	 * @param cf
	 * @return
	 */
	private boolean hasColumn(Bytes colName, CfDef cf) {
		if (!cf.isSetColumn_metadata()) {
			return false;
		}

		for (ColumnDef col : cf.getColumn_metadata()) {
			if (colName.equals(new Bytes(col.getName()))) {
				return true;
			}
		}

		return false;
	}

}
