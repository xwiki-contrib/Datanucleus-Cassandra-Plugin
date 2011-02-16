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
    ...
 ***********************************************************************/
package com.spidertracks.datanucleus.utils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.SlicePredicate;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.metadata.InheritanceMetaData;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.store.ExecutionContext;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;

/**
 * Utility class to convert instance data to Cassandra columns and data types
 * 
 * TODO Cache the data returned here for better performance
 * 
 * @author Todd Nine
 * 
 */
public class MetaDataUtils {

	public static final Charset UTF8 = Charset.forName("UTF-8");

	//A null place holder for the cached values
	private static final String NULL = "\uffff\uffff";

	private static ConcurrentMap<String, String> classToCfNames = new ConcurrentHashMap<String, String>();

	private static ConcurrentMap<AbstractMemberMetaData, Bytes> fieldToColumnNames = new ConcurrentHashMap<AbstractMemberMetaData, Bytes>();

	private static ConcurrentMap<AbstractMemberMetaData, String> fieldToIndexNames = new ConcurrentHashMap<AbstractMemberMetaData, String>();

	private static ConcurrentMap<String, List<Bytes>> classToSubclasses = new ConcurrentHashMap<String, List<Bytes>>();


	/**
	 * Get the column metadata for the class and fieldname
	 * 
	 * @param metaData
	 * @param absoluteFieldNumber
	 * @return
	 */
	public static Bytes getColumnName(AbstractClassMetaData metaData,
			int absoluteFieldNumber) {

		AbstractMemberMetaData memberMetaData = metaData
				.getMetaDataForManagedMemberAtAbsolutePosition(absoluteFieldNumber);

		Bytes cached = fieldToColumnNames.get(memberMetaData);

		if (cached != null) {
			return cached;
		}

		String name = null;
		// Try the first column if specified
		ColumnMetaData[] colmds = memberMetaData.getColumnMetaData();
		if (colmds != null && colmds.length > 0) {
			name = colmds[0].getName();
		} else {
			name = memberMetaData.getName();
		}

		cached = Bytes.fromUTF8(name);
		
		fieldToColumnNames.putIfAbsent(memberMetaData, cached);

		return cached;

	}

	/**
	 * Get the column metadata for the class and fieldname
	 * 
	 * @param metaData
	 * @param absoluteFieldNumber
	 * @return
	 */
	public static Bytes getIdentityColumn(AbstractClassMetaData metaData) {

		int[] pks = metaData.getPKMemberPositions();

		if (pks.length != 1) {
			throw new NucleusDataStoreException(
					"Currently only single field identity objects are allowed");
		}

		AbstractMemberMetaData memberMetaData = metaData
				.getMetaDataForManagedMemberAtAbsolutePosition(pks[0]);

		// Try the first column if specified
		ColumnMetaData[] colmds = memberMetaData.getColumnMetaData();
		if (colmds != null && colmds.length > 0) {
			return Bytes.fromUTF8(colmds[0].getName());
		}

		// TODO should we allow defaults?
		return Bytes.fromUTF8(memberMetaData.getName());

	}

	/**
	 * Get the column name from the meta data, if it's not specified the default
	 * name of "classtype" is returned
	 * 
	 * @param metaData
	 * @return
	 */
	public static Bytes getDiscriminatorColumnName(
			DiscriminatorMetaData metaData) {
		String name = metaData.getColumnName();

		if (name == null) {
			name = "classtype";
		}

		return Bytes.fromUTF8(name);
	}

	/**
	 * Get the column name from the meta data, if it's not specified the default
	 * name of "classtype" is returned
	 * 
	 * @param acmd
	 * @return
	 */
	public static Bytes getDiscriminatorColumnName(AbstractClassMetaData acmd) {
		DiscriminatorMetaData meta = acmd.getDiscriminatorMetaData();

		if (meta == null) {
			return null;
		}

		return getDiscriminatorColumnName(meta);

	}
	


	/**
	 * Get the name of the index. Will return null if no index is defined. If
	 * one is, it takes the name assigned by the user, otherwise it will create
	 * an index in the format of <TableName>_<FieldName>
	 * 
	 * @param metaData
	 * @return
	 */
	public static String getIndexName(AbstractClassMetaData classMetaData,
			AbstractMemberMetaData fieldMetaData) {

		// already indexed, return it
		String name = fieldToIndexNames.get(fieldMetaData);

		if (NULL == name) {
			return null;
		}

		IndexMetaData metaData = fieldMetaData.getIndexMetaData();

		// no index defined, set it to null and cache it
		if (metaData == null) {
			fieldToIndexNames.putIfAbsent(fieldMetaData, NULL);
			return null;

		}

		name = metaData.getName();

		if (name == null) {
			StringBuffer nameBuffer = new StringBuffer();

			nameBuffer.append(fieldMetaData.getName()).append("_index");
			name = nameBuffer.toString();
		}
		
	
		fieldToIndexNames.putIfAbsent(fieldMetaData, name);

		return name;

	}

	/**
	 * Get the byte value of the column names
	 * 
	 * @param metaData
	 * @param absoluteFieldNumber
	 * @return
	 */
	public static Bytes getColumnNameBytes(AbstractClassMetaData metaData,
			int absoluteFieldNumber) {

		return getColumnName(metaData, absoluteFieldNumber);

	}

	/**
	 * Get the column path to the entire class
	 * 
	 * @param metaData
	 * @return
	 */
	public static ColumnPath getClassColumnFamily(AbstractClassMetaData metaData) {
		return new ColumnPath(getColumnFamily(metaData));

	}

	/**
	 * Get the column parent for the given class metadata. Matches the
	 * "table name" on the class meta data
	 * 
	 * @param op
	 * @return
	 */
	public static ColumnParent getColumnParent(AbstractClassMetaData metaData) {
		return new ColumnParent(getColumnFamily(metaData));
	}

	/**
	 * Get the name of the column family. Uses table name, if one doesn't exist,
	 * it uses the simple name of the class. If this class should never be
	 * persisted directly (subclass table persistence) null is returned
	 * 
	 * @param metaData
	 * @return
	 */
	public static String getColumnFamily(AbstractClassMetaData metaData) {

		String passedClassName = metaData.getFullClassName();
		String cfName = classToCfNames.get(passedClassName);

		if (cfName != null) {
			return cfName;
		}

		AbstractClassMetaData current = metaData;
		InheritanceMetaData inheritance = null;
		String tableName = null;

		while (current != null) {
			tableName = current.getTable();

			if (tableName != null) {
				cfName = tableName;
				break;
			}

			inheritance = metaData.getInheritanceMetaData();

			if (inheritance != null) {

				InheritanceStrategy strategy = inheritance.getStrategy();

				if (InheritanceStrategy.NEW_TABLE.equals(strategy)) {
					cfName = metaData.getTable();

					if (cfName == null) {
						cfName = metaData.getEntityName();

					}

					break;
				}

				// this class should never be persisted directly, return null
				else if (InheritanceStrategy.SUBCLASS_TABLE.equals(strategy)) {
					return null;
				}

			}

			current = (AbstractClassMetaData) current
					.getSuperAbstractClassMetaData();

		}

		classToCfNames.put(passedClassName, cfName);

		return cfName;

	}

	/**
	 * Get all discriminators as strings for this class and all possible
	 * subclasses. Will return at a minimum the discriminator for the passed
	 * class
	 * 
	 * @param className
	 * @param clr
	 * @param ec
	 * @return
	 */
	public static List<Bytes> getDescriminatorValues(String className,
			ClassLoaderResolver clr, ExecutionContext ec) {

		List<Bytes> descriminators = classToSubclasses.get(className);

		if (descriminators != null) {
			return descriminators;
		}

		descriminators = new ArrayList<Bytes>();

		MetaDataManager mdm = ec.getMetaDataManager();

		AbstractClassMetaData metaData = mdm
				.getMetaDataForClass(className, clr);

		DiscriminatorMetaData discriminator = metaData
				.getDiscriminatorMetaData();

		descriminators.add(Bytes.fromUTF8(discriminator.getValue()));

		String[] subClasses = mdm.getSubclassesForClass(className, true);

		if (subClasses != null) {

			for (String subclassName : subClasses) {
				metaData = mdm.getMetaDataForClass(subclassName, clr);

				discriminator = metaData.getDiscriminatorMetaData();

				descriminators.add(Bytes.fromUTF8(discriminator.getValue()));
			}
		}

		classToSubclasses.putIfAbsent(className, descriminators);

		return descriminators;
	}
	


	/**
	 * Create a slice predicate with all mapped fetch column lists
	 * 
	 * @param metaData
	 * @param fieldNumbers
	 * @return
	 */
	public static SlicePredicate getFetchColumnList(
			AbstractClassMetaData metaData, int[] fieldNumbers) {

		Bytes[] fieldNames = new Bytes[fieldNumbers.length];

		for (int i = 0; i < fieldNumbers.length; i++) {
			fieldNames[i] = getColumnName(metaData, fieldNumbers[i]);
		}

		return Selector.newColumnsPredicate(fieldNames);
	}

	/**
	 * Create a slice predicate that will retreive the discriminator column if
	 * one doesn't exist, null is returned
	 * 
	 * @param metaData
	 * @param fieldNumbers
	 * @return
	 */
	public static SlicePredicate getDescriminatorColumn(
			AbstractClassMetaData metaData) {

		DiscriminatorMetaData discriminatorMetaData = metaData
				.getDiscriminatorMetaData();

		if (discriminatorMetaData == null) {
			return null;
		}

		Bytes columnName = getDiscriminatorColumnName(discriminatorMetaData);

		return Selector.newColumnsPredicate(columnName);
	}


}
