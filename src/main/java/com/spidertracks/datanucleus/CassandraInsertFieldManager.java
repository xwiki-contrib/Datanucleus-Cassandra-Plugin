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

import static com.spidertracks.datanucleus.utils.MetaDataUtils.getColumnName;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.collection.WriteCollection;
import com.spidertracks.datanucleus.collection.WriteMap;
import com.spidertracks.datanucleus.convert.ByteConverterContext;

/**
 * @author Todd Nine
 * 
 */
public class CassandraInsertFieldManager extends AbstractFieldManager {

	private ExecutionContext context;
	private Selector selector;
	private Mutator mutator;
	private AbstractClassMetaData metaData;
	private ObjectProvider objectProvider;
	private String columnFamily;
	private ByteConverterContext byteContext;
	private Bytes key;

	/**
	 * @param columns
	 * @param metaData
	 */
	public CassandraInsertFieldManager(Selector selector, Mutator mutator,
			ObjectProvider op, String columnFamily, Bytes key) {
		super();

		this.selector = selector;
		this.mutator = mutator;
		this.objectProvider = op;
		this.metaData = op.getClassMetaData();
		this.context = op.getExecutionContext();
		this.byteContext = ((CassandraStoreManager) context.getStoreManager())
				.getByteConverterContext();
		this.columnFamily = columnFamily;
		this.key = key;

	}

	@Override
	public void storeBooleanField(int fieldNumber, boolean value) {

		try {
			mutator.writeColumn(columnFamily, key, mutator.newColumn(
					getColumnName(metaData, fieldNumber),
					byteContext.getBytes(value)));

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeByteField(int fieldNumber, byte value) {

		try {

			mutator.writeColumn(columnFamily, key,
					mutator.newColumn(getColumnName(metaData, fieldNumber),
							Bytes.fromByte(value)));

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeCharField(int fieldNumber, char value) {

		try {
			mutator.writeColumn(columnFamily, key, mutator.newColumn(
					getColumnName(metaData, fieldNumber),
					byteContext.getBytes(value)));

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeDoubleField(int fieldNumber, double value) {

		try {
			mutator.writeColumn(columnFamily, key, mutator.newColumn(
					getColumnName(metaData, fieldNumber),
					byteContext.getBytes(value)));

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeFloatField(int fieldNumber, float value) {

		try {
			mutator.writeColumn(columnFamily, key, mutator.newColumn(
					getColumnName(metaData, fieldNumber),
					byteContext.getBytes(value)));
		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeIntField(int fieldNumber, int value) {

		try {
			mutator.writeColumn(columnFamily, key, mutator.newColumn(
					getColumnName(metaData, fieldNumber),
					byteContext.getBytes(value)));

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeLongField(int fieldNumber, long value) {

		try {
			mutator.writeColumn(columnFamily, key, mutator.newColumn(
					getColumnName(metaData, fieldNumber),
					byteContext.getBytes(value)));

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeShortField(int fieldNumber, short value) {
		try {
			mutator.writeColumn(columnFamily, key, mutator.newColumn(
					getColumnName(metaData, fieldNumber),
					byteContext.getBytes(value)));

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeObjectField(int fieldNumber, Object value) {
		try {

			Bytes columnName = getColumnName(metaData, fieldNumber);

			ClassLoaderResolver clr = context.getClassLoaderResolver();
			AbstractMemberMetaData fieldMetaData = metaData
					.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);

			int relationType = fieldMetaData.getRelationType(clr);

			// check if this is a relationship

			if (relationType == Relation.ONE_TO_ONE_BI
					|| relationType == Relation.ONE_TO_ONE_UNI
					|| relationType == Relation.MANY_TO_ONE_BI) {
				// Persistable object - persist the related object and store the
				// identity in the cell

				if (fieldMetaData.isEmbedded()) {

					throw new NucleusDataStoreException(
							"Embedded objects are unsupported.  Mark the object as persistent and use a serializable class instead");

				}

				// delete operation
				if (value == null) {
					this.mutator.deleteColumn(columnFamily, key, columnName);
					return;
				}

				Object persisted = context.persistObjectInternal(value,
						objectProvider, fieldNumber, StateManager.PC);

				Serializable objectPk = (Serializable) context.getApiAdapter()
						.getIdForObject(persisted);

				mutator.writeColumn(
						columnFamily,
						key,
						mutator.newColumn(columnName,
								byteContext.getRowKeyForId(objectPk)));

				return;
			}

			if (relationType == Relation.MANY_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_UNI) {
				// Collection/Map/Array

				if (fieldMetaData.hasCollection()) {

					WriteCollection collectionWriter = new WriteCollection(
							selector, byteContext, columnFamily, key,
							columnName);

					if (value == null) {
						collectionWriter.removeAllColumns(mutator);
						return;
					}

					Object persisted = null;
					Object objectPk = null;

					for (Object element : (Collection<?>) value) {
						// persist the object
						persisted = context.persistObjectInternal(element,
								objectProvider, fieldNumber, StateManager.PC);

						objectPk = context.getApiAdapter().getIdForObject(
								persisted);

						collectionWriter.writeRelationship(mutator, objectPk);
					}

					// TODO remove this when SCO is working
					collectionWriter.removeRemaining(mutator);

					objectProvider.wrapSCOField(fieldNumber, value, true, true,
							true);

					return;

				} else if (fieldMetaData.hasMap()) {

					WriteMap mapWriter = new WriteMap(selector, byteContext,
							columnFamily, key, columnName);

					if (value == null) {
						mapWriter.removeAllColumns(mutator);
						return;
					}

					ApiAdapter adapter = context.getApiAdapter();

					Map<?, ?> map = ((Map<?, ?>) value);

					// serialized values to store per item
					Object serializedKey = null;
					Object serializedValue = null;

					// value set by the user in the map
					Object mapValue = null;

					// pointer to what we persisted
					Object persisted = null;

					// get each element and persist it.
					for (Object mapKey : map.keySet()) {

						mapValue = map.get(mapKey);

						// handle the case if our key is a persistent class
						// itself
						if (adapter.isPersistable(mapKey)) {

							persisted = context.persistObjectInternal(mapKey,
									objectProvider, fieldNumber,
									StateManager.PC);

							serializedKey = context.getApiAdapter()
									.getIdForObject(persisted);
						} else {
							serializedKey = mapKey;
						}

						// persist the value if it can be persisted
						if (adapter.isPersistable(mapValue)) {

							persisted = context.persistObjectInternal(mapValue,
									objectProvider, fieldNumber,
									StateManager.PC);

							serializedValue = context.getApiAdapter()
									.getIdForObject(persisted);

						} else {
							serializedValue = mapValue;
						}

						mapWriter.writeRelationship(mutator, serializedKey,
								serializedValue);

						// TODO remove this when SCO is working
						mapWriter.removeRemaining(mutator);

					}

					objectProvider.wrapSCOField(fieldNumber, value, true, true,
							true);

					return;

				} else if (fieldMetaData.hasArray()) {

					Object persisted = null;
					Object objectPk = null;

					WriteMap mapWriter = new WriteMap(selector, byteContext,
							columnFamily, key, columnName);

					if (value == null) {
						mapWriter.removeAllColumns(mutator);
						return;
					}

					for (int i = 0; i < Array.getLength(value); i++) {

						// persist the object
						persisted = context.persistObjectInternal(
								Array.get(value, i), objectProvider,
								fieldNumber, StateManager.PC);

						objectPk = context.getApiAdapter().getIdForObject(
								persisted);

						mapWriter.writeRelationship(mutator, i, objectPk);
					}

					// TODO remove this when SCO is working
					mapWriter.removeRemaining(mutator);
				}

				objectProvider.wrapSCOField(fieldNumber, value, true, true,
						true);

				return;
			}

			if (value == null) {
				this.mutator.deleteColumn(columnFamily, key, columnName);
				return;
			}

			Bytes data = byteContext.getBytes(value);

			mutator.writeColumn(columnFamily, key,
					mutator.newColumn(columnName, data));

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}

	@Override
	public void storeStringField(int fieldNumber, String value) {
		try {

			if (value == null) {
				mutator.deleteColumn(columnFamily, key,
						getColumnName(metaData, fieldNumber));
				return;
			}

			mutator.writeColumn(columnFamily, key, mutator.newColumn(
					getColumnName(metaData, fieldNumber),
					byteContext.getBytes(value)));

		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}
	}
}
