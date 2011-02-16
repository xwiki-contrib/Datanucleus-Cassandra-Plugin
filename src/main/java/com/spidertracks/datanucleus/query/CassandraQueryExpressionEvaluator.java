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
package com.spidertracks.datanucleus.query;

import static com.spidertracks.datanucleus.utils.MetaDataUtils.getColumnName;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.query.evaluator.AbstractExpressionEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spidertracks.datanucleus.convert.ByteConverterContext;
import com.spidertracks.datanucleus.query.runtime.AndOperand;
import com.spidertracks.datanucleus.query.runtime.CompressableOperand;
import com.spidertracks.datanucleus.query.runtime.EqualityOperand;
import com.spidertracks.datanucleus.query.runtime.Operand;
import com.spidertracks.datanucleus.query.runtime.OrOperand;

/**
 * Class that will recursively query and merge results from our tree as we're
 * visited. Supports basic result set building from secondary indexes. Will work
 * with < > == && || and limits. Everything else comes from the in memory
 * evaluator because it can't be evaluated with result sets from cassandra
 * 
 * @author Todd Nine
 * 
 */
public class CassandraQueryExpressionEvaluator extends
		AbstractExpressionEvaluator {

	private static final Logger logger = LoggerFactory
			.getLogger(CassandraQueryExpressionEvaluator.class);

	private Stack<IndexParam> indexKeys = new Stack<IndexParam>();
	private Stack<Operand> operationStack = new Stack<Operand>();

	private AbstractClassMetaData metaData;

	/** Map of input parameter values, keyed by their name. */
	private Map<String, Object> parameterValues;

	
	private int maxSize;
	
	private ByteConverterContext byteConverter;
	

	/**
	 * Constructor for an in-memory evaluator.
	 * 
	 * @param ec
	 *            ExecutionContext
	 * @param params
	 *            Input parameters
	 * @param state
	 *            Map of state values keyed by their symbolic name
	 * @param imports
	 *            Any imports
	 * @param clr
	 *            ClassLoader resolver
	 * @param candidateAlias
	 *            Alias for the candidate class. With JDOQL this is "this".
	 */
	public CassandraQueryExpressionEvaluator(AbstractClassMetaData metaData, int maxSize, ByteConverterContext byteConverter, Map<String, Object> params) {
		this.metaData = metaData;
		this.parameterValues = (params != null ? params
				: new HashMap<String, Object>());
		
		this.maxSize = maxSize;
		this.byteConverter = byteConverter;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processAndExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processAndExpression(Expression expr) {
		logger.debug("Processing && expression {}", expr);

		// get our current left and right
		Operand left = operationStack.pop();
		Operand right = operationStack.pop();

		// compress the right and left on this && into a single statement for
		// efficiency
		if (left instanceof CompressableOperand
				&& right instanceof CompressableOperand) {
			
			EqualityOperand op = new EqualityOperand(maxSize);

			op.addAll(((CompressableOperand) left).getIndexClause()
					.getExpressions());

			op.addAll(((CompressableOperand) right).getIndexClause()
					.getExpressions());

			return operationStack.push(op);
		}

		// we can't compress, just add the left and right
		AndOperand op = new AndOperand();
		op.setLeft(left);
		op.setRight(right);

		return operationStack.push(op);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processEqExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processEqExpression(Expression expr) {

		logger.debug("Processing == expression {}", expr);

		// get our corresponding index name from the stack
		IndexParam indexKey = getIndexKeyResult();

		IndexExpression expression = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.EQ,
				indexKey.getIndexValue());

		EqualityOperand op = new EqualityOperand(maxSize);
		op.addExpression(expression);

		return this.operationStack.push(op);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processGteqExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processGteqExpression(Expression expr) {
		// get our corresponding index name from the stack
		IndexParam indexKey = getIndexKeyResult();

		IndexExpression expression = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.GTE,
				indexKey.getIndexValue());

		EqualityOperand op = new EqualityOperand(maxSize);
		op.addExpression(expression);

		return this.operationStack.push(op);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processGtExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processGtExpression(Expression expr) {
		// get our corresponding index name from the stack
		IndexParam indexKey = getIndexKeyResult();

		IndexExpression expression = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.GT,
				indexKey.getIndexValue());

		EqualityOperand op = new EqualityOperand(maxSize);
		op.addExpression(expression);

		return this.operationStack.push(op);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processLteqExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processLteqExpression(Expression expr) {
		// get our corresponding index name from the stack
		IndexParam indexKey = getIndexKeyResult();

		IndexExpression expression = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.LTE,
				indexKey.getIndexValue());

		EqualityOperand op = new EqualityOperand(maxSize);
		op.addExpression(expression);

		return this.operationStack.push(op);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processLtExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processLtExpression(Expression expr) {
		// get our corresponding index name from the stack
		IndexParam indexKey = getIndexKeyResult();

		IndexExpression expression = Selector.newIndexExpression(
				indexKey.getIndexName(), IndexOperator.LT,
				indexKey.getIndexValue());

		EqualityOperand op = new EqualityOperand(maxSize);
		op.addExpression(expression);

		return this.operationStack.push(op);

	}

	

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processOrExpression(org.datanucleus.query.expression.Expression)
	 */
	@Override
	protected Object processOrExpression(Expression expr) {
		logger.debug("Processing || expression {}", expr);

		// get our current left and right
		Operand left = operationStack.pop();
		Operand right = operationStack.pop();

		// we can't compress, just add the left and right
		OrOperand op = new OrOperand();
		op.setLeft(left);
		op.setRight(right);

		return operationStack.push(op);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processParameterExpression
	 * (org.datanucleus.query.expression.ParameterExpression)
	 */
	@Override
	protected Object processParameterExpression(ParameterExpression expr) {
		logger.debug("Processing expression param {}", expr);

		Object value = QueryUtils.getValueForParameterExpression(
				parameterValues, expr);

		Bytes byteVal = byteConverter.getBytes(value);

		IndexParam param = indexKeys.peek();

		param.setIndexValue(byteVal);

		return param;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.datanucleus.query.evaluator.AbstractExpressionEvaluator#
	 * processPrimaryExpression
	 * (org.datanucleus.query.expression.PrimaryExpression)
	 */
	@Override
	protected Object processPrimaryExpression(PrimaryExpression expr) {

		// should be the root object return the value on the set
		logger.debug("Processing expression primary {}", expr);

		AbstractMemberMetaData member = metaData.getMetaDataForMember(expr
				.getSymbol().getQualifiedName());

		Bytes columnName = getColumnName(metaData,
				member.getAbsoluteFieldNumber());

		IndexParam param = new IndexParam(columnName, null);

		return indexKeys.push(param);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLiteral
	 * (org.datanucleus.query.expression.Literal)
	 */
	@Override
	protected Object processLiteral(Literal expr) {
		logger.debug("Processing expression primary {}", expr);

		Object value = expr.getLiteral();

		Bytes byteVal =  byteConverter.getBytes(value);

		IndexParam param = indexKeys.peek();

		param.setIndexValue(byteVal);

		return param;
	}



	/**
	 * Get the index value off the stack. Will only pop if the stack sizes are
	 * equal
	 * 
	 * @return
	 */
	private IndexParam getIndexKeyResult() {
		if (indexKeys.size() == 0) {
			return null;
		}

		return indexKeys.pop();
	}

	

	/**
	 * Helper class to wrap indexing functinality
	 * 
	 * @author Todd Nine
	 * 
	 */
	private class IndexParam {
		private Bytes indexName;
		private Bytes indexValue;

		private IndexParam(Bytes indexName, Bytes indexValue) {
			super();
			this.indexName = indexName;
			this.indexValue = indexValue;
		}

		/**
		 * @return the indexName
		 */
		public Bytes getIndexName() {
			return indexName;
		}

		/**
		 * @return the indexValue
		 */
		public Bytes getIndexValue() {
			return indexValue;
		}

		/**
		 * 
		 * @param indexValue
		 */
		public void setIndexValue(Bytes indexValue) {
			this.indexValue = indexValue;
		}

	}

}
