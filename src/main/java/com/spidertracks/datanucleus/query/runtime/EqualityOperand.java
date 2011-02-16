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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.datanucleus.exceptions.NucleusException;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.client.Consistency;

/**
 * @author Todd Nine
 * 
 */
public class EqualityOperand extends Operand implements CompressableOperand {

	private IndexClause clause;

	public EqualityOperand(int count) {
		clause = new IndexClause();
		clause.setStart_key(new byte[] {});
		clause.setCount(count);
		candidateKeys = new LinkedHashSet<Columns>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.spidertracks.datanucleus.query.runtime.Operand#complete(com.spidertracks
	 * .datanucleus.query.runtime.Operand)
	 */
	@Override
	public void complete(Operand child) {
		throw new UnsupportedOperationException(
				"Equality operands should have no children");
	}

	/**
	 * Add the index expression to the clause
	 * 
	 * @param expression
	 */
	public void addExpression(IndexExpression expression) {
		clause.addToExpressions(expression);
	}

	/**
	 * Add all expression to the index clause
	 * 
	 * @param expressions
	 */
	public void addAll(List<IndexExpression> expressions) {
		for (IndexExpression expr : expressions) {
			clause.addToExpressions(expr);
		}
	}

	@Override
	public IndexClause getIndexClause() {
		return clause;
	}

	@Override
	public void performQuery(String poolName, String cfName, Bytes[] columns) {

		try {
			Map<Bytes, List<Column>> results = Pelops.createSelector(poolName)
					.getIndexedColumns(cfName, clause,
							Selector.newColumnsPredicate(columns),
							Consistency.get());
			Columns cols;

			for (Entry<Bytes, List<Column>> entry : results.entrySet()) {

				if (entry.getValue().size() == 0) {
					continue;
				}

				cols = new Columns(entry.getKey());

				for (Column currentCol : entry.getValue()) {

					cols.addResult(currentCol);
				}

				super.candidateKeys.add(cols);
			}

		} catch (Exception e) {
			throw new NucleusException("Error processing secondary index", e);
		}

		// signal to the parent node the query completed
		if (parent != null) {
			parent.complete(this);
		}

	}

	@Override
	public Operand optimizeDescriminator(Bytes descriminatorColumnValue,
			List<Bytes> possibleValues) {

		// the equality node is always a leaf, so we don't need to recurse

		if (possibleValues.size() == 1) {

			IndexExpression leaf = new IndexExpression();

			leaf.setColumn_name(descriminatorColumnValue.getBytes());

			leaf.setValue(possibleValues.get(0).toByteArray());

			leaf.setOp(IndexOperator.EQ);
			
			addExpression(leaf);

			return this;
		}

		Stack<EqualityOperand> eqOps = new Stack<EqualityOperand>();

		Stack<OrOperand> orOps = new Stack<OrOperand>();

		for (Bytes value : possibleValues) {

			if (orOps.size() == 2) {
				OrOperand orOp = new OrOperand();
				orOp.setLeft(orOps.pop());
				orOp.setRight(orOps.pop());

				orOps.push(orOp);
			}

			if (eqOps.size() == 2) {
				OrOperand orOp = new OrOperand();
				orOp.setLeft(eqOps.pop());
				orOp.setRight(eqOps.pop());
				orOps.push(orOp);
			}

			EqualityOperand subClass = new EqualityOperand(clause.getCount());

			// add the existing clause
			subClass.addAll(this.getIndexClause().getExpressions());

			IndexExpression expression = new IndexExpression();

			expression.setColumn_name(descriminatorColumnValue.getBytes());

			expression.setValue(value.getBytes());

			expression.setOp(IndexOperator.EQ);

			// now add the discriminator
			subClass.addExpression(expression);

			// push onto the stack
			eqOps.push(subClass);

		}

		// only rewritten without needing to OR to other clauses, short circuit

		while (eqOps.size() > 0) {

			OrOperand orOp = new OrOperand();

			if (eqOps.size() % 2 == 0) {
				orOp.setLeft(eqOps.pop());
				orOp.setRight(eqOps.pop());

			}

			else {
				orOp.setLeft(eqOps.pop());
				orOp.setRight(orOps.pop());
			}

			orOps.push(orOp);
		}

		while (orOps.size() > 1) {

			OrOperand orOp = new OrOperand();

			orOp.setLeft(orOps.pop());
			orOp.setRight(orOps.pop());

			orOps.push(orOp);
		}

		// check if there's anything left in the eqOps.

		return orOps.pop();

	}

}
