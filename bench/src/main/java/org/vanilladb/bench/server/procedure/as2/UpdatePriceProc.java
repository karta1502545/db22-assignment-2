/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.bench.server.procedure.as2;

import org.vanilladb.bench.server.param.as2.UpdateItemProcParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureHelper;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;

public class UpdatePriceProc extends StoredProcedure<UpdateItemProcParamHelper> {

	public UpdatePriceProc() {
		super(new UpdateItemProcParamHelper());
	}

	@Override
	protected void executeSql() {
		UpdateItemProcParamHelper paramHelper = getParamHelper();
		Transaction tx = getTransaction();

		double maxPrice = As2BenchConstants.MAX_PRICE;
		double minPrice = As2BenchConstants.MIN_PRICE;

		// SELECT
		for (int idx = 0; idx < paramHelper.getUpdateCount(); idx++) {
			int itemId = paramHelper.getUpdateItemId(idx);
			
			String sql = "SELECT i_name, i_price FROM item WHERE i_id = " + itemId;
			Scan s = StoredProcedureHelper.executeQuery(sql, tx);
			double price;
			s.beforeFirst();
			if (s.next()) {
				price = (Double) s.getVal("i_price").asJavaVal();
			} else
				throw new RuntimeException("Could not find item record with i_id = " + itemId);

			s.close();

			Double updatePrice;
			if(price > maxPrice) {
				updatePrice = minPrice;
			}
			else {
				updatePrice = Double.sum(price, paramHelper.getRaise(idx));
			}

			sql = "Update item SET i_price = " + updatePrice + " WHERE i_id = " + itemId;
			int result = StoredProcedureHelper.executeUpdate(sql, tx);
			if(result == 0) {
				throw new RuntimeException("cannot update the record with i_id = " + itemId);
			}
			// tx.commit();
			// executeSQL之後回到上一層會commit
		}
	}
}
