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
package org.vanilladb.bench.benchmarks.as2.rte.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.bench.remote.jdbc.VanillaDbJdbcResultSet;
import org.vanilladb.bench.rte.jdbc.JdbcJob;
import org.vanilladb.bench.server.param.as2.UpdateItemProcParamHelper;
import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;

public class UpdateItemTxnJdbcJob implements JdbcJob {
	private static Logger logger = Logger.getLogger(UpdateItemTxnJdbcJob.class.getName());

	@Override
	public SutResultSet execute(Connection conn, Object[] pars) throws SQLException {
		// modified by bobby
		UpdateItemProcParamHelper paramHelper = new UpdateItemProcParamHelper();
		paramHelper.prepareParameters(pars);

		double maxPrice = As2BenchConstants.MAX_PRICE;
		double minPrice = As2BenchConstants.MIN_PRICE;

		// Output message
		StringBuilder outputMsg = new StringBuilder("[");

		// Execute logic
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = null;
			
			// TODO: distinguish sources of "raises" and "itemPrice"
			// SELECT
			for (int i = 0; i < paramHelper.getUpdateCount(); i++) {
				int itemId = paramHelper.getUpdateItemId(i);
				String sql = "SELECT i_name FROM item WHERE i_id = " + paramHelper.getUpdateItemId(i);
				double price;
				rs = statement.executeQuery(sql);
				rs.beforeFirst();
				if (rs.next()) {
					outputMsg.append(String.format("'%s', ", rs.getString("i_name")));
					price = rs.getDouble("i_price");
				} else
					throw new RuntimeException("cannot find the record with i_id = " + itemId);
				rs.close();
				
				
				
				Double updatePrice;
				if(price > maxPrice) {
					updatePrice = minPrice;
				}
				else {
					updatePrice = Double.sum(price, paramHelper.getRaise(i));
				}
				sql = "Update item SET i_price = " + updatePrice + " WHERE i_id = " + itemId;
				int result = statement.executeUpdate(sql);
				if(result == 0) {
					throw new RuntimeException("cannot update the record with i_id = " + itemId);
				}
			}
			conn.commit();

			outputMsg.deleteCharAt(outputMsg.length() - 2);
			outputMsg.append("]");

			return new VanillaDbJdbcResultSet(true, outputMsg.toString());
		}catch(

	Exception e)
	{
		if (logger.isLoggable(Level.WARNING))
			logger.warning(e.toString());
		return new VanillaDbJdbcResultSet(false, "");
	}
}
}