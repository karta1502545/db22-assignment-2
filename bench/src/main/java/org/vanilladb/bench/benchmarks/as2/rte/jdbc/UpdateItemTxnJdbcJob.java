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
		// TODO
		// UpdateItemProcParamHelper paramHelper = new UpdateItemProcParamHelper();
		// paramHelper.prepareParameters(pars);

		// Parse parameter
		int updateCount = (Integer) pars[0];
		int[] itemIds = new int[updateCount];
		double[] raises = new double[updateCount];
		for (int i = 0; i < updateCount; i++) {
			itemIds[i] = (Integer) pars[++i];
			raises[i] = (double) ((Integer) pars[i + updateCount + 1]) / 10.0;
		}

		// Output message
		StringBuilder outputMsg = new StringBuilder("[");

		// Execute logic
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = null;

			// SELECT
			for (int i = 0; i < 10; i++) {
				String sql = "SELECT i_name FROM item WHERE i_id = " + itemIds[i];

				rs = statement.executeQuery(sql);
				rs.beforeFirst();
				if (rs.next()) {
					outputMsg.append(String.format("'%s', ", rs.getString("i_name")));
					double price = rs.getDouble("i_price");
					if (price < As2BenchConstants.MAX_PRICE) {
						price = Double.sum(price, raises[i]);
						sql = "Update item SET i_price = " + price + " WHERE i_id = " + itemIds[i];
					} else {
						sql = "Update item SET i_price = " + As2BenchConstants.MIN_PRICE + " WHERE i_id = "
								+ itemIds[i];
					}
					statement.executeUpdate(sql);
				} else
					throw new RuntimeException("cannot find the record with i_id = " + itemIds[i]);
				rs.close();
			}

			conn.commit();

			outputMsg.deleteCharAt(outputMsg.length() - 2);
			outputMsg.append("]");

			return new VanillaDbJdbcResultSet(true, outputMsg.toString());
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(e.toString());
			return new VanillaDbJdbcResultSet(false, "");
		}
	}
}