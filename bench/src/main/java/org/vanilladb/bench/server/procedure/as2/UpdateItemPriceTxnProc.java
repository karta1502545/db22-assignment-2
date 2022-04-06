package org.vanilladb.bench.server.procedure.as2;

import org.vanilladb.bench.server.param.as2.UpdateItemPriceProcParamHelper;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class UpdateItemPriceTxnProc extends StoredProcedure<UpdateItemPriceProcParamHelper> {

	public UpdateItemPriceTxnProc() {
		super(new UpdateItemPriceProcParamHelper());
	}

	@Override
	protected void executeSql() {
		UpdateItemPriceProcParamHelper paramHelper = getParamHelper();
		Transaction tx = getTransaction();

		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getItemId(idx);

			Plan p = VanillaDb.newPlanner().createQueryPlan("SELECT i_name, i_price FROM item WHERE i_id = " + iid, tx);
			Scan s = p.open();
			s.beforeFirst();
			if (s.next()) {
				String name = (String) s.getVal("i_name").asJavaVal();
				double price = (Double) s.getVal("i_price").asJavaVal();

				paramHelper.setItemName(name, idx);
				paramHelper.setItemPrice(price, idx);
			} else
				throw new RuntimeException("Cloud not find item record with i_id = " + iid);

			s.close();
			// Update part
			int result = VanillaDb.newPlanner()
					.executeUpdate("UPDATE item SET i_price = " + paramHelper.getUpdatedItemPrice(idx) + " WHERE i_id = " + iid, tx);
			if (result == 0) {
				throw new RuntimeException("Could not update item record with i_id = " + iid);
			}
		}
	}
}
