package org.vanilladb.bench.benchmarks.as2.rte;

import java.util.LinkedList;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.benchmarks.as2.As2BenchTransactionType;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.RandomValueGenerator;

public class As2UpdateItemPriceTxnParamGen implements TxParamGenerator<As2BenchTransactionType> {
	private static final int WRITE_COUNT = 10;
	private static final int MAX_RAISE = 50;

	@Override
	public As2BenchTransactionType getTxnType() {
		return As2BenchTransactionType.UPDATE_ITEM_PRICE;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		LinkedList<Object> paramList = new LinkedList<Object>();

		paramList.add(WRITE_COUNT);

		for (int i = 0; i < WRITE_COUNT; i++) {
			int itemId = rvg.number(1, As2BenchConstants.NUM_ITEMS);
			double raise = ((double) rvg.number(0, MAX_RAISE)) / 10;

			paramList.add(new UpdateItemPriceTxnParam(itemId, raise));
		}

		return paramList.toArray();
	}
}
