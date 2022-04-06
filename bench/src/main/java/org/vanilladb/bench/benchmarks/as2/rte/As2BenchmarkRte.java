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
package org.vanilladb.bench.benchmarks.as2.rte;

import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.benchmarks.as2.As2BenchTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.RandomValueGenerator;

public class As2BenchmarkRte extends RemoteTerminalEmulator<As2BenchTransactionType> {
	
	private As2BenchmarkTxExecutor executor;
	private static final int precision = 100;

	public As2BenchmarkRte(SutConnection conn, StatisticMgr statMgr, long sleepTime) {
		super(conn, statMgr, sleepTime);
	}
	
	protected As2BenchTransactionType getNextTxType() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		
		// flag would be 100 if READ_WRITE_TX_RATE is 1.0
		int flag = (int) (As2BenchConstants.READ_WRITE_TX_RATE * precision);

		if (rvg.number(0, precision - 1) < flag) {
			return As2BenchTransactionType.READ_ITEM;
		} else {
			return As2BenchTransactionType.UPDATE_ITEM_PRICE;
		}
	}
	
	protected As2BenchmarkTxExecutor getTxExeutor(As2BenchTransactionType type) {
		TxParamGenerator<As2BenchTransactionType> paraGen;
		switch (type) {
		case READ_ITEM:
			paraGen = new As2ReadItemParamGen();
			break;

		case UPDATE_ITEM_PRICE:
			paraGen = new As2UpdateItemPriceTxnParamGen();
			break;

		default:
			paraGen = new As2ReadItemParamGen();
			break;
		}
		executor = new As2BenchmarkTxExecutor(paraGen);
		return executor;
	}
}
