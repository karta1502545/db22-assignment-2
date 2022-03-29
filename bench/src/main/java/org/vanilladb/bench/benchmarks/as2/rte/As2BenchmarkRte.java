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

import org.vanilladb.bench.util.RandomValueGenerator;
import org.vanilladb.bench.rte.*;

public class As2BenchmarkRte extends RemoteTerminalEmulator<As2BenchTransactionType> {
	
	private As2BenchmarkTxExecutor executor;

	public As2BenchmarkRte(SutConnection conn, StatisticMgr statMgr, long sleepTime) {
		super(conn, statMgr, sleepTime);
		executor = new As2BenchmarkTxExecutor(new As2ReadItemParamGen());
	}
	
	protected As2BenchTransactionType getNextTxType() {
		RandomValueGenerator rand = new RandomValueGenerator();
		
		if(rand.number(0, 99) < As2BenchConstants.Read_Write_Tx_Rate * 100) {
			return As2BenchTransactionType.READ_ITEM;
		}else
			return As2BenchTransactionType.UPDATE_ITEM_PRICE;
	}
	
	protected As2BenchmarkTxExecutor getTxExeutor(As2BenchTransactionType type) {
		TxParamGenerator<As2BenchTransactionType> argu;
		
		if(type == As2BenchTransactionType.UPDATE_ITEM_PRICE) {
			argu = new UpdatePriceParamGen();
		}else{
			argu = new As2ReadItemParamGen();
		}
		executor = new As2BenchmarkTxExecutor(argu);
		return executor;
	}
}
