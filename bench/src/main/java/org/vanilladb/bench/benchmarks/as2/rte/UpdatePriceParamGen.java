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
 
import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.benchmarks.as2.As2BenchTransactionType;
import org.vanilladb.bench.rte.TxParamGenerator;

import org.vanilladb.bench.util.RandomValueGenerator;
import java.util.LinkedList;
import org.vanilladb.bench.benchmarks.as2.rte.jdbc.*;

public class UpdatePriceParamGen implements TxParamGenerator<As2BenchTransactionType> {
	
	private static final int Update_Num = 10;

	@Override
	public As2BenchTransactionType getTxnType() {
		return As2BenchTransactionType.UPDATE_ITEM_PRICE;
	}

	@Override
	public Object[] generateParameter() {
		// [# of items]
		RandomValueGenerator a = new RandomValueGenerator();
		LinkedList<Object> List = new LinkedList<Object>();
		
		for(int i=0;i<Update_Num;i++) {
			int id = a.number(0, As2BenchConstants.NUM_ITEMS);
			double price_raise = a.number(0, 50)/10;
			
			//List.add(new UpdateItemPriceTxnParam(id,price_raise));
			
		}
		return List.toArray();
	}

}
