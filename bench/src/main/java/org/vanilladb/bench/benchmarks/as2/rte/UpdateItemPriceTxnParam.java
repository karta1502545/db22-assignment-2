package org.vanilladb.bench.benchmarks.as2.rte;

import java.io.Serializable;

public class UpdateItemPriceTxnParam implements Serializable {
	public int itemId;
	public double raise;
	
	private static final long serialVersionUID=1l;
	
	public UpdateItemPriceTxnParam(int itemId, double raise) {
		this.itemId = itemId;
		this.raise = raise;
	}
}
