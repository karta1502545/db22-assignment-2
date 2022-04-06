package org.vanilladb.bench.server.param.as2;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.benchmarks.as2.rte.UpdateItemPriceTxnParam;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class UpdateItemPriceProcParamHelper extends StoredProcedureParamHelper{
	// Parameters
	private int readCount;
	private int[] itemIds;
	private double[] raises;
	private String[] itemNames;

	// Results
	private double[] itemPrices;

	public int getReadCount() {
		return readCount;
	}

	public int getItemId(int index) {
		return itemIds[index];
	}

	public void setItemName(String s, int idx) {
		itemNames[idx] = s;
	}

	public void setItemPrice(double d, int idx) {
		itemPrices[idx] = d;
	}

	public double getUpdatedItemPrice(int idx) {
		double originalPrice = itemPrices[idx] ;
		return (Double) (originalPrice > As2BenchConstants.MAX_PRICE ? As2BenchConstants.MIN_PRICE : originalPrice + raises[idx]);
	}

	@Override
	public void prepareParameters(Object... pars) {

		// Show the contents of paramters
		// System.out.println("Params: " + Arrays.toString(pars));

		int indexCnt = 0;

		readCount = (Integer) pars[indexCnt++];
		itemIds = new int[readCount];
		itemNames = new String[readCount];
		itemPrices = new double[readCount];
		raises = new double[readCount];

		for (int i = 0; i < readCount; i++) {
			itemIds[i] = (Integer) (((UpdateItemPriceTxnParam) pars[indexCnt]).itemId);
			raises[i] = (Double) (((UpdateItemPriceTxnParam) pars[indexCnt]).raise);
			indexCnt++;
		}
	}

	@Override
	public Schema getResultSetSchema() {
		Schema sch = new Schema();

		Type intType = Type.INTEGER;
		Type itemPriceType = Type.DOUBLE;
		Type itemNameType = Type.VARCHAR(24);

		sch.addField("rc", intType);

		for (int i = 0; i < itemNames.length; i++) {
			sch.addField("i_name_" + i, itemNameType);
			sch.addField("i_price_" + i, itemPriceType);
		}

		return sch;
	}

	@Override
	public SpResultRecord newResultSetRecord() {
		SpResultRecord rec = new SpResultRecord();

		rec.setVal("rc", new IntegerConstant(itemNames.length));
		for (int i = 0; i < itemNames.length; i++) {
			rec.setVal("i_name_" + i, new VarcharConstant(itemNames[i], Type.VARCHAR(24)));
			rec.setVal("i_price_" + i, new DoubleConstant(itemPrices[i]));
		}

		return rec;
	}
}
