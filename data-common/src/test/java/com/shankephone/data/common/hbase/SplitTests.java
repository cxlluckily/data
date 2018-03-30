package com.shankephone.data.common.hbase;

import java.math.BigInteger;

public class SplitTests {

	public static void main(String[] args) {
		byte[][] splits = getHexSplits("0000", "FFFF", 3);
		String splitStr = "";
		for (int i = 0; i < splits.length; i++) {
			if (i != 0) {
				splitStr += ",";
			}
			splitStr += "'" + new String(splits[i]) + "'";
		}
		System.out.print(splitStr);
	}

	public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
		byte[][] splits = new byte[numRegions - 1][];
		BigInteger lowestKey = new BigInteger(startKey, 16);
		BigInteger highestKey = new BigInteger(endKey, 16);
		BigInteger range = highestKey.subtract(lowestKey);
		BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
		lowestKey = lowestKey.add(regionIncrement);
		for (int i = 0; i < numRegions - 1; i++) {
			BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
			byte[] b = String.format("%04x", key).getBytes();
			splits[i] = b;
		}
		return splits;
	}
}
