/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package umn.dcsg.wieralocalserver.utils;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Utility functions.
 */
public class Utils {
	private static final Random rand = new Random();
	private static final ThreadLocal<Random> rng = new ThreadLocal<>();

	public static Random random() {
		Random ret = rng.get();
		if (ret == null) {
			ret = new Random(rand.nextLong());
			rng.set(ret);
		}

		return ret;
	}

	/**
	 * Generate a random ASCII string of a given length.
	 */
	public static String ASCIIString(int length) {
		int interval = '~' - ' ' + 1;

		byte[] buf = new byte[length];
		random().nextBytes(buf);
		for (int i = 0; i < length; i++) {
			if (buf[i] < 0) {
				buf[i] = (byte) ((-buf[i] % interval) + ' ');
			} else {
				buf[i] = (byte) ((buf[i] % interval) + ' ');
			}
		}
		return new String(buf);
	}

	/**
	 * Hash an integer value.
	 */
	public static long hash(long val) {
		return FNVhash64(val);
	}

	public static final int FNV_offset_basis_32 = 0x811c9dc5;
	public static final int FNV_prime_32 = 16777619;

	/**
	 * 32 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
	 *
	 * @param val The value to hash.
	 * @return The hash value
	 */
	public static int FNVhash32(int val) {
		//from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
		int hashval = FNV_offset_basis_32;

		for (int i = 0; i < 4; i++) {
			int octet = val & 0x00ff;
			val = val >> 8;

			hashval = hashval ^ octet;
			hashval = hashval * FNV_prime_32;
			//hashval = hashval ^ octet;
		}
		return Math.abs(hashval);
	}

	public static final long FNV_offset_basis_64 = 0xCBF29CE484222325L;
	public static final long FNV_prime_64 = 1099511628211L;

	/**
	 * 64 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
	 *
	 * @param val The value to hash.
	 * @return The hash value
	 */
	public static long FNVhash64(long val) {
		//from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
		long hashval = FNV_offset_basis_64;

		for (int i = 0; i < 8; i++) {
			long octet = val & 0x00ff;
			val = val >> 8;

			hashval = hashval ^ octet;
			hashval = hashval * FNV_prime_64;
			//hashval = hashval ^ octet;
		}
		return Math.abs(hashval);
	}

	public static int convertToInteger(Object obj) {
		//Temp code.
		if (obj instanceof Long) {
			return ((Long) obj).intValue();
		} else if (obj instanceof Double) {
			return ((Double) obj).intValue();
		} else {
			return (int) obj;
		}
	}

	public static long convertToLong(Object obj) {
		//Temp code.
		if (obj instanceof Integer) {
			return ((Integer) obj).longValue();
		} else if (obj instanceof Double) {
			return ((Double) obj).longValue();
		} else {
			return (long) obj;
		}
	}

	public static double convertToDouble(Object obj) {
		//Temp code.
		if (obj instanceof Integer) {
			return ((Integer) obj).doubleValue();
		} else if (obj instanceof Long) {
			return ((Long) obj).doubleValue();
		} else {
			return (double) obj;
		}
	}

	public static long getSizeFromHumanReadable(Object size, long defaultSize) {
		long lSize = defaultSize;

		if (size != null) {
			//Set available size for each storage
			if (size instanceof Integer) {
				lSize = (long) size;
			} else if (size instanceof String) {
				String strSize = ((String) size).toUpperCase();
				String[] tokens = strSize.split("(?<=\\d)(?=\\D)");
				String strSizeUnit = tokens[1];
				long lFactor;

				//Parse Human readable size to bytes
				switch(strSizeUnit) {
					case "B":
						lFactor = 1;
						break;
					case "KB":
						lFactor = 1024;
						break;
					case "MB":
						lFactor = 1024 * 1024;
						break;
					case "GB":
						lFactor = 1024 * 1024 * 1024;
						break;
					case "TB":
						lFactor = 1024 * 1024 * 1024 * 1024;
						break;
					default:
						lFactor = 1;
				}

				lSize = Long.parseLong(tokens[0]) * lFactor;
			}
		}

		return lSize;
	}

	public static byte[] createDummyData(long size) {
		StringBuilder buff = new StringBuilder();

		for (int i = 0; i < size; i++) {
			buff.append(i % 10);
		}

		try {
			return buff.toString().getBytes("US-ASCII");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static double getPercentile(double[] list, double lPercentile, double lDefaultIfNotExist) {
		if (list.length == 0 || lPercentile == 0) {
			return lDefaultIfNotExist;
		}

		Percentile percentile = new Percentile();
		Arrays.sort(list);
		percentile.setData(list);

		return percentile.evaluate(lPercentile);
	}
}