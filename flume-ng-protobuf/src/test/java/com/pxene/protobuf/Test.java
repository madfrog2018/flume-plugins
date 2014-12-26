package com.pxene.protobuf;

import org.apache.log4j.Logger;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Test {
	private final static Logger logger = Logger.getLogger(Test.class);
    public static void main(String[] args) {


		Long lon = 1418971485557L;
		byte[] bytes1 = longToByteArray(lon);
		long aa = byteArrayToInt(bytes1);
		System.out.println(aa);
		Date date1 = new Date(lon);
		System.out.println(date1);

		Map<String, String> map = new HashMap<String, String>();
		String NULL = "NULL";
		String c = "\\|";
		int index = 0;
		int position = 0;
//		map.put("1", NULL);
//		logger.info(map.get("1"));
//		map.put("1", "122");
//		logger.info(map.get("1"));
		StringBuilder sb = new StringBuilder();
		sb.append("1111").append("|").append("2222").append("|").append("3333").append("|").append("4444");
		index = sb.indexOf("|", index);
		position = index -2;
		System.out.println(sb.indexOf("|", 0));
		sb.insert(position, ",aaaa");
		
		String ss = sb.toString();
		System.out.println(ss);
		String[] sss = ss.split(c);
		for (String string : sss) {
			System.out.println(string);
		}
	}

	/**
	 * 将64位的long值放到8字节的byte数组
	 * @param num
	 * @return 返回转换后的byte数组
	 */
	public static byte[] longToByteArray(long num) {
		byte[] result = new byte[8];
		result[0] = (byte) (num >>> 56);// 取最高8位放到0下标
		result[1] = (byte) (num >>> 48);// 取最高8位放到0下标
		result[2] = (byte) (num >>> 40);// 取最高8位放到0下标
		result[3] = (byte) (num >>> 32);// 取最高8位放到0下标
		result[4] = (byte) (num >>> 24);// 取最高8位放到0下标
		result[5] = (byte) (num >>> 16);// 取次高8为放到1下标
		result[6] = (byte) (num >>> 8); // 取次低8位放到2下标
		result[7] = (byte) (num); // 取最低8位放到3下标
		return result;
	}

	/**
	 * 将8字节的byte数组转成一个long值
	 * @param byteArray
	 * @return 转换后的long型数值
	 */
	public static long byteArrayToLong(byte[] byteArray) {
		byte[] a = new byte[8];
		int i = a.length - 1, j = byteArray.length - 1;
		for (; i >= 0; i--, j--) {// 从b的尾部(即int值的低位)开始copy数据
			if (j >= 0)
				a[i] = byteArray[j];
			else
				a[i] = 0;// 如果b.length不足4,则将高位补0
		}
		// 注意此处和byte数组转换成int的区别在于，下面的转换中要将先将数组中的元素转换成long型再做移位操作，
		// 若直接做位移操作将得不到正确结果，因为Java默认操作数字时，若不加声明会将数字作为int型来对待，此处必须注意。
		long v0 = (long) (a[0] & 0xff) << 56;// &0xff将byte值无差异转成int,避免Java自动类型提升后,会保留高位的符号位
		long v1 = (long) (a[1] & 0xff) << 48;
		long v2 = (long) (a[2] & 0xff) << 40;
		long v3 = (long) (a[3] & 0xff) << 32;
		long v4 = (long) (a[4] & 0xff) << 24;
		long v5 = (long) (a[5] & 0xff) << 16;
		long v6 = (long) (a[6] & 0xff) << 8;
		long v7 = (long) (a[7] & 0xff);
		return v0 + v1 + v2 + v3 + v4 + v5 + v6 + v7;
	}

	/**
	 * 将4字节的byte数组转成一个int值
	 * @param b
	 * @return
	 */
	public static int byteArrayToInt(byte[] b){
		byte[] a = new byte[4];
		int i = a.length - 1,j = b.length - 1;
		for (; i >= 0 ; i--,j--) {//从b的尾部(即int值的低位)开始copy数据
			if(j >= 0)
				a[i] = b[j];
			else
				a[i] = 0;//如果b.length不足4,则将高位补0
		}
		int v0 = (a[0] & 0xff) << 24;//&0xff将byte值无差异转成int,避免Java自动类型提升后,会保留高位的符号位
		int v1 = (a[1] & 0xff) << 16;
		int v2 = (a[2] & 0xff) << 8;
		int v3 = (a[3] & 0xff) ;
		return v0 + v1 + v2 + v3;
	}
}
