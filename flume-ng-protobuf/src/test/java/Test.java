
public class Test {

	public static void main(String[] args) {
		
		String ss = "1419068159320|3|9278538b1d940c4d0aa1859f7ea206e5|1|^B|^B|93.21.188.97|^B|480|^B|1|0|^B|0|^B|^B|http://clickthrough.domain7.com^Ahttp://clickthrough.domain2.com|^B|^B|0|^B|^B|^B|0|mm_123_456_789|400x208|1|103|9^A4|71|SCREEN_OTHER|5|^B|^B|72002^A70406^A70602^A70707^A71902^A70207|0|^B|^B|true|^B|false|^B|com.sotu|android|zte|zte+u960s3|android|4.0.4|3|1|||728x480|800|^B|^B|^B|^B|^B|^B|^B|^B|^B|^B|^B";
//		String ss1 = ",,,1";
		String[] str = ss.split("\\|");
		for (String string : str) {
			System.out.println(string);
		}
		System.out.println(str.length);
	}
}
