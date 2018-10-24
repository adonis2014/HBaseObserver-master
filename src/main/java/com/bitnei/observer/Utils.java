package com.bitnei.observer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * ClassName: Utils 
 * @Description: 工具类
 * @author luhaiyou
 * @date 2015年7月9日
 */
public class Utils {
	/**
	 * 
	 * @Description: 把不包含{}的json串解析成map
	 * @param @param message json串
	 * @param @return   
	 * @return Map<String,String>  
	 * @throws
	 * @author luhaiyou
	 * @date 2015年7月9日
	 */
	public static Map<String,String>  processMessageToMap(String message){
		//把{}去掉
		if(message.startsWith("{")){
			message = message.substring(1);
		}
		if(message.endsWith("}")){
			message = message.substring(0, message.length()-1);
		}
		
		Map<String,String> map = new HashMap<>();
		String[] keyValues = message.split(",");
		for(String kv : keyValues){
			String[] fields = kv.split(":");
			if(fields.length==1){
				map.put(StringUtils.trim(fields[0]).replaceAll("\"", ""), "");
			}else{
				map.put(StringUtils.trim(fields[0]).replaceAll("\"", ""), StringUtils.trim(fields[1]).replaceAll("\"", ""));
			}
		}
		return map;
	}
	/**
	 * @throws ParseException 
	 * 类描述:根据字符串得到年月日路径  
	 * 创建人:luhaiyou   
	 * 创建时间:2015年7月20日 下午2:19:45  
	 * @param @param time  20150717173636
	 * @return String  2015/07/17  如果错误则返回 9999/99/99
	 * @throws
	 */
	public static String getTimePath(String time) throws ParseException{
		SimpleDateFormat dirSdf = new SimpleDateFormat("yyyy/MM/dd");
		SimpleDateFormat timeSdf = new SimpleDateFormat("yyyyMMddHHmmss");
		if(time.length()=="150714140400".length()){
			time = "20"+time;
		}
		String dir = dirSdf.format(timeSdf.parse(time));
		return dir;
		
	}
	/**
	 * 类描述:字符串时间转换成UTC时间
	 * 创建人:luhaiyou   
	 * 创建时间:2015年7月20日 下午2:25:22  
	 * @param @param time 20150717173636
	 * @param @throws ParseException
	 * @return long  毫秒值
	 * @throws
	 */
	public static long convertToUTC(String time) throws ParseException{
		if(time.length()=="150714140400".length()){
			time = "20"+time;
		}
		SimpleDateFormat timeSdf = new SimpleDateFormat("yyyyMMddHHmmss");
		return timeSdf.parse(time).getTime();
	}
}
