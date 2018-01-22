package function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class test {

	public static void main(String[] args) throws Exception {
		SparkConf conf=new SparkConf().setAppName("CanTimeReducer").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//1506787111000 0x01 0x22 0x122 0x201
		TelegramHash telegramHash1 = new TelegramHash("1111",1506787111000L);
		CanUnitBean	canUnitBean11 = createCanUnitBean((short)0x01,"1506787111010");
		CanUnitBean	canUnitBean12 = createCanUnitBean((short)0x22,"1506787111020");
		CanUnitBean	canUnitBean13 = createCanUnitBean((short)0x122,"1506787111030");
		CanUnitBean	canUnitBean14 = createCanUnitBean((short)0x201,"1506787111050");

		//1506787222000 0x01 0x22 0x122 0x201
		TelegramHash telegramHash2 = new TelegramHash("2222",1506787112001L);
		CanUnitBean	canUnitBean21 = createCanUnitBean((short)0x01,"1506787222010");
		CanUnitBean	canUnitBean22 = createCanUnitBean((short)0x22,"1506787222020");
		CanUnitBean	canUnitBean23 = createCanUnitBean((short)0x122,"1506787222030");
		CanUnitBean	canUnitBean24 = createCanUnitBean((short)0x201,"1506787222040");

		JavaRDD<Tuple2<TelegramHash, CanUnitBean>> rdd = sc.parallelize(Arrays.asList(
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean11),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean12),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean13),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean14),

				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean21),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean22),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean23),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean24)
				),2);

		JavaPairRDD<TelegramHash, CanUnitBean> pairRDD = JavaPairRDD.fromJavaRDD(rdd);

		JavaPairRDD<TelegramHash, CanUnitBean> pairRDDReducer =
				pairRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<TelegramHash, CanUnitBean>>,TelegramHash, CanUnitBean>(){

					
					@Override
					public Iterable<Tuple2<TelegramHash, CanUnitBean>> call(
							Iterator<Tuple2<TelegramHash, CanUnitBean>> its) throws Exception {

						ArrayList<Tuple2<TelegramHash, CanUnitBean>> tuble2s = new ArrayList<>();
						
		                while (its.hasNext()) {
		                	Tuple2<TelegramHash, CanUnitBean> next = its.next();
		                	tuble2s.add(new Tuple2<TelegramHash, CanUnitBean>(next._1,next._2));
		                }
		                
						return tuble2s;
					}
					
					
				}
				);
		dataConsolidation(pairRDDReducer);

		pairRDDReducer.foreach(new VoidFunction<Tuple2<TelegramHash, CanUnitBean>>() {
			
            @Override
            public void call(Tuple2<TelegramHash, CanUnitBean> tp2) throws Exception {
                System.out.println(tp2._1.deviceId + "->" + tp2._2.getCanId() + "->" + tp2._2.getCanTime() + tp2._2.getConvertedDataMap().toString());
            }
        });
	}
	
	private static CanUnitBean createCanUnitBean(short id ,String canTime) {
		CanUnitBean bean = new CanUnitBean();
		bean.setCanId(id);
		bean.setCanTime(Long.parseLong(canTime));
		Map<String,Object> values = new HashMap<>();

		switch(id) {
			case 0x01:
			  values.put("dummy15", 1L);
			  values.put("dummy16", 2L);
			  values.put("dummy17", 3L);
			  break;
			case 0x22:
			  values.put("dummy15", 1L);
			  values.put("dummy16", 2L);
			  values.put("dummy17", 3L);
			  values.put("dummy18", 4L);
			  break;
			case 0x122:
			  values.put("dummy15", 1L);
			  values.put("dummy16", 2L);
			  values.put("dummy17", 3L);
			  values.put("dummy18", 4L);
			  values.put("dummy19", 5L);
			  break;
			case 0x201:
			  values.put("dummy15", 1L);
			  values.put("dummy16", 2L);
			  values.put("dummy17", 3L);
			  values.put("dummy18", 4L);
			  values.put("dummy19", 5L);
			  values.put("dummy20", 6L);
			  break;
		}
		bean.setConvertedDataMap(values);
		return bean;
	}
	
	public static void dataConsolidation(JavaPairRDD<TelegramHash, CanUnitBean> pairRDDReducer) throws Exception {
		String deviceIdCurr = null;
		String reliveTimeCurr = null;
		String deviceIdTime = null;
		Short canIdCurr = 0;
		long  a = 1L;
		String dummy15Curr = null;
		String dummy16Curr = null;
		String dummy17Curr = null;
		String dummy18Curr = null;
		String dummy19Curr = null;
		String dummy20Curr = null;
		Object dummy15Obj = null;
		Object dummy16Obj = null;
		Object dummy17Obj = null;
		Object dummy18Obj = null;
		Object dummy19Obj = null;
		Object dummy20Obj = null;
		Map<String,ArrayList<HashMap<Short,ArrayList<String>>>> deviceIdDtlMap = new HashMap<String,ArrayList<HashMap<Short,ArrayList<String>>>>();		
        for (Tuple2<TelegramHash, CanUnitBean> next : pairRDDReducer.collect()) {
        	
        	deviceIdCurr = next._1.deviceId;
            reliveTimeCurr = String.valueOf(next._1.timestamp);
            deviceIdTime = deviceIdCurr + reliveTimeCurr;
            canIdCurr = next._2.getCanId();
            
            System.out.println(next._2.getConvertedDataMap().keySet());
            
            dummy15Obj = next._2.getConvertedDataMap().get("dummy15");
            dummy16Obj = next._2.getConvertedDataMap().get("dummy16");
            dummy17Obj = next._2.getConvertedDataMap().get("dummy17");
            dummy18Obj = next._2.getConvertedDataMap().get("dummy18");
            dummy19Obj = next._2.getConvertedDataMap().get("dummy19");
            dummy20Obj = next._2.getConvertedDataMap().get("dummy20");
            
            if(!"".equals(dummy15Obj) && null !=dummy15Obj) {
            	dummy15Curr = dummy15Obj.toString();
            }
            if(!"".equals(dummy16Obj) && null !=dummy16Obj) {
            	dummy16Curr = dummy16Obj.toString();
            }
            if(!"".equals(dummy17Obj) && null !=dummy17Obj) {
            	dummy17Curr = dummy17Obj.toString();
            }
            if(!"".equals(dummy18Obj) && null !=dummy18Obj) {
            	dummy18Curr = dummy18Obj.toString();
            }
            if(!"".equals(dummy19Obj) && null !=dummy19Obj) {
            	dummy19Curr = dummy19Obj.toString();
            }
            if(!"".equals(dummy20Obj) && null !=dummy20Obj) {
            	dummy20Curr = dummy20Obj.toString();
            }
            if(!deviceIdDtlMap.containsKey(deviceIdTime)) {
            	ArrayList<HashMap<Short,ArrayList<String>>> canIdList = new ArrayList<HashMap<Short,ArrayList<String>>>();
        		HashMap<Short,ArrayList<String>> canIdMap = new HashMap<Short,ArrayList<String>>();
        		ArrayList<String> lableList = new ArrayList<String>();
        		if("" != dummy15Curr && null != dummy15Curr) {
        			lableList.add(dummy15Curr);
    			}
        		if("" != dummy16Curr && null != dummy16Curr) {
        			lableList.add(dummy16Curr);
    			}
        		if("" != dummy17Curr && null != dummy17Curr) {
        			lableList.add(dummy17Curr);
    			}
        		if("" != dummy18Curr && null != dummy18Curr) {
        			lableList.add(dummy18Curr);
    			}
        		if("" != dummy19Curr && null != dummy19Curr) {
        			lableList.add(dummy19Curr);
    			}
        		if("" != dummy20Curr && null != dummy20Curr) {
        			lableList.add(dummy20Curr);
    			}
        		canIdMap.put(canIdCurr, lableList);
        		canIdList.add(canIdMap);
        		deviceIdDtlMap.put(deviceIdTime, canIdList);
            }else {
            	boolean canIdContainFlag = false;
            	int index = 0;
            	for(int i = 0 ; i < deviceIdDtlMap.get(deviceIdTime).size();i++) {
            		if(deviceIdDtlMap.get(deviceIdTime).get(i).containsKey(canIdCurr)) {
            			canIdContainFlag = true;
            			index = i;
            			break;
            		}
            	}
            	if(canIdContainFlag) {
            		ArrayList<String> LableListBef = deviceIdDtlMap.get(deviceIdCurr).get(index).get(canIdCurr);
            		if("" != dummy15Curr && null != dummy15Curr) {
            			String lable15Bef = LableListBef.get(0);
            			if(0 > dummy15Curr.compareTo(lable15Bef)) {
            				LableListBef.add(0, dummy15Curr);
            			}
        			}
            		if("" != dummy16Curr && null != dummy16Curr) {
            			String lable16Bef = LableListBef.get(1);
            			if(0 > dummy16Curr.compareTo(lable16Bef)) {
            				LableListBef.add(1, dummy16Curr);
            			}
        			}
            		if("" != dummy17Curr && null != dummy17Curr) {
            			String lable17Bef = LableListBef.get(2);
            			if(0 > dummy17Curr.compareTo(lable17Bef)) {
            				LableListBef.add(2, dummy17Curr);
            			}
        			}
            		if("" != dummy18Curr && null != dummy18Curr) {
            			String lable18Bef = LableListBef.get(3);
            			if(0 > dummy18Curr.compareTo(lable18Bef)) {
            				LableListBef.add(3, dummy18Curr);
            			}
        			}
            		if("" != dummy19Curr && null != dummy19Curr) {
            			String lable19Bef = LableListBef.get(4);
            			if(0 > dummy19Curr.compareTo(lable19Bef)) {
            				LableListBef.add(4, dummy19Curr);
            			}
        			}
            		if("" != dummy20Curr && null != dummy20Curr) {
            			String lable20Bef = LableListBef.get(5);
            			if(0 > dummy20Curr.compareTo(lable20Bef)) {
            				LableListBef.add(5, dummy20Curr);
            			}
        			}
            		
            		deviceIdDtlMap.get(deviceIdTime).get(index).put(canIdCurr, LableListBef);	            		
            	}else {
            		HashMap<Short,ArrayList<String>> canIdMap = new HashMap<Short,ArrayList<String>>();
            		ArrayList<String> lableList = new ArrayList<String>();
            		if("" != dummy15Curr && null != dummy15Curr) {
            			lableList.add(dummy15Curr);
        			}
            		if("" != dummy16Curr && null != dummy16Curr) {
            			lableList.add(dummy16Curr);
        			}
            		if("" != dummy17Curr && null != dummy17Curr) {
            			lableList.add(dummy17Curr);
        			}
            		if("" != dummy18Curr && null != dummy18Curr) {
            			lableList.add(dummy18Curr);
        			}
            		if("" != dummy19Curr && null != dummy19Curr) {
            			lableList.add(dummy19Curr);
        			}
            		if("" != dummy20Curr && null != dummy20Curr) {
            			lableList.add(dummy20Curr);
        			}
            		canIdMap.put(canIdCurr, lableList);
            		deviceIdDtlMap.get(deviceIdTime).add(canIdMap);
            	}
            }
            dummy15Curr = null;
    		dummy16Curr = null;
    		dummy17Curr = null;
    		dummy18Curr = null;
    		dummy19Curr = null;
    		dummy20Curr = null;
        }
        for (Entry<String,ArrayList<HashMap<Short,ArrayList<String>>>> entry : deviceIdDtlMap.entrySet()) {
        	String deviceIdKey = entry.getKey();
        	ArrayList<HashMap<Short,ArrayList<String>>> deviceIdVal = deviceIdDtlMap.get(deviceIdKey);
        	ArrayList<Short> canIdStrList = new ArrayList<Short>();
        	String lableStr = "";
        	for(HashMap<Short,ArrayList<String>> canIdHash : deviceIdVal) {
        		for(Entry<Short,ArrayList<String>> canIdEntry : canIdHash.entrySet()) {
        			Short canIdkey = canIdEntry.getKey();
        			canIdStrList.add(canIdkey);
        			ArrayList<String> lableList = canIdEntry.getValue();
        			for(String lable : lableList) {
        				lableStr = lableStr + lable;
        			}
        		}
        	}
        	
        	String result = deviceIdKey + canIdStrList.toString() + lableStr;
        	System.out.println(result);
        }
	}
}
