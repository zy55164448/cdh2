package function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class DriverIteretor777 {

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("CanTimeReducer").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//1506787111000 0x01 0x22 0x122 0x201
		TelegramHash telegramHash1 = new TelegramHash("1111", 1506787111000L);
		CanUnitBean canUnitBean11 = Unit.createCanUnitBean((short) 0x01, "1506787111010");
		CanUnitBean canUnitBean12 = Unit.createCanUnitBean((short) 0x22, "1506787111020");
		CanUnitBean canUnitBean13 = Unit.createCanUnitBean((short) 0x122, "1506787111030");
		CanUnitBean canUnitBean14 = Unit.createCanUnitBean((short) 0x201, "1506787111050");

		CanUnitBean canUnitBean11_1 = Unit.createCanUnitBean2((short) 0x01, "1506787111110");
		CanUnitBean canUnitBean12_1 = Unit.createCanUnitBean2((short) 0x22, "1506787111120");
		CanUnitBean canUnitBean13_1 = Unit.createCanUnitBean2((short) 0x122, "1506787111130");
		CanUnitBean canUnitBean14_1 = Unit.createCanUnitBean2((short) 0x201, "1506787111150");

		CanUnitBean canUnitBean11_null = Unit.createCanUnitBeanNull((short) 0x01, "1506787111110");
		CanUnitBean canUnitBean12_null = Unit.createCanUnitBeanNull((short) 0x22, "1506787111120");
		CanUnitBean canUnitBean13_null = Unit.createCanUnitBeanNull((short) 0x122, "1506787111130");
		CanUnitBean canUnitBean14_null = Unit.createCanUnitBeanNull((short) 0x201, "1506787111150");

		//1506787222000 0x01 0x22 0x122 0x201
		TelegramHash telegramHash2 = new TelegramHash("2222", 1506787222000L);
		CanUnitBean canUnitBean21 = Unit.createCanUnitBean((short) 0x01, "1506787222010");
		CanUnitBean canUnitBean22 = Unit.createCanUnitBean((short) 0x22, "1506787222020");
		CanUnitBean canUnitBean23 = Unit.createCanUnitBean((short) 0x122, "1506787222030");
		CanUnitBean canUnitBean24 = Unit.createCanUnitBean((short) 0x201, "1506787222040");

		CanUnitBean canUnitBean21_1 = Unit.createCanUnitBean2((short) 0x01, "1506787222110");
		CanUnitBean canUnitBean22_1 = Unit.createCanUnitBean2((short) 0x22, "1506787222120");
		CanUnitBean canUnitBean23_1 = Unit.createCanUnitBean2((short) 0x122, "1506787222130");
		CanUnitBean canUnitBean24_1 = Unit.createCanUnitBean2((short) 0x201, "1506787222140");
		CanUnitBean canUnitBean21_null = Unit.createCanUnitBeanNull((short) 0x01, "1506787222210");
		CanUnitBean canUnitBean22_null = Unit.createCanUnitBeanNull((short) 0x22, "1506787222220");
		CanUnitBean canUnitBean23_null = Unit.createCanUnitBeanNull((short) 0x122, "1506787222230");
		CanUnitBean canUnitBean24_null = Unit.createCanUnitBeanNull((short) 0x201, "1506787222240");

		List<Tuple2<TelegramHash, CanUnitBean>> rdd = Arrays.asList(
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean11),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean12),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean13),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean14),

				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean11_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean12_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean13_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean14_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean11_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean12_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean13_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1, canUnitBean14_null),

				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean21),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean22),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean23),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean24),

				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean21_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean22_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean23_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean24_1),

				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean21_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean22_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean23_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2, canUnitBean24_null));

		Iterator<Tuple2<TelegramHash, List<CanUnitBean>>> aa = call(rdd.iterator());
		while (aa.hasNext()) {
			Tuple2<TelegramHash, List<CanUnitBean>> bbb = aa.next();
			System.out.println(bbb._1.deviceId + ":" + bbb._1.timestamp + "------------------------------------------");

			for (CanUnitBean bean : bbb._2) {
				System.out.println(bean.getCanId() + ":" + bean.getCanTime() + ":" + bean.getConvertedDataMap());
			}
			System.out.println("---------------------------------------------------------------");
		}

	}

	public static Iterator<Tuple2<TelegramHash, List<CanUnitBean>>> call(
			final Iterator<Tuple2<TelegramHash, CanUnitBean>> tuples) throws Exception {

        return new Iterator<Tuple2<TelegramHash, List<CanUnitBean>>>() {

        	private TelegramHash progress = null;
        	private CanUnitBean canUnitBean = null;
        	private List<CanUnitBean> message = null;
        	private Tuple2<TelegramHash, CanUnitBean> aheadTuple = null;
        	private int timeInterval = 100;

        	private void ensureNexrElement() {
        		if (progress !=null || message != null) {
        			return;
        		}

        		this.message = new ArrayList<>();

        		TreeMap<Short, CanUnitBean> rvTelegram = new TreeMap<Short, CanUnitBean>();
        		if (aheadTuple != null) {
        			this.progress = aheadTuple._1;
        			this.canUnitBean = aheadTuple._2;
        			this.aheadTuple = null;
        			rvTelegram.put(this.canUnitBean.getCanId(), this.canUnitBean);
        		}
        		while (tuples.hasNext()) {
        			final Tuple2<TelegramHash, CanUnitBean> tuple = tuples.next();

        			if (progress == null || (progress.equals(tuple._1) &&
        					canUnitBean.compareToTimeInterval(tuple._2, timeInterval))) {
        				Short catId = tuple._2.getCanId();
        				CanUnitBean rvCanUnitBean = rvTelegram.get(catId);
        				if(rvCanUnitBean == null) {
        					rvCanUnitBean = tuple._2;
        					rvTelegram.put(catId, rvCanUnitBean);
        				}else{//百毫秒未改变
        					//处理假定 某个VIN/CanId标识的所有报文的Label数目相同
        					Map<String ,Object> rvMap = (Map<String,Object>)rvCanUnitBean.getConvertedDataMap();
        					for (Map.Entry<String, Object> entry  : rvMap.entrySet()) {

        						//当前时戳下值为空，pass
        						Object refreshValue = tuple._2.getConvertedDataMap().get(entry.getKey());
        						if(refreshValue == null)
        							continue;

        						rvMap.replace(entry.getKey(),refreshValue);
        					}
        				}
        				this.progress = tuple._1;
        				this.canUnitBean = tuple._2;
        			} else {
        				this.aheadTuple = tuple;
        				break;
        			}
        		}

        		System.out.println("rvTelegram:" + rvTelegram.size());
        		Iterator<Short> it = rvTelegram.keySet().iterator();
        		while(it.hasNext()) {
        			short key = it.next();
        			this.message.add(rvTelegram.get(key));

        		}
        	}
        	@Override
        	public boolean hasNext() {
        		ensureNexrElement();
        		return message != null && !message.isEmpty();
        	}
        	@Override
        	public Tuple2<TelegramHash, List<CanUnitBean>> next() {
        		if (!hasNext()) {
        			//throw new Exception();
        		}
        		Tuple2<TelegramHash, List<CanUnitBean>> next = new Tuple2<TelegramHash, List<CanUnitBean>>(progress,message);
    			this.progress = null;
    			this.message= null;
    			this.canUnitBean= null;
        		return next;
        	}
        };
	}

}
