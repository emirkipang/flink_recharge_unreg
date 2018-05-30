package map;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import util.Helper;

public class ReinaFlatMap implements
		FlatMapFunction<String, Tuple2<String, Long>> {

	/**
* 
*/
	private static final long serialVersionUID = 1L;
	private String flag;

	public ReinaFlatMap(String flag) {
		this.flag = flag;
	}

	public String getFlag() {
		return this.flag;
	}

	@Override
	public void flatMap(String in, Collector<Tuple2<String, Long>> out)
			throws Exception {
		// TODO Auto-generated method stub
		String[] lines = in.split("\n");
		Long msisdn = null;
		String timestamp;
		String status;

		for (String line : lines) {
			String[] items = line.split("\\|", -1);

			status = getFlag();
			if (status.equals("DEL")) {
				timestamp = "1900-01-01 00:00:00";
				msisdn = Long.parseLong(items[0].replaceAll("[^0-9]", ""));
			} else {
				timestamp = items[0];				
				msisdn = Long.parseLong("62" + items[1].replaceAll("[^0-9]", ""));
			}

			out.collect(new Tuple2<String, Long>(timestamp + "|" + status,
					msisdn));
		}

	}
}
