package map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

import util.Helper;

public class RechargeFlatMap implements FlatMapFunction<String, Tuple1<Long>> {

	/**
* 
*/
	private static final long serialVersionUID = 1L;
	private String feed;

	public RechargeFlatMap(String feed) {
		// TODO Auto-generated constructor stub
		this.feed = feed;
	}

	public String getFeed() {
		return this.feed;
	}

	@Override
	public void flatMap(String in, Collector<Tuple1<Long>> out)
			throws Exception {
		// TODO Auto-generated method stub
		String[] lines = in.split("\n");
		Long msisdn = (long) 0;

		switch (getFeed()) {
		case "MKIOS":
			for (String line : lines) {
				String[] items = line.split("\\|", -1);
				if (items[13].equalsIgnoreCase("O00")) {
					msisdn = Long
							.parseLong("62"
									+ Helper.isEmpty(items[9].replaceAll(
											"[^0-9]", "")));
				}
				out.collect(new Tuple1<Long>(msisdn));
			}
			break;

		case "URP":
			for (String line : lines) {
				String[] items = line.split("\\/");
				if (items[6].equalsIgnoreCase("61")) {
					msisdn = Long.parseLong(Helper.isEmpty(items[13]
							.replaceAll("[^0-9]", "")));
				}
				out.collect(new Tuple1<Long>(msisdn));
			}

			break;
		}

	}
}
