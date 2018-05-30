package map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import util.Helper;

public class OutputMap implements
		MapFunction<Tuple2<Long, String>, Tuple1<Long>> {

	private int sendAPI;

	public OutputMap(int sendAPI) {
		this.sendAPI = sendAPI;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple1<Long> map(Tuple2<Long, String> in) throws Exception {
		Long msisdn = in.f0;

		if (sendAPI == 1) {
			Helper.SendAPI(Long.toString(msisdn));
		}

		return new Tuple1<Long>(msisdn);
	}
}
