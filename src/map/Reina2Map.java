package map;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;


public class Reina2Map implements MapFunction<Tuple2<String, Long>, Tuple3<String, String, Long>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple3<String, String, Long> map(Tuple2<String, Long> in) throws Exception {
		// TODO Auto-generated method stub
		String[] str = in.f0.split("\\|", -1);
		String timestamp = str[0];
		String status = str[1];
		Long msisdn = in.f1;
		
		return new Tuple3<String, String, Long>(timestamp, status, msisdn);
	}
}
