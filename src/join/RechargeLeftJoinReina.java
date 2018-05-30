package join;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class RechargeLeftJoinReina implements
		FlatJoinFunction<Tuple1<Long>, Tuple3<String, String, Long>, Tuple2<Long, String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void join(Tuple1<Long> leftElem, Tuple3<String, String, Long> rightElem,
			Collector<Tuple2<Long, String>> out) throws Exception {
		// TODO Auto-generated method stub
		if (rightElem == null) // MkiosMDLLeftOuterJoinMstDmnt
			out.collect(new Tuple2<Long, String>(leftElem.f0, "UNREG"));
		else
			out.collect(new Tuple2<Long, String>(leftElem.f0, rightElem.f1));

	}

}
