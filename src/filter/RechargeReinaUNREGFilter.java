package filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class RechargeReinaUNREGFilter implements FilterFunction<Tuple2<Long, String>> {

	private static final long serialVersionUID = 1L;

	@Override
	public boolean filter(Tuple2<Long, String> in) throws Exception {
		return in.f1.equals("UNREG");
	}

}