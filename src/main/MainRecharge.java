package main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import join.RechargeLeftJoinReina;
import map.OutputMap;
import map.RechargeFlatMap;
import map.Reina2Map;
import map.ReinaFlatMap;

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType;

import filter.RechargeReinaREGFilter;
import filter.RechargeReinaUNREGFilter;
import util.Constant;
import util.Helper;

public class MainRecharge {
	private HashMap<String, DataSet<String>> dataset_inputs = new HashMap<String, DataSet<String>>();
	private ExecutionEnvironment env;
	private int proses_paralel;
	private int sink_paralel;
	private String sendAPI;
	private String APISlot;
	private Configuration parameter;
	private String outputPath;
	private String feed;

	// tuples variable
	private DataSet<Tuple1<Long>> recharge_tuples;
	private DataSet<Tuple2<String, Long>> reina_reg_tuples;
	private DataSet<Tuple2<String, Long>> reina_unreg_tuples;
	private DataSet<Tuple2<String, Long>> reina_del_tuples;

	private DataSet<Tuple3<String, String, Long>> reina2_reg_unreg_tuples;
	private DataSet<Tuple3<String, String, Long>> reina2_del_tuples;

	private DataSet<Tuple2<Long, String>> recharge_reina_tuples;
	private DataSet<Tuple2<Long, String>> recharge_reina_reg_tuples;
	private DataSet<Tuple2<Long, String>> recharge_reina_reg_del_tuples;
	private DataSet<Tuple2<Long, String>> recharge_reina_unreg_tuples;

	private DataSet<Tuple1<Long>> output;

	public MainRecharge(int proses_paralel, int sink_paralel,
			String outputPath, String feed, String sendAPI, String APISlot) {
		this.env = ExecutionEnvironment.getExecutionEnvironment();
		this.parameter = new Configuration();
		this.outputPath = outputPath;

		this.proses_paralel = proses_paralel;
		this.sink_paralel = sink_paralel;
		this.env.setParallelism(this.proses_paralel);
		this.parameter.setBoolean("recursive.file.enumeration", true);
		this.feed = feed;
		this.sendAPI = sendAPI;
		this.APISlot = APISlot;

		// BasicConfigurator.configure(); //remove log warn

	}

	public String getAPISlot() {
		return this.APISlot;
	}

	public String getFeed() {
		return feed;
	}

	public void setFeed(String feed) {
		this.feed = feed;
	}

	private String getOutputPath() {
		return this.outputPath;
	}

	private Configuration getParameter() {
		return this.parameter;
	}

	private ExecutionEnvironment getEnv() {
		return this.env;
	}

	public int getSink_paralel() {
		return this.sink_paralel;
	}

	private void setInput(HashMap<String, String> files) {

		for (Map.Entry<String, String> file : files.entrySet()) {
			dataset_inputs.put(
					file.getKey(),
					getEnv().readTextFile(file.getValue()).withParameters(
							getParameter()));
		}

	}

	public void processInput() {
		recharge_tuples = dataset_inputs.get("source")
				.flatMap(new RechargeFlatMap(feed))
				.name("GET INPUT - Recharge");
		reina_reg_tuples = dataset_inputs.get("ref_reina_reg")
				.flatMap(new ReinaFlatMap("REG")).name("GET INPUT - Reina Reg");
		reina_unreg_tuples = dataset_inputs.get("ref_reina_unreg")
				.flatMap(new ReinaFlatMap("UNREG"))
				.name("GET INPUT - Reina Unreg");
		reina_del_tuples = dataset_inputs.get("ref_reina_del")
				.flatMap(new ReinaFlatMap("DEL")).name("GET INPUT - Reina Del");
	}

	public void processAggregate() throws Exception {
		// Recharge
		recharge_tuples = recharge_tuples.distinct()
				.name("DISTINCT - Recharge");

		// Reina
		reina2_reg_unreg_tuples = reina_reg_tuples.union(reina_unreg_tuples)
				.groupBy(1).aggregate(Aggregations.MAX, 0).map(new Reina2Map())
				.name("MAX - Reina Reg + Unreg");
		reina2_del_tuples = reina_del_tuples.map(new Reina2Map()).distinct()
				.name("DISTINCT - Reina Del");

		// Recharge - reina
		recharge_reina_tuples = recharge_tuples
				.leftOuterJoin(reina2_reg_unreg_tuples,
						JoinHint.REPARTITION_HASH_FIRST).where(0).equalTo(2)
				.with(new RechargeLeftJoinReina())
				.name("LEFT JOIN - Recharge with Reina");
		recharge_reina_reg_tuples = recharge_reina_tuples.filter(
				new RechargeReinaREGFilter())
				.name("FILTER - RechargeReina Reg");
		recharge_reina_unreg_tuples = recharge_reina_tuples.filter(
				new RechargeReinaUNREGFilter()).name(
				"FILTER - RechargeReina Unreg");

		recharge_reina_reg_del_tuples = (recharge_reina_reg_tuples
				.join(reina2_del_tuples).where(0).equalTo(2).projectFirst(0)
				.projectSecond(1));

		// Output
		output = recharge_reina_unreg_tuples
				.union(recharge_reina_reg_del_tuples)
				.map(new OutputMap(Integer.parseInt(sendAPI)))
				.setParallelism(Integer.parseInt(getAPISlot()))
				.name("SEND API - Recharge Unreg");

	}

	public void sink() throws Exception {
		output.writeAsCsv(getOutputPath(), "\n", "|", WriteMode.OVERWRITE)
				.setParallelism(getSink_paralel());

		// recharge_reina_tuples.writeAsCsv(getOutputPath(), "\n", "|",
		// WriteMode.OVERWRITE).setParallelism(getSink_paralel());
	}

	public static void main(String[] args) throws Exception {
		// set data input
		HashMap<String, String> files = new HashMap<String, String>();

		/** prod **/
		ParameterTool params = ParameterTool.fromArgs(args);

		int proses_paralel = params.getInt("slot");
		int sink_paralel = params.getInt("sink");

		String source = params.get("source"); // mkios or urp
		String ref_reina_reg = params.get("ref_reina_reg");
		String ref_reina_unreg = params.get("ref_reina_unreg");
		String ref_reina_del = params.get("ref_reina_del");
		String feed = params.get("feed");
		String output = params.get("output");
		String sendAPI = params.get("send_api");
		String APISlot = params.get("slot_api");
		String dateNOW = params.get("date_now");

		MainRecharge main = new MainRecharge(proses_paralel, sink_paralel,
				output, feed, sendAPI, APISlot);

		files.put("source", source);
		files.put("ref_reina_reg", ref_reina_reg);
		files.put("ref_reina_unreg", ref_reina_unreg);
		files.put("ref_reina_del", ref_reina_del);

		/** dev **/
		// int proses_paralel = 2;
		// int sink_paralel = 1;
		// String feed = "MKIOS";
		//
		// MainRecharge main = new MainRecharge(proses_paralel, sink_paralel,
		// Constant.OUTPUT, feed);
		// files.put("source", Constant.FILE_SOURCE);
		// files.put("ref_reina_reg", Constant.REF_REINA_REG);
		// files.put("ref_reina_unreg", Constant.REF_REINA_UNREG);
		// files.put("ref_reina_del", Constant.REF_REINA_DEL);

		/****/
		main.setInput(files);
		main.processInput();
		main.processAggregate();
		main.sink();

		try {
			main.getEnv().execute(
					dateNOW + ": "+main.getFeed() + " - Recharge Unreg");
		} catch (Exception e) {
			// TODO Auto-generated catch blockF
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}
