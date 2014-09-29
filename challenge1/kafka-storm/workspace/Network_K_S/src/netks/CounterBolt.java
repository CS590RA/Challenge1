package netks;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CounterBolt extends BaseBasicBolt{
	private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =
        Logger.getLogger(CounterBolt.class);
    //private static final ObjectMapper mapper = new ObjectMapper();
    Map<String, Double> counts = new HashMap<String,Double>();
 
	public void execute(Tuple t1, BasicOutputCollector boc) {
		// TODO Auto-generated method stub
		
		LOGGER.debug("Counting for counter bandwidth");
		String bw=t1.getStringByField("bandwidth");
		String sip=t1.getStringByField("sourceip");
		String dip=t1.getStringByField("destinationip");
		Double bwf=Double.parseDouble(bw);
			sip.concat(" ");
			sip.concat(dip);
	        Double count = counts.get(sip);
	       
	        if(count==null) count = (double) 0;
	        count=count+bwf;
	        counts.put(sip, count);
	        boc.emit(new Values(sip, count));
	
	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		// TODO Auto-generated method stub
		ofd.declare(new Fields("ip_address","counted_bandwidth"));
	}

}
