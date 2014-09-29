package netks;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
//import java.util.Map.Entry;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class TopBolt extends BaseBasicBolt{
	private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =
        Logger.getLogger(TopBolt.class);
    //private static final ObjectMapper mapper = new ObjectMapper();
    Map<String, Double> sort = new HashMap<String,Double>();
 
	public void execute(Tuple t1, BasicOutputCollector boc) {
		// TODO Auto-generated method stub

		LOGGER.debug("Sorting for top bandwidth");
		Double bw=t1.getDoubleByField("counted_bandwidth");
		String ip=t1.getStringByField("ip_address");
		
		/*for (Entry<String, Double> e : sort.entrySet()) {
		    String key    = e.getKey();
		    Double value  = e.getValue();
		}*/
		
		boc.emit(new Values(ip,bw));
	
	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		// TODO Auto-generated method stub
		ofd.declare(new Fields("ip_ad","Band_width"));
	}

}
