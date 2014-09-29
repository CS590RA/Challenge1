package netks;

import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class PrintBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =
        Logger.getLogger(PrintBolt.class);
   // private static final ObjectMapper mapper = new ObjectMapper();
    File file = new File("/root/Downloads/documents/output.txt");
	public void execute(Tuple t1, BasicOutputCollector boc) {
		// TODO Auto-generated method stub

		LOGGER.debug("Printing");

		FileWriter f=null;
		BufferedWriter b=null;
		try
		{
			if(!file.exists())
			{
				file.createNewFile();
			}
			f = new FileWriter(file.getAbsoluteFile(),true);
			b = new BufferedWriter(f);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		Double bw=t1.getDoubleByField("counted_bandwidth");
		String ip=t1.getStringByField("ip_address");
		
		String text="Band width"+bw.toString()+" : IP ADDRESS"+ip;
		try {
			b.write(text);
			b.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
