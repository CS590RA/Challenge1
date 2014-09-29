package netks;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BandwidthBolt extends BaseBasicBolt
{
	private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =
        Logger.getLogger(BandwidthBolt.class);
    //private static final ObjectMapper mapper = new ObjectMapper();
    
   
	public void execute(Tuple t1, BasicOutputCollector boc) {
		// TODO Auto-generated method stub

		LOGGER.debug("Filtering network data");
		String d=t1.getString(0);
		char da[]=d.toCharArray();
		int count=0,i=0;
		char bw[]={};
		char sip[]={};
		char dip[]={};
		for(i=0;i<=da.length;i++)
		{
			if(da[i]==' ')
			{
				count++;
			}

			if(count==18)
			{
				i=i+1;
				for(int j=0;j<=da.length;j++,i++)
				{
					if(da[i]==' ')
						break;
					bw[j]=da[i];
				}
				
			}
		
			if(count==8)
			{
				i=i+1;
				for(int j=0;j<=da.length;j++,i++)
				{
					if(da[i]==' ')
						break;
					sip[j]=da[i];
				}
				
			}

			if(count==13)
			{
				i=i+1;
				for(int j=0;j<=da.length;j++,i++)
				{
					if(da[i]==' ')
						break;
					dip[j]=da[i];
				}
				
			}
		
			}
		boc.emit(new Values(sip,dip,bw));
		}
		
		
		


	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		// TODO Auto-generated method stub
		
		ofd.declare(new Fields("sourceip","destinationip","bandwidth"));
	}

}
