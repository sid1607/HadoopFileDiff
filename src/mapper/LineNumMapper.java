package mapper;
import java.io.IOException;

import dataTypes.LineTuple;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;

public class LineNumMapper extends MapReduceBase implements Mapper<Text,LineTuple, Text,LineTuple> {

	@Override
	public void map(Text inKey, LineTuple inValue,OutputCollector<Text, LineTuple> out, Reporter reporter) throws IOException {
		out.collect(inKey, inValue);	
	}

}
