package writer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class MultiFileOutputFormatByKey extends MultipleTextOutputFormat<Text,Text> {
	@Override
	protected String generateFileNameForKeyValue(Text key, Text value,String name) {
		return new Path(key.toString().replaceAll("/", "_")+".diff").toString();
	}
	@Override
	protected Text generateActualKey(Text key, Text value) {
		return null;
	}
}
