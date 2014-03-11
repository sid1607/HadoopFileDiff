package reader;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;

import dataTypes.LineTuple;

public class LineMapCombineInputFormat extends CombineFileInputFormat<Text, LineTuple>{
	
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return false;
	}
	@Override
	public RecordReader<Text,LineTuple> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		return new LineNumMapCombineRecordReader(split,job,reporter);
	}
	

}
