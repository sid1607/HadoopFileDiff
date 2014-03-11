package app;

import mapper.LineNumMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import dataTypes.LineTuple;
import reader.LineMapFileInputFormat;
import reducer.LineDiffReducer;
import tools.Utils;
import writer.MultiFileOutputFormatByKey;

public class FileDiffReporter extends Configured implements Tool {
	private void printUsage() {
		System.out.println("Usage : <input_dir> <output>" );
	}
	public int run(String[] args) throws Exception {
		if(args.length < 2) {
	      printUsage();
	      return 1;
	    }
		/*for(int i=0;i<args.length;i++)
		  System.out.println(args[i]);*/
	    JobConf job = new JobConf(getConf(), FileDiffReporter.class);
	    job.setJobName("FileDiffReporter");

	    //set the InputFormat of the job to our InputFormat
	    job.setInputFormat(LineMapFileInputFormat.class);
	    job.setOutputFormat(MultiFileOutputFormatByKey.class);
	    // the keys are words (strings)
	    job.setOutputKeyClass(Text.class);
	    // the values are counts (ints)
	    job.setOutputValueClass(LineTuple.class);

	    //use the defined mapper
	    job.setMapperClass(LineNumMapper.class);
	    //use the WordCount Reducer
	    //job.setCombinerClass(LineDiffReducer.class);
	    job.setReducerClass(LineDiffReducer.class);
	    
	    Path[] paths = Utils.getRecursivePaths(FileSystem.get(job), args[0]);
	    LineMapFileInputFormat.setInputPaths(job, paths);
	    MultiFileOutputFormatByKey.setOutputPath(job, new Path(args[1]));

	    JobClient.runJob(job);
	    
	    return 0;
	  }

	  public static void main(String[] args) throws Exception {
	    int ret = ToolRunner.run(new FileDiffReporter(), args);
	    System.exit(ret);
	  }
}
