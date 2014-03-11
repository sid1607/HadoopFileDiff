package reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;


import dataTypes.LineTuple;


public class LineNumMapCombineRecordReader implements RecordReader<Text, LineTuple>
{
	private CombineFileSplit split;
	private Path[] paths;
	private long totLength,offset;
	private int count;
	private FileSystem fs;
	private BufferedReader currentReader;
	private FSDataInputStream currentStream;
	private long lineNum;
    private Log log = LogFactory.getLog(LineNumMapCombineRecordReader.class);
	public LineNumMapCombineRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException
	{
		// This InputSplit is a FileInputSplit
        this.split = (CombineFileSplit) split;
        this.fs = FileSystem.get(job);
        this.paths = this.split.getPaths();
        for(int i=0;i<paths.length;i++)
        	System.out.println(paths[i]);
        this.totLength = this.split.getLength();
        this.offset = 0;
        this.count=0;
        this.lineNum=0;
        //open the first file
        Path file = paths[count];
        currentStream = fs.open(file);
        currentReader = new BufferedReader(new InputStreamReader(currentStream));
	}
	
	@Override
	public void close() throws IOException {
		if (currentReader!=null) currentReader.close();
	}

	@Override
	public Text createKey() {
		// TODO Auto-generated method stub
		return new Text();
	}

	@Override
	public LineTuple createValue() {
		// TODO Auto-generated method stub
		return new LineTuple();
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		 long currentOffset = currentStream == null ? 0 : currentStream.getPos();
	     return offset + currentOffset;
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return ((float)getPos()) / totLength;
	}
	
	@Override
	public boolean next(Text key, LineTuple value) throws IOException {
		if(count >= split.getNumPaths())
	        return false;

	      /* Read from file, fill in key and value, if we reach the end of file,
	       * then open the next file and continue from there until all files are
	       * consumed.  
	       */
	      String line;
	      do {
	        line = currentReader.readLine();
	        if(line == null) {
	          //close the file
	          currentReader.close();
	          offset += split.getLength(count);
	          if(++count >= split.getNumPaths()) //if we are done
	            return false;
	          //open a new file
	          Path file = paths[count];
	          currentStream = fs.open(file);
	          currentReader=new BufferedReader(new InputStreamReader(currentStream));
	          this.lineNum=0;
	        }
	      } while(line == null);
	      //update the key and value
	      key.set(paths[count].toString());
	      value.setLine(line);
	      value.setLineNum(++this.lineNum);
	      return true;	 
   }	
}