package reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import app.FileDiffReporter;

import dataTypes.LineTuple;

public class LineNumMapFileRecordReader implements RecordReader<Text,LineTuple> {
	private FileSplit split;
	private Path path;
	private Path newPath;
	private long totLength,offset;
	private FileSystem fs;
	private BufferedReader currentReader;
	private FSDataInputStream currentStream;
	private long lineNum;
	private int repoNum;
	public static final Log log = LogFactory.getLog(FileDiffReporter.class);
	
	public LineNumMapFileRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException
	{
		// This InputSplit is a FileInputSplit
        this.split = (FileSplit) split;
        this.fs = FileSystem.get(job);
        this.path = this.split.getPath();
        this.newPath = getNewPath(path);
        this.totLength = this.split.getLength();
        this.offset = 0;
        this.lineNum=0;
        //open the first file
        currentStream = fs.open(path);
        currentReader = new BufferedReader(new InputStreamReader(currentStream));
	}
	
	public Path getNewPath(Path fullPath)
	{
		Path newPath = new Path(fullPath.getName());
		String prefix = "repo";
		String parentPath = fullPath.getParent().toString();
		String[] parents = parentPath.split("/");
		int index = parents.length-1;
		while(!(parents[index].regionMatches(0, prefix, 0, prefix.length())))
			newPath = new Path(parents[index--],newPath);
		if(parents[index].equals("repo-1"))
			repoNum=1;
		else
			repoNum=2;
		return newPath;	
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
	          offset += split.getLength();
	          //open a new file
	          key=null;
	          value=null;
	          return false;
	        }
	      } while(line == null);
	      //update the key and value
	      key.set(newPath.toString());
	      value.setLine(line);
	      value.setLineNum(++this.lineNum);
	      value.setRepoNum(repoNum);
	      return true;
	}
}
