package reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import dataTypes.LineTuple;
import java.util.Map;

public class LineDiffReducer extends MapReduceBase implements Reducer<Text,LineTuple,Text,Text> {
	
	private class RepoLine{
		int repoNum;
		String line;
		public RepoLine(int repoNum, String line)
		{
			this.repoNum = repoNum;
			this.line = line;
		}
		public int getRepoNum()
		{
			return repoNum;
		}
		public String  getLine()
		{
			return line;
		}
	}
	@Override
	public void reduce(Text inKey, Iterator<LineTuple> inValueItr,OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		Map<Long,RepoLine> cache = new HashMap<Long, RepoLine>();
		while (inValueItr.hasNext())
		{
			LineTuple value = inValueItr.next();
			long lineNum = value.getLineNum();
			if(!cache.containsKey(lineNum))
			{
				cache.put(lineNum, new RepoLine(value.getRepoNum(), value.getLine()));
				//out.collect(inKey, new Text("First::::"+value.getLine()));
			}
			else
			{
				String oldLine, newLine;
				RepoLine cacheValue = cache.get(lineNum);
				if(cacheValue.getRepoNum()==1)
				{
					oldLine = cacheValue.getLine();
					newLine = value.getLine();
				}
				else
				{
					oldLine = value.getLine();
					newLine = cacheValue.getLine();
				}
				if(!oldLine.equals(newLine))
				{
					out.collect(inKey, new Text(lineNum+"\t\t-"+oldLine));
					out.collect(inKey, new Text("\t"+lineNum+"\t+"+newLine));
				}
				else
				{
					out.collect(inKey, new Text(lineNum+"\t\t"+oldLine));
				}
			}
		}
	}

}
