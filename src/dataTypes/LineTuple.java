package dataTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LineTuple implements WritableComparable<LineTuple> {
	private long lineNum;
	private String line;
	private int repoNum;
	public LineTuple()
	{
	}
	
	public LineTuple(long lineNum, String line, int repoNum)
	{
		this.lineNum = lineNum;
		this.line = line;
		this.repoNum = repoNum;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.lineNum = in.readLong();
	    this.line = Text.readString(in);	
	    this.repoNum = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		 out.writeLong(lineNum);
	     Text.writeString(out, line);	
	     out.writeInt(repoNum);
	}

	@Override
	public int compareTo(LineTuple lt) {
		if(line.equals(lt.getLine())) return 0;
		else return 1;	
	}
	public long getLineNum() {
		return lineNum;
	}
	public String getLine() {
		return line;
	}

	public void setLineNum(long lineNum) {
		this.lineNum = lineNum;
	}

	public void setLine(String line) {
		this.line = line;
	}
	public String retainString()
	{
		return lineNum+"\t\t"+line;
	}
	public String addString(String preTab,String postTab)
	{
		return preTab+lineNum+postTab+"+"+line;
	}
	public String delString(String preTab, String postTab)
	{
		return preTab+lineNum+postTab+"-"+line;
	}
	public int getRepoNum() {
		return repoNum;
	}

	public void setRepoNum(int repoNum) {
		this.repoNum = repoNum;
	}

}
