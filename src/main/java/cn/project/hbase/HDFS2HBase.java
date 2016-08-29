package cn.project.hbase;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class HDFS2HBase {
	public static void main(String[] args) {
		
	}
	static class BatchImportMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		Text v2 = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
		
			final String[] splited = value.toString().split("\t");
			try{
			//对比前面MR的v2
			v2.set(value.toString());
			context.write(new LongWritable(Long.parseLong(splited[0])), v2);
			}catch(NumberFormatException e){
				//计数器
				Counter counter = context.getCounter("BatchImport", "ErrorFormat");
				counter.increment(1L);
				System.out.println("出错："+splited[0]+""+e.getMessage());
				
				
			}
		}
	}
}
