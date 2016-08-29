package cn.project.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

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
	
	static class BatchImportReducer extends TableReducer<LongWritable, Text, NullWritable>{
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, NullWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				String[] splited = text.toString().split("\t");
				Put put = new Put(Bytes.toBytes(splited[0]));
				put.add(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes(splited[1]));
				put.add(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes(splited[2]));
				//put.add ...
				context.write(NullWritable.get(), put);
			}
		}
	}
}
