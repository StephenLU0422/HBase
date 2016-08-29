package cn.project.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class HDFS2HBase {
	public static void main(String[] args) throws Exception {
		//配置
		Configuration conf = HBaseConfiguration.create();
		conf.set(TableOutputFormat.OUTPUT_TABLE, "t2");
		Job job = Job.getInstance(conf, HDFS2HBase.class.getSimpleName());
		//设置jar依赖/mapreduce包
		TableMapReduceUtil.addDependencyJars(job);
		job.setJarByClass(HDFS2HBase.class);
		//set Mapper/Reducer
		job.setMapperClass(BatchImportMapper.class);
		job.setReducerClass(BatchImportReducer.class);
		//设置map输出，不设置reduce的
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		//输出格式类型，不指定输出路径
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.setInputPaths(job, "/hbase/t1-out/part-*");
		job.waitForCompletion(true);
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
