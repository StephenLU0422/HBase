package cn.project.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HBase2HDFS {
	public static void main(String[] args) throws Exception {
		//HBase配置
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, HBase2HDFS.class.getSimpleName());
		//setjar
		job.setJarByClass(HBase2HDFS.class);
		//初始化TableMapperJob
		TableMapReduceUtil.initTableMapperJob("t1", new Scan(), HBase2HDFSMapper.class, Text.class, Text.class, job);
		//设置类
		job.setMapperClass(HBase2HDFSMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("/hbase/t1-out"));
		//设置reduce数量
		job.setNumReduceTasks(0);
		job.waitForCompletion(true);
	}
	static class HBase2HDFSMapper extends TableMapper<Text, Text>{
		Text value = new Text();
		@Override
		protected void map(
				ImmutableBytesWritable rowkey,
				Result result,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
				byte[] nameBytes = result.getColumnLatestCell("f1".getBytes(), "name".getBytes()).getValue();
				//获取age
				byte[] ageBytes = result.getColumnLatestCell("f1".getBytes(),"age".getBytes()).getValue();
				value.set(new String(nameBytes)+"\t"+new String(ageBytes));
				//写出去
				context.write(new Text(rowkey.get()), value);
				
		}
	}
}
