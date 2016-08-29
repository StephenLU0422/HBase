package cn.project.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseUtils {
	/**
	 * Hbase 表名称
	 */
	public static final String TABLE_NAME = "spider";
	
	/**
	 * 列族1 商品信息
	 */
	public static final String COLUMNFAMILY_1 = "goodsinfo";
	
	/**
	 * 列族1中的列
	 */
	public static final String COLUMNFAMILY_1_DATA_URL = "data_url";
	public static final String COLUMNFAMILY_1_PIC_URL = "picpath";
	public static final String COLUMNFAMILY_1_TITLE = "title";
	public static final String COLUMNFAMILY_1_PRICE = "price";
	
	/**
	 * 列族2 商品规格
	 */
	public static final String COLUMNFAMILY_2 = "spec";
	public static final String COLUMNFAMILY_2_PARAM = "param";
			
	
	HBaseAdmin admin = null;
	Configuration conf = null;
	/**
	 * 构造函数，加载配置
	 */
	public HbaseUtils(){
		conf = new Configuration();
		//CDH:8020,apache:9000
		//remenber map 192.168.43.98 Stephen98 to the ${windows_user}/hosts记得映射主机号
		conf.set("hbase.zookeeper.quorum", "192.168.43.98:2181");
		conf.set("hbase.rootdir", "hdfs://192.168.43.98:9000/hbase");
		try {
			 admin = new HBaseAdmin(conf);
			 System.err.println("hbase 连接成功！");
		} catch (IOException e) {
			System.err.println("hbase 连接失败！");
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws Exception {
		HbaseUtils hbase = new HbaseUtils();
		String goodsid = null;
		String url=null;
		//创建一张表
//		hbase.createTable("spider","goodsinfo","spec");
		//查询所有表名
//		hbase.getALLTable();
//		hbase.put(HbaseUtils.TABLE_NAME,goodsid,HbaseUtils.COLUMNFAMILY_1,HbaseUtils.COLUMNFAMILY_1_DATA_URL,url);
		
		//往表中添加一条记录
//		hbase.addOneRecord("stu","key1","cf","name","lisi");
//		hbase.addOneRecord("stu","key1","cf","age","22");
		//查询一条记录
//		hbase.getKey("stu","key1");
		//获取表的所有数据
//	hbase.getALLData("spider");
//		hbase.getALLData("stu");
		//删除表中一条数据
//		hbase.deleteOneRecord("stu","key1");
		//删除表
//	hbase.deleteTable("spider");
//		hbase.deleteTable("stu");
		//scan过滤器的使用
//		hbase.getScanData("stu","cf","age");
		//filter
//		hbase.getRowFilter("gprs_log","_201303122359\\d*$");
	}
	
	private void getRowFilter(String tableName, String reg) throws Exception {
		HTable hTable = new HTable(conf, tableName);
		Scan scan = new Scan();
		//filter过滤器NOEQUAL不等于条件
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(reg));
		/*多组过滤器使用：MUST_PASS_ONE:只要一组满足就可以，MUST_PASS_ALL：所有过滤器都满足
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		filterList.addFilter(rowFilter);
		filterList.addFilter(rowFilter);
		scan.setFilter(filterList);*/
		scan.setFilter(rowFilter);
		ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			if (result.raw().length==0) {
				System.out.println(tableName+"table null");
			}else{
				for (KeyValue kv : result.raw()) {
					System.out.println(new String(kv.getRow())+"\t"+new String(kv.getValue()));
					
				}
			}
		}
	}
	private void getScanData(String tableName, String family, String qualifier) throws Exception {
		HTable hTable = new HTable(conf, tableName);
		Scan scan = new Scan();
		scan.addColumn(family.getBytes(), qualifier.getBytes());
		ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			if (result.raw().length==0) {
				System.out.println(tableName+"表"+family+":"+qualifier+"数据为空");
			}else{
				for (KeyValue kv : result.raw()) {
					System.out.println(new String(kv.getRow())+"\t"+new String(kv.getValue()));
					
				}
			}
		}
		
	}
	/**
	 * 删除所有表
	 * @param tableName
	 */
	private void deleteTable(String tableName) {
		//判断存在表
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName+"表删除成功！");
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(tableName+"表删除失败！");
		}
		
	}
	/**
	 * 删除一条记录
	 * @param tableName
	 * @param rowKey
	 */
	private void deleteOneRecord(String tableName, String rowKey) {
		HTablePool hTablePool = new HTablePool(conf, 1000);
		HTableInterface table = hTablePool.getTable(tableName);
		Delete delete = new Delete(rowKey.getBytes());
		try {
			table.delete(delete);
			System.out.println(rowKey+"记录删除成功！");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(rowKey+"记录删除失败！");
		}
	}
	/**
	 * 获取表的所有数据
	 * @param tableName
	 */
	public void getALLData(String tableName) {
		try {
			HTable hTable = new HTable(conf, tableName);
			Scan scan = new Scan();
			ResultScanner scanner = hTable.getScanner(scan);
			for (Result result : scanner) {
				if(result.raw().length==0){
					System.out.println(tableName+" 表数据为空！");
				}else{
					for (KeyValue kv: result.raw()){
						System.out.println(new String(kv.getRow())+"\t"+new String(kv.getValue()));
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	/**
	 * 查询一条记录
	 * @param tableName
	 * @param rowKey
	 */
	private void getKey(String tableName, String rowKey) {
		HTablePool hTablePool = new HTablePool(conf, 1000);
		HTableInterface table = hTablePool.getTable(tableName);
		Get get = new Get(rowKey.getBytes());
		try {
			Result result = table.get(get);
			System.out.println("查询一条记录："+result);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("查询失败！");
		}
	}
	/**
	 * 添加一条记录
	 * @param tableName
	 * @param rowkey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	private void addOneRecord(String tableName, String rowkey, String family,
			String qualifier, String value) {
		//创建HTable连接池
		HTablePool hTablePool = new HTablePool(conf, 1000);
		//获得表明
		HTableInterface table = hTablePool.getTable(tableName);
		//put Object
		Put put = new Put(rowkey.getBytes());
		put.addImmutable(family.getBytes(), qualifier.getBytes(), value.getBytes());
		//添加进table
		try {
			table.put(put);
			System.out.println("添加记录"+rowkey+"成功！");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("添加记录"+rowkey+"失败！");
		}
	}
	// 添加一条记录
	public  void put(String tableName, String row, String columnFamily,
			String column, String data) throws IOException {
		HTablePool hTablePool = new HTablePool(conf, 1000);
		HTableInterface table = hTablePool.getTable(tableName);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put'" + row + "'," + columnFamily + ":" + column
				+ "','" + data + "'");	
	}
	/**
	 * 创建一张表
	 * @param tableName
	 * @param column
	 * @throws Exception
	 */
	private void createTable(String tableName, String column) throws Exception {
		if (admin.tableExists(tableName)) {
			System.out.println(tableName+"表已经存在！");
			
		}else{
			//创建表的描述
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.addFamily(new HColumnDescriptor(column.getBytes()));
			admin.createTable(tableDesc);
			System.out.println(tableName+"表创建成功！");
		}
		
	}
	/**
	 * 查询所有表明
	 * @return
	 * @throws Exception
	 */
	
	private List<String> getALLTable() throws Exception {
		ArrayList<String> tables = new ArrayList<String>();
		if (admin!=null) {
			HTableDescriptor[] listTables = admin.listTables();
			if (listTables.length>0) {
				for (HTableDescriptor tableDesc : listTables) {
					tables.add(tableDesc.getNameAsString());
					System.out.println(tableDesc.getNameAsString());
				}
				
			}
			
		}
		return tables;
		
	}
	

}
