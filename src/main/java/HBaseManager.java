
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 *
 */
public class HBaseManager
{

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    /**
     * 初始化链接
     */
    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","172.16.25.21,172.16.25.22");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        configuration.set("zookeeper.znode.parent","/hbase");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     */
    public static  void close(){
        try {
            if(null != admin)
                admin.close();
            if(null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 建表
     * @param tableNmae
     * @param cols
     * @throws IOException
     */
    public static void createTable(String tableNmae,String[] cols) throws IOException {

        init();
        TableName tableName = TableName.valueOf(tableNmae);

        if(admin.tableExists(tableName)){
            System.out.println("talbe is exists!");
        }else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String col:cols){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        close();
    }


    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException{
        init();
        TableName tn = TableName.valueOf(tableName);
        if(admin.tableExists(tn)){
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }



    /**
     * 列出多有的表
     * @throws IOException
     */
    public static void listTables() throws IOException{
        init();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for(HTableDescriptor ht : hTableDescriptors){
            System.out.println(ht.getNameAsString());
        }
        close();
    }

    /**
     * 插入数据
     * @param tableName
     * @param rowkey
     * @param colFamily
     * @param col
     * @param val
     * @throws IOException
     */
    public static void insterRow(String tableName, String rowkey, String colFamily, String col, String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);

        //批量插入
        List<Put> putList = new ArrayList<Put>();
        putList.add(put);
        table.put(putList);
        table.close();
        close();
    }

    /**
     * 删除数据
     * @param tableName
     * @param rowkey
     * @param colFamily
     * @param col
     * @throws IOException
     */
    public static void delRow(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete del = new Delete(Bytes.toBytes(rowkey));
        //删除指定的列族
        del.addFamily(Bytes.toBytes(colFamily));
        //删除指定的列
        del.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        table.delete(del);

        //批量删除
        List<Delete> delList = new ArrayList<Delete>();
        delList.add(del);
        table.delete(delList);
        table.close();
        close();
    }

    /**
     * 根据rowkey查找数据
     * @param tableName
     * @param rowkey
     * @param colFamily
     * @param col
     * @throws IOException
     */
    public static void getData(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        //获取指定列族的数据
        get.addFamily(Bytes.toBytes(colFamily));
        //获取指定咧的数据
        get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        Result res = table.get(get);

        showCell(res);
        table.close();
        close();
    }

    /**
     * 格式化数据
     * @param result
     */
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for (Cell cell: cells){
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("TimeStamp: " + new String(cell.getTimestamp() + " "));
            System.out.println("column Family: " + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name: " + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value: " + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

    /**
     * 批量查找
     * @param tableName
     * @param startRow
     * @param stopRow
     * @throws IOException
     */
    public static void scanData(String tableName, String startRow, String stopRow) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner results = table.getScanner(scan);
        for (Result res : results){
            showCell(res);
        }
        table.close();
        close();
    }















    public static void main( String[] args ) throws IOException
    {
//        createTable("t3",new String[]{"cf1","cf2"});
//        insterRow("t3", "sdfdsfdsfds","cf1","h1","hello world");
//        listTables();
        getData("t3", "sdfdsfdsfds","cf1","h1");
    }
}
