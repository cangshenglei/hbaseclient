package Client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class Hbasedml {

    public static Connection connection=null;

    static {
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void putCell(String nameSpace,String tableName,String rowkey,String family,String column,String value) throws IOException {
        //获取table
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        //创建put对象
        Put put = new Put(Bytes.toBytes(rowkey));
        //添加put属性
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
        //传递put数据
        table.put(put);
        //关闭资源
        table.close();
    }

    public static String getCells(String nameSpace,String tablename,String rowkey,String family,String column) throws IOException {
        //获取table
        Table table = connection.getTable(TableName.valueOf(nameSpace, tablename));
        //获取Get对象
        Get get = new Get(Bytes.toBytes(rowkey));
        //添加get属性
        get.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));
        //get数据
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        String values="";
        for (Cell cell : cells) {
            values +=Bytes.toString(CellUtil.cloneValue(cell))+ "-";
        }
        table.close();
        return values;
    }

        public static void deleteColumn(String nameSpace,String tableName,String rowKey,String family,String column) throws IOException {
        //获取table
            Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
            //创建delete对象
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            //添加删除信息
            //同名的方法
            //1删除单个版本
            delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));
            //删除全部版本
            delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));
            //删除列族
            delete.addFamily(Bytes.toBytes(family));
            table.delete(delete);
            table.close();
        }


            public static List<String> scanRows(String nameSpace, String tableName, String startRow, String stopRow) throws IOException {
                Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
                //创建Scan对象
                Scan scan = new Scan().withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));
                //扫描数据
                ResultScanner scanner = table.getScanner(scan);
                //获取结果
                ArrayList<String> arrayList = new ArrayList<>();
                for (Result result : scanner) {
                    arrayList.add(Bytes.toString(result.value()));
                }
                scanner.close();
                table.close();
                return arrayList;
            }

    public static void closeConnection() throws IOException {
     if (connection!=null){
         connection.close();
     }
    }

    public static void main(String[] args) throws IOException {
        putCell("bigdata","student","1001","info","name","zhangsan");
        getCells("bigdata","student","1001","info","name");
        deleteColumn("bigdata", "student", "1001", "info", "name");
        List<String>strings=scanRows("bigdata", "student", "1001", "2000");
        for (String string : strings) {
            System.out.println(string);
        }
    }
}
