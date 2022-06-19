package Client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnect {

    public static void main(String[] args) throws IOException {
        //获取配置类
        Configuration configuration = HBaseConfiguration.create();
        //给配置类添加信息
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println(connection);
    //关闭连接
        connection.close();
    }

}
