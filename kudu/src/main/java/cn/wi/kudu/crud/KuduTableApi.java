package cn.wi.kudu.crud;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

/**
 * @ProjectName: kudu_impala
 * @ClassName: KuduTableDataCRUD
 * @Author: xianlawei
 * @Description: 使用Java API 对Kudu表的数据进行CRUD
 * @date: 2019/9/10 20:22
 */
public class KuduTableApi {
    /**
     * 声明一个KuduClient实例对象，用于创建表和删除表
     */
    private KuduClient kuduClient = null;

    @Before
    public void initialization() {
        //指定Kudu集群的地址
        String address = "node01:7051,node02:7051,node03:7051";
        kuduClient = new KuduClient
                //使用建造者模式构建KuduClient
                .KuduClientBuilder(address)
                //设置超时时间
                .defaultOperationTimeoutMs(6000)
                .build();
    }

    /**
     * @param name  字段名称
     * @param type  字段类型
     * @param isKey 是否为Key
     * @return ColumnSchema对象
     */
    private ColumnSchema newColumnSchema(String name, Type type, boolean isKey) {
        //创建ColumnSchemaBuilder实例对象
        ColumnSchema.ColumnSchemaBuilder columnSchemaBuilder = new ColumnSchema
                .ColumnSchemaBuilder(name, type);
        //设置是否为主键
        columnSchemaBuilder.key(isKey);
        return columnSchemaBuilder.build();
    }

    @Test
    public void insertKuduTable() throws KuduException {
        //获取对数据DML操作会话实例对象kuduSession
        KuduSession kuduSession = kuduClient.newSession();
        //设置手动提交  如果不调用Flush   数据不会提交
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(3000);

        //获取对某张表操作句柄KuduTable
        KuduTable kudutable = kuduClient.openTable("users");

        Random random = new Random();

        int number = 100;
        for (int i = 0; i < number; i++) {
            //构建插入Insert实例对象，表示向Kudu表中插入数据(类似构建HBase中Put对象）
            Insert insert = kudutable.newInsert();

            //获取Row的实例对象
            PartialRow row = insert.getRow();

            // 获取Row实例对象，设置每行数据中各个列的值
            row.addInt("id", 2001 + i);
            row.addString("name", "ZhaSan" + i);
            row.addByte("age", (byte) (20 + random.nextInt(10)));

            //使用KuduSession将数据应用到Kudu表中
            kuduSession.apply(insert);
        }
        //手动提交
        kuduSession.flush();

        //关闭
        kuduSession.close();
    }

    @Test
    public void queryKuduTable() throws KuduException {
        //获取对某张表操作句柄KuduTable
        KuduTable kuduTable = kuduClient.openTable("users");

        //构建KuduScanner扫描器，读取数据
        KuduScanner kuduScanner = kuduClient.newScannerBuilder(kuduTable).build();

        //查询数据集
        //表示Kudu表中每个Tablet编号
        int batchIndex = 1;
        //kuduScanner.hasMoreRows()  已经拿取到数据
        while (kuduScanner.hasMoreRows()) {
            System.out.println("batchIndex = " + (batchIndex++));
            //从迭代器中获取每个Tablet中的数据，封装在迭代器中
            RowResultIterator rowResults = kuduScanner.nextRows();
            //遍历迭代器
            while (rowResults.hasNext()) {
                RowResult result = rowResults.next();
                System.out.println(
                        "id = " + result.getInt("id")
                                + ",name=" + result.getString("name")
                                + ",age" + result.getByte("age")
                );
            }
        }
    }

    @After
    public void close() throws KuduException {
        if (kuduClient != null) {
            kuduClient.close();
        }
    }
}
