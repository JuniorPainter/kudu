package cn.wi.kudu.createtable;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @ProjectName: kudu_impala
 * @ClassName: KuduInitializationDemo
 * @Author: xianlawei
 * @Description: 基于Java API对Kudu 进行CRUD操作，包含创建表，对数据CRUD及上传的操作
 * @date: 2019/9/10 16:58
 */
public class KuduInitializationDemo {
    /**
     * 对于Kudu的操作，获取KuduClient客户端实例对象
     */
    private KuduClient kuduClient = null;

    /**
     * 初始化KuduClient实例对象
     */
    @Before
    public void initialization() {
        //Kudu Master Server地址信息
        String masterAddress = "node01:7051,node02:7051,node03:7051";

        //构建KuduClient实例对象
        kuduClient = new KuduClient
                //使用建造者模式，构建KuduClient
                .KuduClientBuilder(masterAddress)
                //设置超时时间间隔，默认值为10s
                .defaultOperationTimeoutMs(6000)
                //采用建造者模式构建实例对象
                .build();
    }

    /**
     * 用于构建kudu表中每个字段的信息
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


    /**
     * 创建Kudu中的表，表的结构如下所示：
     *      create table users(
     *      id int,
     *      name string,
     *      age byte,
     *      primary key(id)
     *      )
     *      partition by hash(id) partitions 3
     *      stored as kudu ;
     */
    @Test
    public void createKuduTable() throws KuduException {
        //构建表的Schema信息
        ArrayList<ColumnSchema> columnSchemas = new ArrayList<>();
        //TODO 添加Kudu表的每一列的定义
        //newColumnSchema(String name, Type type, boolean isKey
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
        columnSchemas.add(newColumnSchema("name", Type.STRING, false));
        columnSchemas.add(newColumnSchema("age", Type.INT8, false));

        //定义Schema
        Schema schema = new Schema(columnSchemas);

        //指定kudu表的分区策略及分区副本数目设置
        CreateTableOptions createTableOptions = new CreateTableOptions();

        //哈希分区
        List<String> columns = new ArrayList<>();
        //按照id进行分区操作
        columns.add("id");
        //设置三个分区数目
        createTableOptions.addHashPartitions(columns, 3);

        //副本数设置 必须是奇数
        createTableOptions.setNumReplicas(3);

        KuduTable userTable = kuduClient.createTable("users", schema, createTableOptions);

        System.out.println(userTable.toString());
    }

    @Test
    public void deleteKuduTable() throws KuduException {
        String name ="users";
        if (kuduClient.tableExists(name)){
            DeleteTableResponse deleteTableResponse = kuduClient.deleteTable(name);
            System.out.println("deleteTableResponse = " + deleteTableResponse);
        }
    }
    /**
     * 操作结束，关闭KuduClient
     */
    @After
    public void close() throws KuduException {
        if (kuduClient != null) {
            kuduClient.close();
        }
    }
}
