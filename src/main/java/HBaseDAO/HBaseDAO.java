package HBaseDAO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.lang.String;


public class HBaseDAO {

    //declare static config
    private static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quoram", "localhost");
    }


    /**
     *
     * create table
     *
     * @tableName
     *
     * @family
     *
     */

    public static void createTable(String tableName, String[] family) throws Exception{
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("table already exist!");
            System.exit(0);
        } else {
            admin.createTable(desc);
            System.out.println("create table successfully!");
        }
    }





    /**
     *
     * Add data into table(available for columnFamily-known table)
     *
     * @rowKey rowKey
     *
     * @tableName tableName
     *
     * @column1 1st colummnFamily
     *
     * @value1 values of 1st columnFamily
     *
     * @column2 2nd columnFamily
     *
     * @value2 values of 2nd columnFamily
     *
     *
     *
     */


    public static void addData(String rowKey, String tableName, String[] column1, String[] value1, String[] column2, String[] value2) throws IOException {

        //set rowKey
        Put put = new Put(Bytes.toBytes(rowKey));

        //record CRUD for operation on table
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            //Get name of colFamily
            String familyName = columnFamilies[i].getNameAsString();
            //Add colFamily data named as article and author into put Object
            if (familyName.equals("article")) {
                for (int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }

            if (familyName.equals("author")) {
                for (int j = 0; j < column2.length; j++) {
                    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }

        }

        table.put(put);
        System.out.println("Add data successfully!");
    }


    /**
     *
     * query by RowKey
     *
     * @param rowKey
     *
     * @param tableName
     *
     */


    public static Result getResult(String tableName, String rowKey) throws Exception{
        Get get = new Get(Bytes.toBytes(rowKey));
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Result result = table.get(get);
        for (Cell cell : result.listCells()) {
            System.out.println("family:" + Bytes.toString(cell.getFamilyArray()));
            System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray()));
            System.out.println("value:" + Bytes.toString(cell.getValueArray()));
            System.out.println("Timestamp:" + cell.getTimestamp());
            System.out.println("----------------------------------");
        }

        return result;
    }






    /**
     *
     * traverse HBase table for query
     *
     * @param tableName
     *
     */

    public static void getResultScan(String tableName) throws IOException {
        Scan scan = new Scan();
        printTableInfo(tableName, scan);

    }


    /**
     *
     * traverse HBase table with start & stop rowKey
     *
     */

    public static void getResultScan(String tableName, String startingRowKey, String endingRowKey) throws IOException {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startingRowKey));
        scan.withStopRow(Bytes.toBytes(endingRowKey));
        printTableInfo(tableName, scan);


    }


    /**
     *
     * common method
     *
     * @param tableName
     * @param scan
     * @throws IOException
     */

    static void printTableInfo(String tableName, Scan scan) throws IOException{
        ResultScanner rs = null;
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {
                    System.out.println("row:" + Bytes.toString(cell.getRowArray()));
                    System.out.println("family:" + Bytes.toString(cell.getFamilyArray()));
                    System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray()));
                    System.out.println("value:" + Bytes.toString(cell.getValueArray()));
                    System.out.println("timestamp:" + cell.getTimestamp());
                    System.out.println("-------------------------------");
                }
            }
        } finally {
            connection.close();
            rs.close();
        }
    }


    /**
     *
     * Get Result By Column
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @throws Exception
     */

    public static void getResultByColumn(String tableName, String rowKey, String familyName, String columnName) throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //Get certain column
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        Result result = table.get(get);
        for (Cell cell : result.listCells()) {
            System.out.println("family:" + Bytes.toString(cell.getFamilyArray()));
            System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray()));
            System.out.println("value:" + Bytes.toString(cell.getValueArray()));
            System.out.println("timestamp:" + cell.getTimestamp());
            System.out.println("------------------------------");
        }
    }


    /**
     *
     * Update Table
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @param value
     * @throws IOException
     */

    public static void updateTable (String tableName, String rowKey, String familyName, String columnName, String value) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        table.put(put);
        System.out.println("Update table successfully!");
    }


    /**
     *
     * check versions for certain column
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @throws IOException
     */


    public static void getResultByVersion(String tableName, String rowKey, String familyName, String columnName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);
        for (Cell cell : result.listCells()) {
            System.out.println("family:" + Bytes.toString(cell.getFamilyArray()));
            System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray()));
            System.out.println("value:" + Bytes.toString(cell.getValueArray()));
            System.out.println("timestamp:" + cell.getTimestamp());
            System.out.println("--------------------------------");
        }
    }


    /**
     *
     * delete certain column
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @throws IOException
     */

    public static void deleteColumn(String tableName, String rowKey, String familyName, String columnName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.addColumn(Bytes.toBytes(tableName), Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println(familyName + ": " + columnName + "is deleted!");
    }


    /**
     *
     * delete certain column
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */


    public static void deleteAllColumn(String tableName, String rowKey) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);
        System.out.println("all columns are deleted!");
    }




    public static void deleteTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println(tableName + "is deleted!");
    }





}




