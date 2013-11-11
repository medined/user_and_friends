package com.affy;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

public class UserAndFriendsDriver {

    /*
     * ROW       | Column Family | Column Qualifier | Value
     * ----------------------------------------------------------
     * <user_id> | info:name     |                  | mark twain
     * <user_id> | info:gender   |                  | male
     * <user_id> | friend:<type> |                  | <user_id>
     * 
     * John, a male, has a old friend named Mark.
     * Mary, a female, has a new friend named Mark.
     */
    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream in = loader.getResourceAsStream("accumulo.properties");
        prop.load(in);

        String user = prop.getProperty("accumulo.user");
        String password = prop.getProperty("accumulo.password");
        String instanceInfo = prop.getProperty("accumulo.instance");
        String zookeepers = prop.getProperty("accumulo.zookeepers");
        
        Instance instance = new ZooKeeperInstance(instanceInfo, zookeepers);

        Connector connector = instance.getConnector(user, new PasswordToken(password));
        if (false == connector.tableOperations().exists("TABLEA")) {
            connector.tableOperations().create("TABLEA");
        }

        BatchWriter wr = connector.createBatchWriter("TABLEA", new BatchWriterConfig());
        Mutation m = new Mutation(new Text("john"));
        m.put("info:name", "", "john henry");
        m.put("info:gender", "", "male");
        m.put("friend:old", "mark", "");
        wr.addMutation(m);
        m = new Mutation(new Text("mary"));
        m.put("info:name", "", "mark wiggins");
        m.put("info:gender", "", "female");
        m.put("friend:new", "mark", "");
        m.put("friend:old", "lucas", "");
        m.put("friend:old", "aaron", "");
        wr.addMutation(m);
        wr.close();

        Scanner scanner = connector.createScanner("TABLEA", new Authorizations());
        scanner.setRange(new Range("a", "z"));
        scanner.fetchColumnFamily(new Text("friend:old"));
        Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Map.Entry<Key, Value> entry = iterator.next();
            Key key = entry.getKey();
            System.out.println(String.format("Old Friends: %s -> %s", key.getRow(), key.getColumnQualifier()));
        }
    }
}
