package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import org.msgpack.MessagePack;
import org.msgpack.annotation.Message;
import org.msgpack.template.Template;
import static org.msgpack.template.Templates.tList;
import static org.msgpack.template.Templates.tMap;
import static org.msgpack.template.Templates.TString;
import static org.msgpack.template.Templates.*;

import java.nio.ByteBuffer;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.Properties;

public class MyBasicDB extends DB {

    public static final int OK = 0;
    public static final int ERROR = -1;

    public static final int BUFFER_SIZE = 1024 * 10;

    public static final String BASIC_CLUSTER_HOSTS = "basic_cluster_hosts";
    public static final String BASIC_CLUSTER_HOST_DEFAULT = "127.0.0.1:10017";

    private ArrayList<Server> servers = null;
    private Random randomGenerator;

    public static class Server {
        // public fields are serialized.
        public Socket socket = null;
        public DataOutputStream out = null;
        public DataInputStream in = null;
    }

    @Message // Annotation
    public static class GET {
        // public fields are serialized.
        public String code = "GET";
        public String table;
        public String key;
    }

    @Message // Annotation
    public static class PUT {
        // public fields are serialized.
        public String code = "PUT";
        public String table;
        public String key;
        public HashMap<String,byte[]> value;
    }

    @Message // Annotation
    public static class UPDATE {
        // public fields are serialized.
        public String code = "UPDATE";
        public String table;
        public String key;
        public HashMap<String,byte[]> value;
    }

    @Message // Annotation
    public static class DELETE {
        // public fields are serialized.
        public String code = "DELETE";
        public String table;
        public String key;
    }

    @Message // Annotation
    public static class GET_RESPONSE {
        // public fields are serialized.
        public String status;
        public HashMap<String,byte[]> value;
    }

    @Message // Annotation
    public static class UPD_RESPONSE {
        // public fields are serialized.
        public String status;
    }

    @Override
    public void init() throws DBException {

        try {
            this.randomGenerator = new Random();
            Properties props = getProperties();
            String cluster_hosts = props.getProperty(BASIC_CLUSTER_HOSTS, BASIC_CLUSTER_HOST_DEFAULT);
            String[] hosts = cluster_hosts.split(",");
            setupConnection(props, hosts);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException("Error connecting to MyBasicDB: " + e.getMessage());
        }
    }

    //Read a single record
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String,ByteIterator> result) {
        int result_db = ERROR;
        // System.out.println("GET key:"+table+key);
        try {
            Server s = getServer();

            GET get = new GET();
            get.table = table;
            get.key = key;

            MessagePack msgpack = new MessagePack();
            byte[] raw = msgpack.write(get);
            s.out.write(raw);

            byte[] res = new byte[BUFFER_SIZE];
            int len = s.in.read(res);
            byte[] res2 = Arrays.copyOf(res, len);
            GET_RESPONSE res3 = msgpack.read(res2, GET_RESPONSE.class);
            // System.out.println("1: Received : st: " + res3.status + " val:" + res3.value.toString());
            if(res3.status.equals("OK")) {
                deserialize(res3.value, result);
                // System.out.println("2: Received : st: " + res3.status + " val:" + result.toString());
                result_db = OK;
            }
        } catch(Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return result_db;
    }

    //Insert a single record
    @Override
    public int insert(String table, String key, HashMap<String,ByteIterator> values) {
        int result_db = ERROR;
        try {
            Server s = getServer();

            PUT put = new PUT();
            put.table = table;
            put.key = key;
            put.value = serialize(values);

            MessagePack msgpack = new MessagePack();
            byte[] raw = msgpack.write(put);
            s.out.write(raw);

            byte[] res = new byte[BUFFER_SIZE];
            int len = s.in.read(res);
            byte[] res2 = Arrays.copyOf(res, len);
            UPD_RESPONSE res3 = msgpack.read(res2, UPD_RESPONSE.class);
            if(res3.status.equals("OK")) {
                result_db = OK;
            }
        } catch(Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return result_db;
    }

    //Update a single record
    @Override
    public int update(String table, String key, HashMap<String,ByteIterator> values) {
        int result_db = ERROR;
        try {
            Server s = getServer();

            UPDATE put = new UPDATE();
            put.table = table;
            put.key = key;
            put.value = serialize(values);

            MessagePack msgpack = new MessagePack();
            byte[] raw = msgpack.write(put);
            s.out.write(raw);

            byte[] res = new byte[BUFFER_SIZE];
            int len = s.in.read(res);
            byte[] res2 = Arrays.copyOf(res, len);
            UPD_RESPONSE res3 = msgpack.read(res2, UPD_RESPONSE.class);
            if(res3.status.equals("OK")) {
                result_db = OK;
            }
        } catch(Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return result_db;
    }

    //Delete a single record
    @Override
    public int delete(String table, String key) {
        int result_db = ERROR;
        try {
            Server s = getServer();

            DELETE put = new DELETE();
            put.table = table;
            put.key = key;

            MessagePack msgpack = new MessagePack();
            byte[] raw = msgpack.write(put);
            s.out.write(raw);

            byte[] res = new byte[BUFFER_SIZE];
            int len = s.in.read(res);
            byte[] res2 = Arrays.copyOf(res, len);
            UPD_RESPONSE res3 = msgpack.read(res2, UPD_RESPONSE.class);
            if(res3.status.equals("OK")) {
                result_db = OK;
            }
        } catch(Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return result_db;
    }

    //Perform a range scan
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result) {
        return OK;
    }

    @Override
    public void cleanup() throws DBException {
        try {
            for(Server s : this.servers) {
                s.out.close();
                s.in.close();
                s.socket.close();
            }
        }
        catch (IOException e) {
            System.out.println(e);
        }
    }

// Private Methods

    private void setupConnection(Properties props, String[] hosts) throws DBException {
        this.servers = new ArrayList<Server>();
        for(String h:hosts) {
            String[] ipAndPort = h.split(":");
            String ip = ipAndPort[0].trim();
            int port = Integer.parseInt(ipAndPort[1].trim());
            System.out.println("MyBasicDB connection to " + ip + ":" + port);
            Server s = new Server();
            try {
                s.socket = new Socket(ip, port);
                // s.socket.setSendBufferSize(BUFFER_SIZE);
                s.out = new DataOutputStream(s.socket.getOutputStream());
                s.in = new DataInputStream(s.socket.getInputStream());
            } catch (UnknownHostException e) {
                System.err.println("Don't know about host: "+h);
            } catch (IOException e) {
                System.err.println("Couldn't get I/O for the connection to: "+h);
            }
            this.servers.add(s);
        }
    }


    private HashMap<String, byte[]> serialize(Map<String,ByteIterator> values) {
      HashMap<String, byte[]> retVal = new HashMap<String, byte[]>();
      for (String key : values.keySet()) {
        retVal.put(key, values.get(key).toArray());
      }
      return retVal;
    }

    private void deserialize(Map<String, byte[]> ori, Map<String, ByteIterator> des) {
        for (String k : ori.keySet()) {
            des.put(k, new ByteArrayByteIterator(ori.get(k)));
        }
    }

    private Server getServer() {
        int index = randomGenerator.nextInt(this.servers.size());
        Server s = this.servers.get(index);
        return s;
    }

}
