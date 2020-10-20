package server;

import java.io.*;
import java.net.*;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class Main {
 
    public static void main(String[] args) {
        String fileName = "server\\data\\db.json";
//        File file = new File(fileName);
//        System.out.println(file.getAbsolutePath());
        Gson gson = new Gson();
        JsonDB db = new JsonDB(fileName, gson);
        HashMap<String, Object> map = db.load();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        ReadWriteLock lock = new ReentrantReadWriteLock();

        System.out.println("Server started!");
        String address = "127.0.0.1";
        int port = 23456;

        End.stopFlag = false;
        try (ServerSocket server = new ServerSocket(port, 50, InetAddress.getByName(address))) {
            while (!End.stopFlag) {
                Socket socket = server.accept(); 
                Task task = new Task(socket, map, gson, lock, db);
                executor.submit(task);
            } 
        } catch (IOException e) {
            e.printStackTrace();
        }

        executor.shutdown();
//        db.save();
        System.out.println("Server stoped!");
    }
}

class End {
    static boolean stopFlag = false;
}

class Task implements Runnable {
    
    Socket socket;
    HashMap<String, Object> map;
    Gson gson;
    ReadWriteLock lock;
    JsonDB db;
    Action action;
    TKVReq req; 
  
    Task(Socket socket,HashMap<String, Object> map, Gson gson, ReadWriteLock lock, JsonDB db) {
        this.socket = socket;
        this.map = map;
        this.gson = gson;
        this.lock = lock;
        this.db = db;
        try {
            DataInputStream input = new DataInputStream(socket.getInputStream());
            String jsonReq = input.readUTF();
            action = new Action(map, gson, lock, db);
            req = gson.fromJson(jsonReq, TKVReq.class);
            if ("exit".equals(req.type)) {
                End.stopFlag = true;
            } 
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void run() {
        try {
            String res = action.reply(req);
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeUTF(res);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }        
    }
}

class Action {
    HashMap<String, Object> map;
    Gson gson;
    ReadWriteLock lock;
    JsonDB db;

    Action(HashMap<String, Object> map, Gson gson, ReadWriteLock lock, JsonDB db) {
        this.map = map;
        this.gson = gson;
        this.lock = lock; 
        this.db = db;
    }

    String reply(TKVReq req) {

        String res = "ERROR";
        switch (req.type) {
            case "get":
                lock.readLock().lock();
                res = get(req.key);
                lock.readLock().unlock(); 
                break;
            case "set":
                lock.writeLock().lock();
                res = set(req.key, req.value);
                db.save();
                lock.writeLock().unlock();
                break;
            case "delete":
                lock.writeLock().lock();
                res = delete(req.key);
                db.save();
                lock.writeLock().unlock();
                break;
            case "exit":
                res = exit();
                break;
            default:
                break;
        }
        return res;
    }

    String get(Object keys) {

        List<String> list = jsonArrayToList(keys);
        int size = list.size();

        String key = list.get(0);
        Object value = map.get(key);
        if (value == null) {
            return errReply();  
        }

        for (int i = 1; i < size; i++) {
            key = list.get(i);
            String json = gson.toJson(value);
            HashMap<String, Object> map2 = gson.fromJson(json, new TypeToken<HashMap<String, Object>>(){}.getType());
            value = map2.get(key);
            if (value == null) {
                return errReply();  
            }
        }

        return okReply(value);
    } 

    String set(Object keys, Object value) {

        List<String> list = jsonArrayToList(keys);
        int size = list.size();

        String key = list.get(0);
        Object prevValue = map.get(key);
        if (size == 1) {
            map.put(key, value);
            return okReply(null);   
        }

        Object value2 = getValue(1, list, prevValue, value);
        if (value2 == null) {
            return errReply();
        }

        map.put(key,value2);
        return okReply(null);   
    }
   
    String delete(Object keys) {

        List<String> list = jsonArrayToList(keys);
        int size = list.size();

        String key = list.get(0);
        Object prevValue = map.get(key);
        if (prevValue == null) {
            return errReply();  
        }
        if (size == 1) {
            map.remove(key);
            return okReply(null);   
        }

        Object value2 = getValue(1, list, prevValue, null);
        if (value2 == null) {
            return errReply();
        }

        map.put(key,value2);
        return okReply(null);
    }

    String exit() {
        return okReply(null);
    }

    String okReply(Object value) {
        if (value == null) {
            return gson.toJson(new OkRes("OK"));
        }

        return gson.toJson(new OkValueRes("OK", value));
    }

    String errReply() {
        return gson.toJson(new ErrRes("ERROR", "No such key"));
    }

    List<String> jsonArrayToList(Object object) {
        String str = object.toString();
        str = str.replace("[", "").replace("]", "").replace(" ", "");
        String[] strs = str.split(",");
        ArrayList<String> list = new ArrayList<>();
        for (String i: strs) {
            list.add(i);
        }
        return list;
    }

    Object getValue(int index, List<String> keys, Object prevValue, Object value) {
        if (prevValue == null) {
            return null;
        }
        String key = keys.get(index);
        String json = gson.toJson(prevValue);
        HashMap<String, Object> map2 = gson.fromJson(json, new TypeToken<HashMap<String, Object>>(){}.getType());
        Object value2 = map2.get(key);
        if (value2 == null) {
            return null;  
        }
        if (index == keys.size() - 1) {
            if (value == null) {
                map2.remove(key);
            } else {
                map2.put(key, value);
            }       
        } else {
            map2.put(key, getValue(index + 1, keys, value2, value));
        }

        return map2;
    }
}

class TKVReq {
    String type;
    Object key;
    Object value;

    TKVReq(String type, Object key, Object value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }
}

class OkRes {

    String response;

    OkRes(String response) {
        this.response = response;
    }
}

class OkValueRes {

    String response;
    Object value;

    OkValueRes(String response, Object value) {
        this.response = response;
        this.value = value;
    }
}

class ErrRes {

    String response;
    String reason;

    ErrRes(String response, String reason) {
        this.response = response;
        this.reason = reason;
    }
}

class JsonDB {

    String fileName;
    Gson gson;
    HashMap<String, Object> map;

    JsonDB(String fileName, Gson gson) {
        this.fileName = fileName;
        this.gson = gson;
    }

    void save() {
 
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            gson.toJson(map, writer);
            writer.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
  
    HashMap<String, Object> load() {
 
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            map = gson.fromJson(reader, new TypeToken<HashMap<String, Object>>(){}.getType());
            if (map  == null) {
               map = new HashMap<>();
            }

            reader.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return map;
    }
}