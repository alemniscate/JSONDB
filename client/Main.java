package client;

import java.io.*;
import java.net.*;
import com.google.gson.Gson;

public class Main {

    public static void main(String[] args) {
        System.out.println("Client started!");
        Gson gson = new Gson();

        String address = "127.0.0.1";
        int port = 23456;

        try (
            Socket socket = new Socket(InetAddress.getByName(address), port);
            DataInputStream input = new DataInputStream(socket.getInputStream());
            DataOutputStream output  = new DataOutputStream(socket.getOutputStream())
        ) {
            Argument argument = new Argument(args, gson);
            String req = argument.getReq();    
            output.writeUTF(req); 
            System.out.println("Sent: " + req);
         
            String res = input.readUTF(); 
            System.out.println("Received: " + res);

        } catch (IOException e) {
            e.printStackTrace();
        }  
    }
}

class TKVReq {
    String type;
    String key;
    String value;

    TKVReq(String type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }
}

class TKReq {
    String type;
    String key;

    TKReq(String type, String key) {
        this.type = type;
        this.key = key;
    }
}

class TReq {
    String type;

    TReq(String type) {
        this.type = type;
    }
}

class Argument {

    String[] args;
    Gson gson;

    Argument(String[] args, Gson gson) {
        this.args = args;
        this.gson = gson;
    }

    String getReq() {
        String type = "";
        String key = "";
        String value = "";
        String fileName = "";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-t":
                    type = args[i + 1];
                    break;
                case "-k":
                    key = args[i + 1];
                    break;
                case "-v":
                    value = args[i + 1];
                    break;
                case "-in":
                    fileName = args[i + 1];
                    break;
                default:
                    break;
            }
        }

        if (!"".equals(fileName)) {
            return getReqRecord("src\\client\\data\\" + fileName);
        }

        String req = "";
        if (!"".equals(type) && !"".equals(key) && !"".equals(value)) {
            req = gson.toJson(new TKVReq(type, key, value));
        } else if (!"".equals(type) && !"".equals(key)) {
            req = gson.toJson(new TKReq(type, key));
        } else if (!"".equals(type)) {
            req = gson.toJson(new TReq(type));   
        }

        return req;
    }

    String getReqRecord(String fileName) {
        File file = new File(fileName);
        System.out.println(file.getAbsolutePath());

        String req = "";
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String rec;
            while ((rec = reader.readLine()) != null) {
                req += rec;
            }
            reader.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return req;
    }

    String getReqDisplay() {
        String command = "";
        String index = "";
        String message = "";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-t":
                    command = args[i + 1];
                    break;
                case "-i":
                    index = args[i + 1];
                    break;
                case "-m":
                    message = args[i + 1];
                    if (message.startsWith("\"")) {
                        message = message.substring(1);
                    }
                    if (message.endsWith("\"")) {
                        message = message.substring(0, message.length() - 1);
                    }
                    break;
                default:
                    break;
            }
        }

        String req = (command + " " + index + " " + message).trim();

        return req;
    }

}
