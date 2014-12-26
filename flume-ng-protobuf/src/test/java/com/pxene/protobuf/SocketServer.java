package com.pxene.protobuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by root
 * 2014/12/6.
 */
public class SocketServer {

    private final  static  Logger logger = LoggerFactory.getLogger(SocketServer.class);

    public static void main(String[] args) throws Exception{
        //服务端在20006端口监听客户端请求的TCP连接
        ServerSocket server = new ServerSocket(5140);
        Socket client = null;
        boolean f = true;
        while(f){
            //等待客户端的连接，如果没有获取连接
            client = server.accept();
            logger.debug("与客户端连接成功！");
            //为每个客户端连接开启一个线程
            new Thread(new SocketServerHandler(client)).start();
        }
        server.close();
    }
}
