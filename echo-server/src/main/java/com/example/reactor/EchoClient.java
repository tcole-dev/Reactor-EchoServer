package com.example.reactor;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

public class EchoClient {
    // 服务端端口
    private int port = 8080;
    // 服务端地址
    private String host = "localhost";

    public void start() {
        try(SocketChannel socketChannel = SocketChannel.open();
            Scanner scanner = new Scanner(System.in);) {
            
            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress(host, port));
            while (!socketChannel.finishConnect()) {
                // 等待连接完成
                Thread.sleep(100);
            }

            ByteBuffer buffer = null;
            System.out.println("连接服务器成功: " + host + ":" + port);
            System.out.println("输入文本:");

            while (true) {
                System.out.print(">");
                String line = scanner.nextLine();
                if (line.equalsIgnoreCase("quit")) {
                    break;
                }

                // 动态调整缓冲区大小，适应不同长度的输入
                buffer = ByteBuffer.allocate(line.getBytes().length);
                buffer.clear();
                buffer.put(line.getBytes());
                buffer.flip();
                
                // 写数据时可能出现半包情况，因此使用循环确保数据全部写入
                // 这和服务端的写入逻辑相对应，只不过client端没有采用事件驱动的方式，只需要while循环即可
                while (buffer.hasRemaining()) {
                    socketChannel.write(buffer);
                }

                buffer.clear();
                int outTime = 0;                  // 响应超时次数
                int waitTime = 100;               // 每次等待时间，指数级增长
                StringBuilder messageBuilder = new StringBuilder();
                boolean isRead = false;           // 是否已读到数据
                // 等待响应，最多5次
                while (outTime < 5) {
                    Thread.sleep(waitTime);
                    while (socketChannel.read(buffer) > 0) {
                        buffer.flip();
                        messageBuilder.append(new String(buffer.array(), 0, buffer.limit()));
                        buffer.clear();
                        isRead = true;
                    }
                    if (isRead) {
                        break;
                    }
                    waitTime *= 2;
                    outTime++;
                }
                
                if (!isRead) {
                    System.out.println("未收到服务器响应");
                } else {
                    System.out.println("收到服务器响应: " + messageBuilder.toString());
                }

            }
            socketChannel.close();
            System.out.println("关闭连接");

        } catch (Exception e) {
            System.out.println("服务端连接失败: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        new EchoClient().start();
    }
}
