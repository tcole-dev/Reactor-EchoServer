package com.example.reactor;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * 主 Reactor
 * 负责监听客户端连接事件，并将连接交给Acceptor进行处理
 */
public class MainReactor implements Runnable {

        // MainReactor的Selector，用于监听客户端连接事件
    private Selector selector;

    // ServerSocketChannel，接受客户端连接
    private ServerSocketChannel serverSocketChannel;

    // 服务端引用，用于Acceptor获取SubReactor
    private EchoServer echoServer;

    /**
     * 构造函数
     * @param serverSocketChannel 服务端SocketChannel，获取客户端连接
     * @param echoServer 服务端引用
     * @throws IOException 创建Selector失败
     */
    public MainReactor(ServerSocketChannel serverSocketChannel, EchoServer echoServer) throws IOException {
        this.selector = Selector.open();

        this.serverSocketChannel = serverSocketChannel;
        this.echoServer = echoServer;
        
        // MainReactor的selector开始监听OP_ACCEPT
        this.serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println("MainSubReactor初始化完成");
    }

    // 主Reactor的主循环，监听ACCEPT事件，分配给SubReactor
    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()) {
            try {
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    // 交给Acceptor处理SelectionKey（交给Acceptor处理）
                    dispatch(key);
                }
            } catch (IOException e) {
                System.out.println("MainReactor监听事件失败: " + e.getMessage());
            }
        }

        try {
            selector.close();
        } catch (IOException e) {
            System.out.println("关闭MainReactor的Selector失败: " + e.getMessage());
        }
    }

    /**
     * 分发SelectionKey
     * @param key 就绪的连接的SelectionKey
     */
    private void dispatch(SelectionKey key) {
        Acceptor acceptor = new Acceptor( (ServerSocketChannel) key.channel(), echoServer);
        acceptor.run();
    }
}

