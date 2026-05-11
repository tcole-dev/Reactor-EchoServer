package com.example.netty;

import java.util.Scanner;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

public class EchoClient {
    int port;
    String host;

    public EchoClient(int port, String host) {
        this.port = port;
        this.host = host;
    }

    public void start() throws InterruptedException { 
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                            new LineBasedFrameDecoder(1024),
                            new StringDecoder(CharsetUtil.UTF_8),
                            new StringEncoder(CharsetUtil.UTF_8),
                            new EchoClientHandler()
                        );
                    }
                });

            System.out.println("正在连接服务器: " + host + ":" + port);

            // 连接服务器，sync()表示阻塞到连接完成
            ChannelFuture future = bootstrap.connect(host, port).sync();
            io.netty.channel.Channel channel = future.channel();

            System.out.println("连接服务器成功: " + host + ":" + port);

            System.out.println("输入文本 (输入 'quit' 退出):");
            try (Scanner scanner = new Scanner(System.in);) {
                while (true) {
                    System.out.print("> ");
                    String input = scanner.nextLine();
                    if ("quit".equals(input)) {
                        break;
                    }
                    channel.writeAndFlush(input + "\n");
                    Thread.sleep(500);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            channel.close().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
    public static void main(String[] args) throws Exception { 
        new EchoClient(8080, "localhost").start();
    }
}

class EchoClientHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        String message = (String) msg;
        System.out.println("收到服务器响应: " + message);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("发生异常: " + cause.getMessage());
        ctx.close();
    }
}