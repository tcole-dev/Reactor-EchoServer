import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class Acceptor {
    // 连接到的端口对应的ServerSocketChannel
    private ServerSocketChannel serverSocketChannel;
    // 服务端引用
    private EchoServer echoServer;

    public Acceptor(ServerSocketChannel serverSocketChannel, EchoServer echoServer) {
        this.serverSocketChannel = serverSocketChannel;
        this.echoServer = echoServer;
    }

    public void run() {
        try {
            SocketChannel socketChannel = serverSocketChannel.accept();

            if (socketChannel != null) {
                // 处理新的客户端连接
                socketChannel.configureBlocking(false);
                
                // 将新的客户端连接注册到SubReactor的Selector上
                SubReactor subReactor = echoServer.getNextSubReactor();

                subReactor.registerChannel(socketChannel);

                System.out.println("新连接: " + socketChannel.getRemoteAddress() + "被分配到SubReactor: " + subReactor.getIndex());
            }
        } catch (Exception e) {
            System.out.println("接受连接失败: " + e.getMessage());
        }
    }
}
