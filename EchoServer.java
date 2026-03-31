import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;

public class EchoServer {
    // 监听端口
    private int port;
    // SubReactor线程数
    private static final int SUB_REACTOR_COUNT = Runtime.getRuntime().availableProcessors();
    // 主Reactor线程
    private MainReactor mainReactor;
    // SubReactor线程
    private SubReactor[] subReactors;
    // SubReactor轮询分配索引
    private int index = 0;

    public EchoServer(int port) throws IOException {
        this.port = port;
        subReactors = new SubReactor[SUB_REACTOR_COUNT];
        for (int i = 0; i < SUB_REACTOR_COUNT; i++) {
            subReactors[i] = new SubReactor(i);
            new Thread(subReactors[i], "SubReactor-" + i).start();
        }
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress(this.port));
        mainReactor = new MainReactor(serverSocketChannel, this);

        System.out.println("启动EchoServer成功: " + this.port);
    }

    public void start() {
        new Thread(mainReactor, "MainReactor").start();
    }



    public SubReactor getNextSubReactor() {
        SubReactor temp = subReactors[index];
        index = (index + 1) % SUB_REACTOR_COUNT;
        return temp;
    }


    public static void main(String[] args) { 
        try {
            EchoServer echoServer = new EchoServer(8080);
            echoServer.start();
        } catch (IOException e) {
            System.out.println("启动EchoServer失败: " + e.getMessage());
        }
    }
}
