import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.util.Iterator;

// 从 Reactor，SubReactor 负责处理客户端的读写事件
public class SubReactor implements Runnable {
    // SubReactor的Selector，用于监听客户端读写事件
    private Selector selector;

    // 待注册的Channel
    private ConcurrentLinkedQueue<Channel> pendingChannels;

    // SubReactor序号
    private int index;

    public int getIndex() {
        return index;
    }

    public SubReactor(int index) throws IOException {
        this.index = index;
        this.selector = Selector.open();
        this.pendingChannels = new ConcurrentLinkedQueue<>();
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 处理待注册的Channel
                processRegister();

                selector.select();
                // 在这里被唤醒，此时没有事件处理，会继续循环，在下一个循环处理待注册的Channel

                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    dispatch(key);    
                }


            } catch (IOException Exception) {
                System.out.println("SubReactor监听事件失败: " + Exception.getMessage());
            }
        }
    }

    private void processRegister() throws IOException {
        SocketChannel socketChannel = null;
        while ((socketChannel = (SocketChannel) pendingChannels.poll()) != null) {
            try {
                // 每个Channel单独持有一个Handler
                socketChannel.register(selector, SelectionKey.OP_READ, new Handler(socketChannel, this));
                System.out.println("连接 " + socketChannel.getRemoteAddress() + "成功注册到 SubReactor-" + index );
            } catch (Exception e) {
                System.out.println("连接 " + socketChannel.getRemoteAddress() + "注册失败");
                socketChannel.close();
            }
        }
    }

    // 添加Channel到待注册队列，并唤醒Selector，因为（selector会因为select方法阻塞）
    public void registerChannel(SocketChannel socketChannel) {
        pendingChannels.add(socketChannel);
        selector.wakeup();
    }

    // 执行不同任务
    private void dispatch(SelectionKey key) {
        Handler handler = (Handler) key.attachment();
        try {
            if (key.isReadable()) {
                handler.handleRead();
            }
            if (key.isWritable()) {
                handler.handleWrite();
            }
        } catch (Exception e) {
            System.out.println("SelectionKey已经被取消");
        }
    }

    public Selector getSelector() {
        return selector;
    }
}
