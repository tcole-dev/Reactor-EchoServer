import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Handler {
    // 当前连接的SocketChannel
    private SocketChannel socketChannel;
    // 对应的SubReactor
    private SubReactor subReactor;
    // 读缓冲区
    private ByteBuffer readBuffer;
    // 写缓冲区
    private ByteBuffer writeBuffer;

    public Handler(SocketChannel socketChannel, SubReactor subReactor) {
        this.socketChannel = socketChannel;
        this.subReactor = subReactor;
        this.readBuffer = ByteBuffer.allocate(1024);
        this.writeBuffer = null;
    }

    public void handleRead() {
        readBuffer.clear();
        try {
            int readLength;
            int totalReadLength = 0;
            StringBuilder messageBuilder = new StringBuilder();
            while ((readLength = socketChannel.read(readBuffer)) > 0) {
                readBuffer.flip();
                messageBuilder.append(new String(readBuffer.array(), 0, readLength));
                readBuffer.clear();
                totalReadLength += readLength;
            }

            System.out.println("客户端 " + socketChannel.getRemoteAddress() + " 发送消息: " + messageBuilder.toString());

            if (readLength == -1) {
                System.out.println("连接 " + socketChannel.getRemoteAddress() + "已关闭");
                closeConnection();
                return;
            }

            writeBuffer = ByteBuffer.allocate(totalReadLength);
            writeBuffer.put(messageBuilder.toString().getBytes());
            writeBuffer.flip();

            // 注册为写事件
            SelectionKey selectionKey = socketChannel.keyFor(subReactor.getSelector());
            if (selectionKey != null) {
                selectionKey.interestOps(SelectionKey.OP_WRITE);
                // 唤醒负责监听读写的SubReactor线程，使其立即处理新的事件
                subReactor.getSelector().wakeup();
            }

        } catch (IOException e) {
            System.out.println("读取数据失败: " + e.getMessage());
            closeConnection();
            return;
        }
    }

    private void switchToRead() { 
        SelectionKey selectionKey = socketChannel.keyFor(subReactor.getSelector());
        if (selectionKey != null) {
            selectionKey.interestOps(SelectionKey.OP_READ);
            subReactor.getSelector().wakeup();
        }
    }
    public void handleWrite() {
        if (writeBuffer == null || !writeBuffer.hasRemaining()) {
            writeBuffer = null;
            switchToRead();
            return;
        }
        try {
            socketChannel.write(writeBuffer);
            if (!writeBuffer.hasRemaining()) {
                // 数据全部写完后退出写事件，重新注册为读事件
                writeBuffer = null;
                switchToRead();
            }
        } catch (Exception e) {
            System.out.println("写入数据失败: " + e.getMessage());
            closeConnection();
            return;
        }
    }


    private void closeConnection() { 
        try {
            SelectionKey selectionKey = socketChannel.keyFor(subReactor.getSelector());
            if (selectionKey != null) {
                selectionKey.cancel();
            }
            socketChannel.close();

            System.out.println("连接 " + socketChannel.getRemoteAddress() + "关闭");
        } catch (Exception e) {}
    }
}
