import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class NewIO_TcpTester {

	private static int BUFSIZE = 32768;
	private static final int connectionCount = 1024;
    private final AtomicLong byteCount = new AtomicLong();
	
	private InetSocketAddress startServer() throws Exception {
		final Selector selector = SelectorProvider.provider().openSelector();
        final ServerSocketChannel server = ServerSocketChannel.open();
        final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFSIZE);
        server.configureBlocking(false);
        server.socket().bind(null);
        server.register(selector, SelectionKey.OP_ACCEPT);
        new Thread(new Runnable() {
			public void run() {
				while(true) {
					try {
						if (selector.select() != 0) {
					        Iterator<SelectionKey> selectedKeys =
					        	selector.selectedKeys().iterator();
					        while (selectedKeys.hasNext()) {
					            SelectionKey key = (SelectionKey) selectedKeys.next();
					            if (!key.isValid()) {
					                continue;
					            }
					            if (key.isAcceptable()) {
					                ServerSocketChannel serverSocketChannel =
					                	(ServerSocketChannel) key.channel();
					                SocketChannel socketChannel = serverSocketChannel.accept();
					                socketChannel.configureBlocking(false);
					                socketChannel.register(selector, SelectionKey.OP_READ);
					            } else if (key.isReadable()) {
					                SocketChannel socketChannel = (SocketChannel) key.channel();
					                buffer.clear();
					                socketChannel.read(buffer);
					                byteCount.addAndGet(buffer.position());
					            }
					            selectedKeys.remove();
					        }
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
        }, "server").start();
        return new InetSocketAddress(server.socket().getLocalPort());
	}
	
	private void startClients(InetSocketAddress address) throws Exception {
		final List<SocketChannel> clients = new ArrayList<SocketChannel>();
		final Selector selector = SelectorProvider.provider().openSelector();
        final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFSIZE);
        for (int i = 0; i < BUFSIZE; i++) {
        	buffer.put((byte)(i % 256));
        }
        buffer.flip();
		for (int i = 0; i < connectionCount; i++) {
            SocketChannel client = SocketChannel.open();
            client.configureBlocking(false);
            client.register(selector, SelectionKey.OP_CONNECT);
            if (client.connect(address)) {
                client.register(selector, SelectionKey.OP_WRITE);
                clients.add(client);
                System.out.println("A client was directly connected.");
                System.out.println(clients.size() + " clients are now connected.");
            }
		}
		new Thread(new Runnable() {
			public void run() {
				while(true) {
					try {
						if (selector.select() != 0) {
							Iterator<SelectionKey> selectedKeys =
								selector.selectedKeys().iterator();
							while (selectedKeys.hasNext()) {
								SelectionKey key = (SelectionKey) selectedKeys.next();
								if (!key.isValid()) {
									continue;
								}
								SocketChannel client = (SocketChannel) key.channel();
								if (key.isConnectable()) {
									if (client.finishConnect()) {
										client.register(selector, SelectionKey.OP_WRITE);
										clients.add(client);
										System.out.println(clients.size() +
												" clients are now connected.");
									}
								} else if (key.isWritable()) {
									buffer.rewind();
									client.write(buffer);
								}
								selectedKeys.remove();
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}, "clients").start();
	}
	
	private void logStatistics() throws Exception {
		while (true) {
			Thread.sleep(1000);
			System.out.println("" + 1.0 * byteCount.getAndSet(0) / 1024);
		}
	}
	
	public static void main(String[] args) throws Exception {
		NewIO_TcpTester tester = new NewIO_TcpTester();
		tester.startClients(tester.startServer());
		tester.logStatistics();
	}

}
