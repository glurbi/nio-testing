import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class NewIO_UdpTester {

	private static int BUFSIZE = 10000;
	private static int channelCount = 2000;
    private final AtomicLong byteCount = new AtomicLong();
    private final AtomicLong messageCount = new AtomicLong();
	
	private int startServer() throws Exception {
        final DatagramChannel server = DatagramChannel.open();
        final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFSIZE);
        server.configureBlocking(true);
        server.socket().bind(new InetSocketAddress(0));
        System.out.println("Server bound to " + server.socket().getLocalSocketAddress());
        new Thread(new Runnable() {
			public void run() {
				while(true) {
					try {
		                buffer.clear();
		                server.receive(buffer);
						//System.out.println("recv: " + buffer.position());
		                byteCount.addAndGet(buffer.position());
		                messageCount.incrementAndGet();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
        }, "server").start();
        return server.socket().getLocalPort();
	}
	
	private void startClient(final int port) throws Exception {
		final Selector selector = SelectorProvider.provider().openSelector();
        final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFSIZE);
        for (int i = 0; i < BUFSIZE; i++) {
        	buffer.put((byte)(i % 256));
        }
        buffer.flip();
        final List<DatagramChannel> channels = new ArrayList<DatagramChannel>();
        InetSocketAddress remote = new InetSocketAddress("127.0.0.1", port);
        for (int i = 0; i < channelCount; i++) {
        	DatagramChannel channel = DatagramChannel.open();
        	channel.configureBlocking(false);
        	channel.socket().bind(null);
        	channels.add(channel);
        	InetSocketAddress local = new InetSocketAddress(channel.socket().getLocalPort());
        	channel.connect(remote);
        	System.out.println("Client " + i + " [" + local + "] connected to " + remote);
        	channel.register(selector, SelectionKey.OP_WRITE);
        }
		new Thread(new Runnable() {
			public void run() {
				while(true) {
					try {
						if (selector.select() == 0) {
							continue;
						}
						Iterator<SelectionKey> it = selector.selectedKeys().iterator();
						while (it.hasNext()) {
							DatagramChannel channel = (DatagramChannel) it.next().channel();
							buffer.rewind();
							channel.write(buffer);
							//System.out.println("sent: " + buffer.position());
							it.remove();
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
		}, "client").start();
	}
	
	private void logStatistics() throws Exception {
		while (true) {
			Thread.sleep(1000);
			System.out.print("" + 1.0 * byteCount.getAndSet(0) / 1024 + " ");
			System.out.println("" + messageCount.getAndSet(0));
		}
	}
	
	public static void main(String[] args) throws Exception {
		NewIO_UdpTester tester = new NewIO_UdpTester();
		tester.startClient(tester.startServer());
		tester.logStatistics();
	}

}
