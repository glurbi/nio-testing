
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class StandardIO_TcpTester {

	private static final int BUFSIZE = 32768;
	private static final int connectionCount = 16;
    private final AtomicLong byteCount = new AtomicLong();

    public InetSocketAddress startNetworkDataConsumer() throws Exception {
    	final AtomicReference<InetSocketAddress> socketAddressRef =
    		new AtomicReference<InetSocketAddress>();
    	final CountDownLatch listenLatch = new CountDownLatch(1);
    	new Thread(new Runnable() {
			public void run() {
				ServerSocket serverSocket;
				try {
					serverSocket = new ServerSocket(0);
					socketAddressRef.set(new InetSocketAddress(
							serverSocket.getInetAddress(), serverSocket.getLocalPort()));
					listenLatch.countDown();
					while (true) {
						startReader(serverSocket.accept());
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
    	}, "accept").start();
    	listenLatch.await();
    	return socketAddressRef.get();
    }

    public void startReader(final Socket socket) {
    	new Thread(new Runnable() {
			public void run() {
				try {
					byte[] bytes = new byte[BUFSIZE];
					InputStream is = new BufferedInputStream(socket.getInputStream(), BUFSIZE);
					while (socket.isConnected()) {
						int count = is.read(bytes);
						//System.out.println(count);
						byteCount.addAndGet(count);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
    	}, "reader").start();
    }
    
    public void startNetworkDataProducer(final InetSocketAddress remoteAddress) {
        final byte[] stuff = "justfowrwardingsothecontentdoesnotmatter".getBytes();
        for (int i = 0; i < connectionCount; i++) {
        	new Thread(new Runnable() {
        		public void run() {
        			try {
        				Socket socket = new Socket(remoteAddress.getAddress(),
        						remoteAddress.getPort());
        				OutputStream os = new BufferedOutputStream(
        						socket.getOutputStream(), BUFSIZE);
        				while (true) {
        					os.write(stuff);
        				}
        			} catch (IOException e) {
        				e.printStackTrace();
        			}
        		}
        	}, "writer-" + i).start();
        }
    }

    public void logStatistics() throws Exception {
        while (true) {
            Thread.sleep(1000);
            System.out.println("" + 1.0 * byteCount.getAndSet(0) / 1024);
        }
    }

    public static void main(String[] args) throws Exception {
    	StandardIO_TcpTester tester = new StandardIO_TcpTester();
        InetSocketAddress address = tester.startNetworkDataConsumer();
        tester.startNetworkDataProducer(address);
        tester.logStatistics();
    }
    
}
