import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

public class Sanity {

static int groupPort = 9876;
static InetAddress groupAddress;
static {
    try {
        groupAddress = InetAddress.getByName("231.7.7.7");
    } catch (Throwable th) {
        throw new RuntimeException(th);
    }
}

public static class Server {
    public static void main(String[] args) throws Throwable {
        System.out.println("server started");
        byte msg[] = "123456789012345678901234567890".getBytes();
        DatagramPacket p = new DatagramPacket(msg, msg.length, groupAddress, groupPort);
        DatagramSocket s = new DatagramSocket();
        while (true) {
            s.send(p);
            Thread.sleep(100);
        }
    }
}

public static class Clients {
    static int SOCKET_TIMEOUT = 500;
    static int PROGRESS_SLEEP = 10_000; // 10 sec
    static int NUM_THREADS = 300;
    static int MAX_RETRIES = 32;

    public static void main(String[] args) throws Throwable {
        System.out.println("clients started");
        AtomicInteger[] progressCnt = new AtomicInteger[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; ++i) {
            AtomicInteger prgCnt = progressCnt[i] = new AtomicInteger(0);
            Thread t = new Thread(() -> {
                try {
                    byte[] data = new byte[65535];
                    DatagramPacket packet = new DatagramPacket(data, data.length);
                    while (true) {
                        MulticastSocket mc = null;
                        try {
                            mc = new MulticastSocket(new InetSocketAddress(groupAddress, groupPort));
                            mc.joinGroup(groupAddress);
                            mc.setSoTimeout(SOCKET_TIMEOUT);
                            for (int j = 0;;) {
                                try {
                                    mc.receive(packet);
                                    prgCnt.incrementAndGet();
                                    break;
                                } catch (InterruptedIOException e) {
                                    if (++j == MAX_RETRIES) {
                                        System.out.println("max retries " + j + " exceeded");
                                        break;
                                    }
                                }
                            }
                        } finally {
                            if (mc != null) mc.close();
                        }
                    }
                } catch (Throwable th) {
                    throw new RuntimeException(th);
                }
            });
            t.start();
        }
        for (int cycle = 0; ; ++cycle) {
//            System.out.println("cycle " + cycle);
            Thread.sleep(PROGRESS_SLEEP);
            for (int i = 0; i < NUM_THREADS; ++i) {
                if (progressCnt[i].getAndSet(0) == 0)
                    System.out.println("no progress in thread " + i);
            }
        }
    }
}

public static class NioClients {
    static int SOCKET_TIMEOUT = 500;
    static int PROGRESS_SLEEP = 10_000; // 10 sec
    static int NUM_THREADS = 300;
    static int MAX_RETRIES = 32;

    public static void main(String[] args) throws Throwable {
        System.out.println("nio clients started");
        AtomicInteger[] progressCnt = new AtomicInteger[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; ++i) {
            AtomicInteger prgCnt = progressCnt[i] = new AtomicInteger(0);
            Thread t = new Thread(() -> {
                try {
                    byte[] data = new byte[65535];
                    DatagramPacket packet = new DatagramPacket(data, data.length);
                    while (true) {
                        DatagramChannel dc = null;
                        DatagramSocket mc = null;
                        try {
                            dc = DatagramChannel.open(StandardProtocolFamily.INET)
                                .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                                .bind(new InetSocketAddress(groupAddress, groupPort));
                            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                            while (networkInterfaces.hasMoreElements()) {
                                NetworkInterface ni = networkInterfaces.nextElement();
                                if (ni.isUp() && ni.supportsMulticast()) {
                                    Enumeration<InetAddress> addrs = ni.getInetAddresses();
                                    while (addrs.hasMoreElements()) {
                                        InetAddress addr = addrs.nextElement();
                                        if (addr instanceof Inet4Address) {
                                            dc.join(groupAddress, ni);
                                            break;
                                        }
                                    }
                                }
                            }
                            mc = dc.socket();
                            mc.setSoTimeout(SOCKET_TIMEOUT);
                            for (int j = 0;;) {
                                try {
                                    mc.receive(packet);
                                    prgCnt.incrementAndGet();
                                    break;
                                } catch (InterruptedIOException e) {
                                    if (++j == MAX_RETRIES) {
                                        System.out.println("max retries " + j + " exceeded");
                                        break;
                                    }
                                }
                            }
                        } finally {
                            if (mc != null) mc.close();
                            if (dc != null) dc.close();
                        }
                    }
                } catch (Throwable th) {
                    throw new RuntimeException(th);
                }
            });
            t.start();
        }
        for (int cycle = 0; ; ++cycle) {
//            System.out.println("cycle " + cycle);
            Thread.sleep(PROGRESS_SLEEP);
            for (int i = 0; i < NUM_THREADS; ++i) {
                if (progressCnt[i].getAndSet(0) == 0)
                    System.out.println("no progress in thread " + i);
            }
        }
    }
}

}
