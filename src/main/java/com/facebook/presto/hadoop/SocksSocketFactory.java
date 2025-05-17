/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hadoop;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import javax.net.SocketFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.io.ByteStreams.readFully;
import static com.google.common.net.InetAddresses.toAddrString;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SOCKS_SERVER_KEY;

/**
 * SocketFactory to create sockets with SOCKS proxy.
 * The created sockets have a SocketChannel as required by the HDFS client.
 */
public class SocksSocketFactory
        extends SocketFactory
        implements Configurable
{
    private Configuration conf;
    private HostAndPort proxy;

    @Override
    public Configuration getConf()
    {
        return conf;
    }

    @Override
    public void setConf(Configuration conf)
    {
        this.conf = conf;
        String server = conf.get(HADOOP_SOCKS_SERVER_KEY);
        if (!isNullOrEmpty(server)) {
            proxy = HostAndPort.fromString(server);
        }
    }

    @Override
    public Socket createSocket()
            throws IOException
    {
        checkState(proxy != null, "proxy was not configured");
        return createSocksSocket(proxy);
    }

    @Override
    public Socket createSocket(InetAddress addr, int port)
    {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public Socket createSocket(InetAddress addr, int port, InetAddress localHostAddr, int localPort)
    {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public Socket createSocket(String host, int port)
    {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHostAddr, int localPort)
    {
        throw new UnsupportedOperationException("method not supported");
    }

    private static Socket createSocksSocket(HostAndPort proxy)
            throws IOException
    {
        return new ForwardingSocket(SocketChannel.open().socket())
        {
            @Override
            public SocketChannel getChannel()
            {
                // hack for NetUtils.connect()
                return isConnected() ? super.getChannel() : null;
            }

            @Override
            public void connect(SocketAddress endpoint, int timeout)
                    throws IOException
            {
                try {
                    SocketAddress address = new InetSocketAddress(InetAddress.getByName(proxy.getHost()), proxy.getPort());
                    socket.connect(address, timeout);
                }
                catch (IOException e) {
                    throw new IOException("Failed to connect to proxy: " + proxy, e);
                }

                InetSocketAddress address = (InetSocketAddress) endpoint;
                String host = (address.getAddress() != null) ? toAddrString(address.getAddress()) : address.getHostString();
                byte[] packet = createSocks4aPacket(host, address.getPort());
                socket.getOutputStream().write(packet);

                byte[] response = new byte[8];
                readFully(socket.getInputStream(), response);
                if (response[1] != 90) {
                    throw new IOException(format("Invalid response from SOCKS server: 0x%02X", response[1]));
                }
            }
        };
    }

    private static byte[] createSocks4aPacket(String hostname, int port)
    {
        ByteArrayDataOutput buffer = ByteStreams.newDataOutput();
        buffer.writeByte(0x04); // SOCKS version
        buffer.writeByte(0x01); // CONNECT
        buffer.writeShort(port); // port
        buffer.writeByte(0x00); // fake ip
        buffer.writeByte(0x00); // fake ip
        buffer.writeByte(0x00); // fake ip
        buffer.writeByte(0x01); // fake ip
        buffer.writeByte(0x00); // empty user (null terminated)
        buffer.write(hostname.getBytes(US_ASCII)); // hostname
        buffer.writeByte(0x00); // null terminate
        return buffer.toByteArray();
    }
}
