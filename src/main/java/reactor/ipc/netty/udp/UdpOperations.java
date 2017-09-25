/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.ipc.netty.udp;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;

import hu.akarnokd.rxjava2.basetypes.Nono;
import hu.akarnokd.rxjava2.functions.PlainBiFunction;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.DatagramChannel;
import org.reactivestreams.Publisher;
import reactor.ipc.netty.FutureNono;
import reactor.ipc.netty.channel.ChannelOperations;
import reactor.ipc.netty.channel.ContextHandler;

/**
 * @author Stephane Maldini
 */
final class UdpOperations extends ChannelOperations<UdpInbound, UdpOutbound>
		implements UdpInbound, UdpOutbound {

	static UdpOperations bind(DatagramChannel channel,
			PlainBiFunction<? super UdpInbound, ? super UdpOutbound, ? extends Publisher<Void>> handler,
			ContextHandler<?> context) {
		return new UdpOperations(channel, handler, context);
	}

	final DatagramChannel  datagramChannel;

	UdpOperations(DatagramChannel channel,
			PlainBiFunction<? super UdpInbound, ? super UdpOutbound, ? extends Publisher<Void>> handler,
			ContextHandler<?> context) {
		super(channel, handler, context);
		this.datagramChannel = channel;
	}

	/**
	 * Join a multicast group.
	 *
	 * @param multicastAddress multicast address of the group to join
	 *
	 * @return a {@link Publisher} that will be complete when the group has been joined
	 */
	public Nono join(final InetAddress multicastAddress, NetworkInterface iface) {
		if (null == iface && null != datagramChannel.config().getNetworkInterface()) {
			iface = datagramChannel.config().getNetworkInterface();
		}

		final ChannelFuture future;
		if (null != iface) {
			future = datagramChannel.joinGroup(new InetSocketAddress(multicastAddress,
					datagramChannel.localAddress()
					               .getPort()), iface);
		}
		else {
			future = datagramChannel.joinGroup(multicastAddress);
		}

		return FutureNono.from(future);
	}

	/**
	 * Leave a multicast group.
	 *
	 * @param multicastAddress multicast address of the group to leave
	 *
	 * @return a {@link Publisher} that will be complete when the group has been left
	 */
	public Nono leave(final InetAddress multicastAddress, NetworkInterface iface) {
		if (null == iface && null != datagramChannel.config().getNetworkInterface()) {
			iface = datagramChannel.config().getNetworkInterface();
		}

		final ChannelFuture future;
		if (null != iface) {
			future = datagramChannel.leaveGroup(new InetSocketAddress(multicastAddress,
					datagramChannel.localAddress()
					               .getPort()), iface);
		}
		else {
			future = datagramChannel.leaveGroup(multicastAddress);
		}

		return FutureNono.from(future);
	}

}
