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
package reactor.ipc.netty.tcp;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import hu.akarnokd.rxjava2.basetypes.Nono;
import hu.akarnokd.rxjava2.functions.Supplier;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.reactivex.functions.Consumer;
import org.junit.Before;
import org.junit.Test;
import reactor.ipc.netty.resources.LoopResources;
import reactor.ipc.netty.resources.PoolResources;

import static org.assertj.core.api.Assertions.assertThat;

public class TcpResourcesTest {

	private AtomicBoolean loopDisposed;
	private AtomicBoolean poolDisposed;
	private LoopResources loopResources;
	private PoolResources poolResources;
	private TcpResources tcpResources;

	@Before
	public void before() {
		loopDisposed = new AtomicBoolean();
		poolDisposed = new AtomicBoolean();

		loopResources = new LoopResources() {
			@Override
			public EventLoopGroup onServer(boolean useNative) {
				return null;
			}

			@Override
			public Nono disposeLater() {
				return Nono.complete().doOnComplete(() -> loopDisposed.set(true));
			}

			@Override
			public boolean isDisposed() {
				return loopDisposed.get();
			}
		};

		poolResources = new PoolResources() {
			@Override
			public ChannelPool selectOrCreate(SocketAddress address,
																				Supplier<? extends Bootstrap> bootstrap,
																				Consumer<? super Channel> onChannelCreate, EventLoopGroup group) {
				return null;
			}

			public Nono disposeLater() {
				return Nono.complete().doOnComplete(() -> poolDisposed.set(true));
			}

			@Override
			public boolean isDisposed() {
				return poolDisposed.get();
			}
		};

		tcpResources = new TcpResources(loopResources, poolResources);
	}

	@Test
	public void disposeLaterDefers() {
		assertThat(tcpResources.isDisposed()).isFalse();

		tcpResources.disposeLater();
		assertThat(tcpResources.isDisposed()).isFalse();

		tcpResources.disposeLater()
		            .doOnComplete(() -> assertThat(tcpResources.isDisposed()).isTrue())
		            .subscribe();
		//not immediately disposed when subscribing
		assertThat(tcpResources.isDisposed()).as("immediate status on disposeLater subscribe").isFalse();
	}

	@Test
	public void shutdownLaterDefers() {
		TcpResources oldTcpResources = TcpResources.tcpResources.getAndSet(tcpResources);
		TcpResources newTcpResources = TcpResources.tcpResources.get();

		try {
			assertThat(newTcpResources).isSameAs(tcpResources);

			TcpResources.shutdownLater();
			assertThat(newTcpResources.isDisposed()).isFalse();

			TcpResources.shutdownLater().blockingAwait();
			assertThat(newTcpResources.isDisposed()).as("shutdownLater completion").isTrue();

			assertThat(TcpResources.tcpResources.get()).isNull();
		}
		finally {
			if (oldTcpResources != null && !TcpResources.tcpResources.compareAndSet(null, oldTcpResources)) {
				oldTcpResources.dispose();
			}
		}
	}

}