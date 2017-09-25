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

package reactor.ipc.netty.channel;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import hu.akarnokd.rxjava2.basetypes.Nono;
import hu.akarnokd.rxjava2.basetypes.NonoProcessor;
import hu.akarnokd.rxjava2.functions.PlainBiFunction;
import hu.akarnokd.rxjava2.functions.PlainConsumer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.ipc.netty.NettyConnector;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.NettyInbound;
import reactor.ipc.netty.NettyOutbound;
import reactor.ipc.netty.NettyPipeline;

/**
 * A bridge between an immutable {@link Channel} and {@link NettyInbound} /
 * {@link NettyOutbound} semantics exposed to user
 * {@link NettyConnector#newHandler(PlainBiFunction)}
 *
 * @author Stephane Maldini
 * @since 0.6
 */
public class ChannelOperations<INBOUND extends NettyInbound, OUTBOUND extends NettyOutbound>
	  extends AtomicReference<Subscription>
		implements NettyInbound, NettyOutbound, NettyContext, Subscriber<Void> {

	/**
	 * Create a new {@link ChannelOperations} attached to the {@link Channel} attribute
	 * {@link #OPERATIONS_KEY}.
	 * Attach the {@link NettyPipeline#ReactiveBridge} handle.
	 *
	 * @param channel the new {@link Channel} connection
	 * @param handler the user-provided {@link PlainBiFunction} i/o handler
	 * @param context the dispose callback
	 * @param <INBOUND> the {@link NettyInbound} type
	 * @param <OUTBOUND> the {@link NettyOutbound} type
	 *
	 * @return the created {@link ChannelOperations} bridge
	 */
	public static <INBOUND extends NettyInbound, OUTBOUND extends NettyOutbound> ChannelOperations<INBOUND, OUTBOUND> bind(
			Channel channel,
			PlainBiFunction<? super INBOUND, ? super OUTBOUND, ? extends Publisher<Void>> handler,
			ContextHandler<?> context) {
		@SuppressWarnings("unchecked") ChannelOperations<INBOUND, OUTBOUND> ops =
				new ChannelOperations<>(channel, handler, context);

		return ops;
	}

	/**
	 * Return a Noop {@link PlainBiFunction} handler
	 *
	 * @param <INBOUND> reified inbound type
	 * @param <OUTBOUND> reified outbound type
	 *
	 * @return a Noop {@link PlainBiFunction} handler
	 */
	@SuppressWarnings("unchecked")
	public static <INBOUND extends NettyInbound, OUTBOUND extends NettyOutbound> PlainBiFunction<? super INBOUND, ? super OUTBOUND, ? extends Publisher<Void>> noopHandler() {
		return PING;
	}

	/**
	 * Return the current {@link Channel} bound
	 * {@link ChannelOperations} or null if none
	 *
	 * @param ch the current {@link Channel}
	 *
	 * @return the current {@link Channel} bound
	 * {@link ChannelOperations} or null if none
	 */
	public static ChannelOperations<?, ?> get(Channel ch) {
		return ch.attr(OPERATIONS_KEY)
		          .get();
	}

	static ChannelOperations<?, ?> tryGetAndSet(Channel ch, ChannelOperations<?, ?> ops) {
		Attribute<ChannelOperations> attr = ch.attr(ChannelOperations.OPERATIONS_KEY);
		for (; ; ) {
			ChannelOperations<?, ?> op = attr.get();
			if (op != null) {
				return op;
			}

			if (attr.compareAndSet(null, ops)) {
				return null;
			}
		}
	}

	final PlainBiFunction<? super INBOUND, ? super OUTBOUND, ? extends Publisher<Void>>
			                    handler;
	final Channel               channel;
	final FlowableReceive inbound;
	final NonoProcessor         onInactive;
	final ContextHandler<?>     context;
	@SuppressWarnings("unchecked")
	volatile Subscription outboundSubscription;
	protected ChannelOperations(Channel channel,
			ChannelOperations<INBOUND, OUTBOUND> replaced) {
		this(channel, replaced.handler, replaced.context, replaced.onInactive);
	}

	protected ChannelOperations(Channel channel,
			PlainBiFunction<? super INBOUND, ? super OUTBOUND, ? extends Publisher<Void>> handler,
			ContextHandler<?> context) {
		this(channel, handler, context, NonoProcessor.create());
	}

	protected ChannelOperations(Channel channel,
			PlainBiFunction<? super INBOUND, ? super OUTBOUND, ? extends Publisher<Void>> handler,
			ContextHandler<?> context, NonoProcessor processor) {
		this.handler = Objects.requireNonNull(handler, "handler");
		this.channel = Objects.requireNonNull(channel, "channel");
		this.context = Objects.requireNonNull(context, "context");
		this.inbound = new FlowableReceive(this);
		this.onInactive = processor;
		Nono.fromPublisher(context.onCloseOrRelease(channel))
		    .subscribe(onInactive);
	}

	@Override
	public InetSocketAddress address() {
		Channel c = channel();
		if (c instanceof SocketChannel) {
			return ((SocketChannel) c).remoteAddress();
		}
		if (c instanceof DatagramChannel) {
			return ((DatagramChannel) c).localAddress();
		}
		throw new IllegalStateException("Does not have an InetSocketAddress");
	}

	@Override
	public final Channel channel() {
		return channel;
	}

	@Override
	public final NettyContext context() {
		return this;
	}

	@Override
	public ChannelOperations<INBOUND, OUTBOUND> context(PlainConsumer<NettyContext> contextCallback) {
		contextCallback.accept(context());
		return this;
	}

	@Override
	public void dispose() {
		inbound.cancel();
		channel.close();
	}

	@Override
	public final boolean isDisposed() {
		return get(channel()) != this;
	}

	@Override
	public final Nono onClose() {
		return Nono.fromPublisher(onInactive);
	}

	@Override
	public NettyContext onClose(final Action onClose) {
		onInactive.subscribe(onClose, e -> onClose.run());
		return this;
	}

	@Override
	public final void onComplete() {
		if (!SubscriptionHelper.cancel(this) || isDisposed()) {
			return;
		}
		onOutboundComplete();
	}

	@Override
	public final void onError(Throwable t) {
		if (!SubscriptionHelper.cancel(this) || isDisposed()) {
			return;
		}
		onOutboundError(t);
	}

	@Override
	public final void onNext(Void aVoid) {
	}

	@Override
	public final void onSubscribe(Subscription s) {
		if (SubscriptionHelper.setOnce(this, s)) {
			s.request(Long.MAX_VALUE);
		}
	}

	@Override
	public Flowable<?> receiveObject() {
		return inbound;
	}

	@Override
	public final InetSocketAddress remoteAddress() {
		return (InetSocketAddress) channel.remoteAddress();
	}

	@Override
	public String toString() {
		return channel.toString();
	}

	/**
	 * Return true if inbound traffic is not expected anymore
	 *
	 * @return true if inbound traffic is not expected anymore
	 */
	protected final boolean isInboundDone() {
		return inbound.inboundDone || !channel.isActive();
	}

	/**
	 * Return true if inbound traffic is not expected anymore
	 *
	 * @return true if inbound traffic is not expected anymore
	 */
	protected final boolean isInboundCancelled() {
		return inbound.isCancelled() || !channel.isActive();
	}


	/**
	 * Return true if inbound traffic is not expected anymore
	 *
	 * @return true if inbound traffic is not expected anymore
	 */
	protected final boolean isOutboundDone() {
		return SubscriptionHelper.isCancelled(outboundSubscription) || !channel.isActive();
	}

	protected boolean shouldEmitEmptyContext() {
		return false;
	}

	/**
	 * Connector handler provided by user
	 *
	 * @return Connector handler provided by user
	 */
	protected final PlainBiFunction<? super INBOUND, ? super OUTBOUND, ? extends Publisher<Void>> handler() {
		return handler;
	}

	/**
	 * React on input initialization
	 *
	 */
	@SuppressWarnings("unchecked")
	protected void onHandlerStart() {
		applyHandler();
		context.fireContextActive(this);
	}

	/**
	 * React on inbound {@link Channel#read}
	 *
	 * @param ctx the context
	 * @param msg the read payload
	 */
	protected void onInboundNext(ChannelHandlerContext ctx, Object msg) {
		if (msg == null) {
			onInboundError(new NullPointerException("msg is null"));
			return;
		}
		inbound.onInboundNext(msg);
	}

	/**
	 * Replace and complete previous operation inbound
	 *
	 * @param ops a new operations
	 *
	 * @return true if replaced
	 */
	protected final boolean replace(ChannelOperations<?, ?> ops) {
		return channel.attr(OPERATIONS_KEY)
		              .compareAndSet(this, ops);
	}

	/**
	 * React on inbound cancel (receive() subscriber cancelled)
	 */
	protected void onInboundCancel() {

	}


	/**
	 * React on inbound completion (last packet)
	 */
	protected void onInboundComplete() {
		if (inbound.onInboundComplete()) {
			context.fireContextActive(this);
		}
	}

	/**
	 * React on inbound/outbound completion (last packet)
	 */
	protected void onOutboundComplete() {
		markPersistent(false);
		onHandlerTerminate();
	}

	/**
	 * React on inbound/outbound error
	 *
	 * @param err the {@link Throwable} cause
	 */
	protected void onOutboundError(Throwable err) {
		discreteRemoteClose(err);
		markPersistent(false);
		onHandlerTerminate();
	}

	/**
	 * Apply the user-provided {@link NettyConnector} handler
	 */
	@SuppressWarnings("unchecked")
	protected final void applyHandler() {
//		channel.pipeline()
//		       .fireUserEventTriggered(NettyPipeline.handlerStartedEvent());
		Nono.fromPublisher(handler.apply((INBOUND) this, (OUTBOUND) this))
		       .subscribe(this);
	}

	/**
	 * Try filtering out remote close unless traced, return true if filtered
	 *
	 * @param err the error to check
	 *
	 * @return true if filtered
	 */
	protected final boolean discreteRemoteClose(Throwable err) {
		if (AbortedException.isConnectionReset(err)) {
			return true;
		}

		return false;
	}

	/**
	 * Final release/close (last packet)
	 */
	protected final void onHandlerTerminate() {
		if (replace(null)) {
			try {
				SubscriptionHelper.cancel(this);
				onInactive.onComplete(); //signal senders and other interests
				onInboundComplete(); // signal receiver

			}
			finally {
				channel.pipeline()
				       .fireUserEventTriggered(NettyPipeline.handlerTerminatedEvent());
			}
		}
	}

	/**
	 * React on inbound error
	 *
	 * @param err the {@link Throwable} cause
	 */
	protected final void onInboundError(Throwable err) {
		discreteRemoteClose(err);
		if (inbound.onInboundError(err)) {
			context.fireContextError(err);
		}
	}

	/**
	 * Return the available parent {@link ContextHandler} for user-facing lifecycle
	 * handling
	 *
	 * @return the available parent {@link ContextHandler}for user-facing lifecycle
	 * handling
	 */
	protected final ContextHandler<?> parentContext() {
		return context;
	}

	/**
	 * Return formatted name of this operation
	 *
	 * @return formatted name of this operation
	 */
	protected final String formatName() {
		return getClass().getSimpleName()
		                 .replace("Operations", "");
	}

	/**
	 * A {@link ChannelOperations} factory
	 */
	@FunctionalInterface
	public interface OnNew<CHANNEL extends Channel> {

		/**
		 * Create a new {@link ChannelOperations} given a netty channel, a parent
		 * {@link ContextHandler} and an optional message (nullable).
		 *
		 * @param c a {@link Channel}
		 * @param contextHandler a {@link ContextHandler}
		 * @param msg an optional message
		 *
		 * @return a new {@link ChannelOperations}
		 */
		ChannelOperations<?, ?> create(CHANNEL c, ContextHandler<?> contextHandler, Object msg);
	}
	/**
	 * The attribute in {@link Channel} to store the current {@link ChannelOperations}
	 */
	protected static final AttributeKey<ChannelOperations> OPERATIONS_KEY = AttributeKey.newInstance("nettyOperations");
	static final PlainBiFunction PING = (i, o) -> Flowable.empty();
}