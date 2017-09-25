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

package reactor.ipc.netty.http.client;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

import hu.akarnokd.rxjava2.basetypes.Perhaps;
import hu.akarnokd.rxjava2.functions.PlainBiFunction;
import hu.akarnokd.rxjava2.functions.PlainConsumer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.logging.LoggingHandler;

import io.reactivex.MaybeEmitter;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import org.reactivestreams.Publisher;
import reactor.ipc.netty.NettyConnector;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.NettyInbound;
import reactor.ipc.netty.NettyOutbound;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.channel.ContextHandler;
import reactor.ipc.netty.http.HttpResources;
import reactor.ipc.netty.http.server.HttpServerResponse;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;
import reactor.ipc.netty.options.ClientOptions;
import reactor.ipc.netty.tcp.TcpClient;

/**
 * The base class for a Netty-based Http client.
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 * @author Violeta Georgieva
 */
public class HttpClient implements NettyConnector<HttpClientResponse, HttpClientRequest> {

	public static final String USER_AGENT = String.format("ReactorNetty/%s", reactorNettyVersion());


	/**
	 * Creates a simple HTTP Client with no address to which this client should connect
	 * and with default sslContext support.
	 *
	 * @return a simple HTTP client
	 */
	public static HttpClient create() {
		return create(HttpClientOptions.Builder::sslSupport);
	}

	/**
	 * Creates a simple HTTP client using provided {@link HttpClientOptions options}
	 *
	 * @param options the options for the client, including the address to which this
	 * client should connect
	 * @return a simple HTTP client using provided {@link HttpClientOptions options}
	 */
	public static HttpClient create(PlainConsumer<? super HttpClientOptions.Builder> options) {
		return builder().options(options).build();
	}

	/**
	 * Creates a simple HTTP client bound on the provided address and port 80
	 *
	 * @param address the host to which this client should connect.
	 * @return a simple HTTP client bound on the provided address and port 80
	 */
	public static HttpClient create(String address) {
		return create(address, 80);
	}

	/**
	 * Creates a simple HTTP client bound on the provided address and port
	 *
	 * @param address the host to which this client should connect.
	 * @param port the port to which this client should connect.
	 * @return a simple HTTP client bound on the provided address and port
	 */
	public static HttpClient create(String address, int port) {
		return create(opts -> opts.connectAddress(() -> InetSocketAddress.createUnresolved(address, port)));
	}

	/**
	 * Creates a simple HTTP client bound on localhost and the provided port
	 *
	 * @param port the port to which this client should connect.
	 * @return a simple HTTP client bound on localhost and the provided port
	 */
	public static HttpClient create(int port) {
		return create("localhost", port);
	}

	/**
	 * Creates a builder for {@link HttpClient HttpClient}
	 *
	 * @return a new HttpClient builder
	 */
	public static HttpClient.Builder builder() {
		return new HttpClient.Builder();
	}

	final TcpBridgeClient client;
	final HttpClientOptions options;

	private HttpClient(HttpClient.Builder builder) {
		HttpClientOptions.Builder clientOptionsBuilder = HttpClientOptions.builder();
		clientOptionsBuilder.loopResources(HttpResources.get())
		                    .poolResources(HttpResources.get());
		if (Objects.nonNull(builder.options)) {
			builder.options.accept(clientOptionsBuilder);
		}
		this.options = clientOptionsBuilder.build();
		this.client = new TcpBridgeClient(options);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been established, the passed handler is
	 * invoked and can be used to tune the request and write data to it.
	 *
	 * @param url the target remote URL
	 * @param handler the {@link Function} to invoke on open channel
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> delete(String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.DELETE, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL.
	 *
	 * @param url the target remote URL
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> delete(String url) {
		return request(HttpMethod.DELETE, url, null);
	}

	/**
	 * HTTP GET the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to tune the request and write data to it.
	 *
	 * @param url the target remote URL
	 * @param handler the {@link Function} to invoke on open channel
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> get(String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.GET, url, handler);
	}

	/**
	 * HTTP GET the passed URL.
	 *
	 * @param url the target remote URL
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> get(String url) {
		return request(HttpMethod.GET, url, null);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Perhaps<HttpClientResponse> newHandler(PlainBiFunction<? super HttpClientResponse, ? super HttpClientRequest, ? extends Publisher<Void>> ioHandler) {
		return (Perhaps<HttpClientResponse>) client.newHandler((PlainBiFunction<NettyInbound, NettyOutbound, Publisher<Void>>) ioHandler);
	}

	/**
	 * HTTP PATCH the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to tune the request and write data to it.
	 *
	 * @param url the target remote URL
	 * @param handler the {@link Function} to invoke on open channel
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> patch(String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.PATCH, url, handler);
	}

	/**
	 * HTTP PATCH the passed URL.
	 *
	 * @param url the target remote URL
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> patch(String url) {
		return request(HttpMethod.PATCH, url, null);
	}

	/**
	 * HTTP POST the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to tune the request and write data to it.
	 *
	 * @param url the target remote URL
	 * @param handler the {@link Function} to invoke on open channel
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> post(String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.POST, url, handler);
	}

	/**
	 * HTTP PUT the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to tune the request and write data to it.
	 *
	 * @param url the target remote URL
	 * @param handler the {@link Function} to invoke on open channel
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> put(String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.PUT, url, handler);
	}

	/**
	 * Use the passed HTTP method to send to the given URL. When connection has been made,
	 * the passed handler is invoked and can be used to tune the request and
	 * write data to it.
	 *
	 * @param method the HTTP method to send
	 * @param url the target remote URL
	 * @param handler the {@link Function} to invoke on opened TCP connection
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public Perhaps<HttpClientResponse> request(HttpMethod method,
			String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {

		if (method == null || url == null) {
			throw new IllegalArgumentException("Method && url cannot be both null");
		}

		return new PerhapsHttpClientResponse(this, url, method, handler(handler, options));
	}

	/**
	 * WebSocket to the passed URL.
	 *
	 * @param url the target remote URL
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> ws(String url) {
		return request(WS, url, HttpClientRequest::sendWebsocket);
	}

	/**
	 * WebSocket to the passed URL.
	 *
	 * @param url the target remote URL
	 * @param headerBuilder the  header {@link Consumer} to invoke before sending websocket
	 * handshake
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> ws(String url,
			final Consumer<? super HttpHeaders> headerBuilder) {
		return request(WS,
				url, ch -> {
					headerBuilder.accept(ch.requestHeaders());
					return ch.sendWebsocket();
				});
	}
	/**
	 * WebSocket to the passed URL, negotiating one of the passed subprotocols.
	 * <p>
	 * The negotiated subprotocol can be accessed through the {@link HttpClientResponse}
	 * by switching to websocket (using any of the {@link HttpClientResponse#receiveWebsocket() receiveWebSocket}
	 * methods) and using {@link WebsocketInbound#selectedSubprotocol()}.
	 * <p>
	 * To send data through the websocket, use {@link HttpClientResponse#receiveWebsocket(PlainBiFunction)}
	 * and then use the function's {@link WebsocketOutbound}.
	 *
	 * @param url the target remote URL
	 * @param subprotocols the subprotocol(s) to negotiate, comma-separated, or null if not relevant.
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> ws(String url, String subprotocols) {
		return request(WS, url, req -> req.sendWebsocket(subprotocols));
	}

	/**
	 * WebSocket to the passed URL, negotiating one of the passed subprotocols.
	 * <p>
	 * The negotiated subprotocol can be accessed through the {@link HttpClientResponse}
	 * by switching to websocket (using any of the {@link HttpClientResponse#receiveWebsocket() receiveWebSocket}
	 * methods) and using {@link WebsocketInbound#selectedSubprotocol()}.
	 * <p>
	 * To send data through the websocket, use {@link HttpClientResponse#receiveWebsocket(PlainBiFunction)}
	 * and then use the function's {@link WebsocketOutbound}.
	 *
	 * @param url the target remote URL
	 * @param headerBuilder the  header {@link Consumer} to invoke before sending websocket
	 * handshake
	 * @param subprotocols the subprotocol(s) to negotiate, comma-separated, or null if not relevant.
	 * @return a {@link Perhaps} of the {@link HttpServerResponse} ready to consume for
	 * response
	 */
	public final Perhaps<HttpClientResponse> ws(String url,
			final Consumer<? super HttpHeaders> headerBuilder, String subprotocols) {
		return request(WS,
				url, ch -> {
					headerBuilder.accept(ch.requestHeaders());
					return ch.sendWebsocket(subprotocols);
				});
	}

	/**
	 * Get a copy of the {@link HttpClientOptions} currently in effect.
	 *
	 * @return the http client options
	 */
	public HttpClientOptions options() {
		return options.duplicate();
	}

	@Override
	public String toString() {
		return "HttpClient: " + options.asSimpleString();
	}

	static final HttpMethod     WS             = new HttpMethod("WS");
	final static String         WS_SCHEME      = "ws";
	final static String         WSS_SCHEME     = "wss";
	final static String         HTTP_SCHEME    = "http";
	final static String         HTTPS_SCHEME   = "https";
	final static LoggingHandler loggingHandler = new LoggingHandler(HttpClient.class);

	@SuppressWarnings("unchecked")
	final class TcpBridgeClient extends TcpClient implements
	                                              BiConsumer<ChannelPipeline, ContextHandler<Channel>> {

		TcpBridgeClient(ClientOptions options) {
			super(options);
		}

		@Override
		protected Perhaps<NettyContext> newHandler(PlainBiFunction<? super NettyInbound, ? super NettyOutbound, ? extends Publisher<Void>> handler,
				InetSocketAddress address,
				boolean secure,
				PlainConsumer<? super Channel> onSetup) {
			return super.newHandler(handler, address, secure, onSetup);
		}

		@Override
		protected ContextHandler<SocketChannel> doHandler(PlainBiFunction<? super NettyInbound, ? super NettyOutbound, ? extends Publisher<Void>> handler,
				MaybeEmitter<NettyContext> sink,
				boolean secure,
				SocketAddress providedAddress,
				ChannelPool pool,
				PlainConsumer<? super Channel> onSetup) {
			return ContextHandler.<SocketChannel>newClientContext(sink,
					options,
					loggingHandler,
					secure,
					providedAddress,
					pool,
					handler != null ? (ch, c, msg) -> {
						if(onSetup != null){
							onSetup.accept(ch);
						}
						return HttpClientOperations.bindHttp(ch, handler, c);
					} : EMPTY).onPipeline(this);
		}

		@Override
		public void accept(ChannelPipeline pipeline, ContextHandler<Channel> c) {
			pipeline.addLast(NettyPipeline.HttpDecoder, new HttpResponseDecoder())
			        .addLast(NettyPipeline.HttpEncoder, new HttpRequestEncoder());
			if (options.acceptGzip()) {
				pipeline.addAfter(NettyPipeline.HttpDecoder,
						NettyPipeline.HttpDecompressor,
						new HttpContentDecompressor());
			}
		}
	}

	static String reactorNettyVersion() {
		String reactorNettyVersion = HttpClient.class.getPackage().getImplementationVersion();
		if (reactorNettyVersion == null) {
			return "dev";
		}
		return reactorNettyVersion;
	}

	static Function<? super HttpClientRequest, ? extends Publisher<Void>> handler(Function<? super HttpClientRequest, ? extends Publisher<Void>> h,
			HttpClientOptions opts) {
		if (opts.acceptGzip()) {
			if (h != null) {
				return req -> h.apply(req.header(HttpHeaderNames.ACCEPT_ENCODING,
						HttpHeaderValues.GZIP));
			}
			else {
				return req -> req.header(HttpHeaderNames.ACCEPT_ENCODING,
						HttpHeaderValues.GZIP);
			}
		}
		else {
			return h;
		}
	}

	public static final class Builder {
		private PlainConsumer<? super HttpClientOptions.Builder> options;

		private Builder() {
		}


		/**
		 * The options for the client, including address and port.
		 *
		 * @param options the options for the client, including address and port.
		 * @return {@code this}
		 */
		public final Builder options(PlainConsumer<? super HttpClientOptions.Builder> options) {
			this.options = Objects.requireNonNull(options, "options");
			return this;
		}

		public HttpClient build() {
			return new HttpClient(this);
		}
	}
}
