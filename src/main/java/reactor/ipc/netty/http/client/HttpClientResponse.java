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

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.Completable;
import io.reactivex.CompletableSource;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiFunction;
import org.reactivestreams.Publisher;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.NettyInbound;
import reactor.ipc.netty.http.HttpInfos;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

/**
 * An HttpClient Reactive read contract for incoming response. It inherits several
 * accessor
 * related to HTTP
 * flow : headers, params,
 * URI, method, websocket...
 *
 * @author Stephane Maldini
 * @since 0.5
 */
public interface HttpClientResponse extends NettyInbound, HttpInfos, NettyContext {

	@Override
	default HttpClientResponse addHandlerFirst(ChannelHandler handler) {
		NettyContext.super.addHandlerFirst(handler);
		return this;
	}

	@Override
	HttpClientResponse addHandlerFirst(String name, ChannelHandler handler);

	@Override
	default HttpClientResponse addHandlerLast(ChannelHandler handler) {
		return addHandlerLast(handler.getClass().getSimpleName(), handler);
	}

	@Override
	HttpClientResponse addHandlerLast(String name, ChannelHandler handler);

	@Override
	default HttpClientResponse addHandler(ChannelHandler handler) {
		return addHandler(handler.getClass().getSimpleName(), handler);
	}

	@Override
	HttpClientResponse addHandler(String name, ChannelHandler handler);

	@Override
	HttpClientResponse removeHandler(String name);

	@Override
	HttpClientResponse replaceHandler(String name, ChannelHandler handler);

	@Override
	HttpClientResponse onClose(Action onClose);

	@Override
	default HttpClientResponse onReadIdle(long idleTimeout, Runnable onReadIdle) {
		NettyInbound.super.onReadIdle(idleTimeout, onReadIdle);
		return this;
	}

	/**
	 * Return a {@link Flowable} of {@link HttpContent} containing received chunks
	 *
	 * @return a {@link Flowable} of {@link HttpContent} containing received chunks
	 */
	default Flowable<HttpContent> receiveContent(){
		return receiveObject().ofType(HttpContent.class);
	}

	/**
	 * Unidirectional conversion to a {@link WebsocketInbound}.
	 * receive operations are invoked on handshake success, otherwise connection wasn't
	 * upgraded by the server and the returned {@link WebsocketInbound} fails.
	 *
	 * @return a {@link WebsocketInbound} completing when upgrade is confirmed
	 */
	WebsocketInbound receiveWebsocket();

	/**
	 * Duplex conversion to {@link WebsocketInbound}, {@link WebsocketOutbound} and a
	 * closing {@link Publisher}. Mono and Callback are invoked on handshake success,
	 * otherwise the returned {@link Completable} fails.
	 *
	 * @param websocketHandler the in/out handler for ws transport
	 *
	 * @return a {@link Completable} completing when upgrade is confirmed
	 */
	default Completable receiveWebsocket(BiFunction<? super WebsocketInbound, ? super WebsocketOutbound, ? extends CompletableSource> websocketHandler) {
		return receiveWebsocket(null, websocketHandler);
	}

	/**
	 * Duplex conversion to {@link WebsocketInbound}, {@link WebsocketOutbound} and a
	 * closing {@link Publisher}. Mono and Callback are invoked on handshake success,
	 * otherwise the returned {@link Completable} fails.
	 *
	 * @param protocols optional sub-protocol
	 * @param websocketHandler the in/out handler for ws transport
	 *
	 * @return a {@link Completable} completing when upgrade is confirmed
	 */
	Completable receiveWebsocket(String protocols,
			BiFunction<? super WebsocketInbound, ? super WebsocketOutbound, ? extends CompletableSource> websocketHandler);

	/**
	 * Return the previous redirections or empty array
	 *
	 * @return the previous redirections or empty array
	 */
	String[] redirectedFrom();

	/**
	 * Return response HTTP headers.
	 *
	 * @return response HTTP headers.
	 */
	HttpHeaders responseHeaders();

	/**
	 * @return the resolved HTTP Response Status
	 */
	HttpResponseStatus status();
}
