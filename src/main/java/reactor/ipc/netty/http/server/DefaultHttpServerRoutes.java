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

package reactor.ipc.netty.http.server;

import hu.akarnokd.rxjava2.functions.PlainBiFunction;
import hu.akarnokd.rxjava2.functions.PlainConsumer;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import hu.akarnokd.rxjava2.basetypes.Nono;
import hu.akarnokd.rxjava2.functions.PlainFunction;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import org.reactivestreams.Publisher;

/**
 * @author Stephane Maldini
 */
final class DefaultHttpServerRoutes implements HttpServerRoutes {


	private final CopyOnWriteArrayList<HttpRouteHandler> handlers =
			new CopyOnWriteArrayList<>();

	@Override
	public HttpServerRoutes directory(String uri, Path directory,
			PlainFunction<HttpServerResponse, HttpServerResponse> interceptor) {
		Objects.requireNonNull(directory, "directory");
		return route(HttpPredicate.prefix(uri), (req, resp) -> {

			String prefix = URI.create(req.uri())
			                   .getPath()
			                   .replaceFirst(uri, "");

			if(prefix.charAt(0) == '/'){
				prefix = prefix.substring(1);
			}

			Path p = directory.resolve(prefix);
			if (Files.isReadable(p)) {

				if (interceptor != null) {
					return interceptor.apply(resp)
					                  .sendFile(p);
				}
				return resp.sendFile(p);
			}

			return resp.sendNotFound();
		});
	}

	@Override
	public HttpServerRoutes route(Predicate<? super HttpServerRequest> condition,
			PlainBiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> handler) {
		Objects.requireNonNull(condition, "condition");
		Objects.requireNonNull(handler, "handler");

		if (condition instanceof HttpPredicate) {
			handlers.add(new HttpRouteHandler(condition,
					handler,
					(HttpPredicate) condition));
		}
		else {
			handlers.add(new HttpRouteHandler(condition, handler, null));
		}
		return this;
	}

	@Override
	public Publisher<Void> apply(HttpServerRequest request, HttpServerResponse response) {
		final Iterator<HttpRouteHandler> iterator = handlers.iterator();
		HttpRouteHandler cursor;

		try {
			while (iterator.hasNext()) {
				cursor = iterator.next();
				if (cursor.test(request)) {
					return cursor.apply(request, response);
				}
			}
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			return Nono.error(t); //500
		}

		return response.sendNotFound();
	}

	/**
	 */
	static final class HttpRouteHandler
			implements PlainBiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>>,
			           Predicate<HttpServerRequest> {

		final Predicate<? super HttpServerRequest>          condition;
		final PlainBiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>>
		                                                    handler;
		final PlainFunction<? super String, Map<String, String>> resolver;

		HttpRouteHandler(Predicate<? super HttpServerRequest> condition,
				PlainBiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> handler,
				PlainFunction<? super String, Map<String, String>> resolver) {
			this.condition = Objects.requireNonNull(condition, "condition");
			this.handler = Objects.requireNonNull(handler, "handler");
			this.resolver = resolver;
		}

		@Override
		public Publisher<Void> apply(HttpServerRequest request,
				HttpServerResponse response) {
			return handler.apply(request.paramsResolver(resolver), response);
		}

		@Override
		public boolean test(HttpServerRequest o) throws Exception {
			return condition.test(o);
		}
	}

}
