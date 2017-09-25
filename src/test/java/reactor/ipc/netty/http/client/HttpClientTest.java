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

import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;

import hu.akarnokd.rxjava2.basetypes.Nono;
import hu.akarnokd.rxjava2.basetypes.NonoProcessor;
import hu.akarnokd.rxjava2.basetypes.Perhaps;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.CharsetUtil;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import reactor.ipc.netty.FutureNono;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.channel.AbortedException;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.options.ClientProxyOptions.Proxy;
import reactor.ipc.netty.resources.PoolResources;
import reactor.ipc.netty.tcp.TcpServer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 * @since 0.6
 */
public class HttpClientTest {

	@Test
	public void abort() throws Exception {
		NettyContext x = TcpServer.create("localhost", 0)
		                          .newHandler((in, out) -> in.receive()
		                                                     .take(1)
																	                       .ignoreElements()
		                                                     .andThen(Flowable.defer(() ->
						                                                     out.context(c ->
								                                                     c.addHandlerFirst(new HttpResponseEncoder()))
						                                                        .sendObject(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.ACCEPTED))
						                                                        .then(Nono.timer(2, TimeUnit.SECONDS)))
		                                                     )
		                          )
		                          .blockingGet(30, TimeUnit.SECONDS);

		PoolResources pool = PoolResources.fixed("test", 1);

		HttpClient.create(opts -> opts.host("localhost")
		                              .port(x.address().getPort())
		                              .poolResources(pool))
		                    .get("/")
		                    .flatMap(r -> Perhaps.just(r.status()
		                                          .code()))
		                    .blockingGet(30, TimeUnit.SECONDS);

		HttpClient.create(opts -> opts.host("localhost")
		                              .port(x.address().getPort())
		                              .poolResources(pool))
		          .get("/")
		          .blockingGet(30, TimeUnit.SECONDS);

		HttpClient.create(opts -> opts.host("localhost")
		                              .port(x.address().getPort())
		                              .poolResources(pool))
		          .get("/")
		          .blockingGet(30, TimeUnit.SECONDS);

	}

	DefaultFullHttpResponse response() {
		DefaultFullHttpResponse r = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
				HttpResponseStatus.ACCEPTED);
		r.headers()
		 .set(HttpHeaderNames.CONTENT_LENGTH, 0);
		return r;
	}

	@Test
	@Ignore
	public void pipelined() throws Exception {
		NettyContext x = TcpServer.create("localhost", 0)
		                          .newHandler((in, out) -> out.context(c -> c.addHandlerFirst(new
				                          HttpResponseEncoder()))
		                                                      .sendObject(Flowable.just(
				                                                      response(),
				                                                      response()))
		                                                      .neverComplete())
		                          .blockingGet(30, TimeUnit.SECONDS);

		PoolResources pool = PoolResources.fixed("test", 1);

		HttpClient.create(opts -> opts.host("localhost")
		                              .port(x.address().getPort())
		                              .poolResources(pool))
		                    .get("/")
		                    .flatMap(r -> Perhaps.just(r.status()
		                                          .code()))
		                    .blockingGet(30, TimeUnit.SECONDS);

		try {
			HttpClient.create(opts -> opts.host("localhost")
			                              .port(x.address().getPort())
			                              .poolResources(pool))
			          .get("/")
			          .blockingGet(30, TimeUnit.SECONDS);
		}
		catch (AbortedException ae) {
			return;
		}

		Assert.fail("Not aborted");
	}

	@Test
	public void backpressured() throws Exception {
		Path resource = Paths.get(getClass().getResource("/public").toURI());
		NettyContext c = HttpServer.create(0)
		                           .newRouter(routes -> routes.directory("/test", resource))
		                           .blockingGet(30, TimeUnit.SECONDS);

		Perhaps<HttpClientResponse> remote = HttpClient.create(c.address().getPort())
		                                            .get("/test/test.css");

		Perhaps<String> page = Perhaps.fromMaybe(remote
				.flatMapPublisher(r -> r.receive()
				               .asString()
				               .rebatchRequests(1))
				.reduce(String::concat));

		Perhaps<String> cancelledPage = Perhaps.fromMaybe(remote
				.flatMapPublisher(r -> r.receive()
				               .asString()
				               .take(5)
				               .rebatchRequests(1))
				.reduce(String::concat));

		page.blockingGet(30, TimeUnit.SECONDS);
		cancelledPage.blockingGet(30, TimeUnit.SECONDS);
		page.blockingGet(30, TimeUnit.SECONDS);
		c.dispose();
	}

	@Test
	public void serverInfiniteClientClose() throws Exception {

		CountDownLatch latch = new CountDownLatch(1);
		NettyContext c = HttpServer.create(0)
		                           .newHandler((req, resp) -> {
			                           req.context()
			                              .onClose(latch::countDown);

			                           return Flowable.interval(1, TimeUnit.SECONDS)
			                                      .flatMap(d -> {
				                                      req.context()
				                                         .channel()
				                                         .config()
				                                         .setAutoRead(true);

				                                      return resp.sendObject(Unpooled.EMPTY_BUFFER)
				                                                 .then()
				                                                 .doOnComplete(() -> req.context()
				                                                                      .channel()
				                                                                      .config()
				                                                                      .setAutoRead(
						                                                                      false));
			                                      });
		                           })
		                           .blockingGet(30, TimeUnit.SECONDS);

		Perhaps<HttpClientResponse> remote = HttpClient.create(c.address().getPort())
		                                            .get("/");

		HttpClientResponse r = remote.blockingGet();
		r.dispose();
		while (r.channel()
		        .isActive()) {
		}
		latch.await();
		c.dispose();
	}

	@Test
	@Ignore
	public void proxy() throws Exception {
		Perhaps<HttpClientResponse> remote = HttpClient.create(o -> o.proxy(ops -> ops.type(Proxy.HTTP)
		                                                                           .host("127.0.0.1")
		                                                                           .port(8888)))
		          .get("https://projectreactor.io",
				          c -> c.followRedirect()
				                .sendHeaders());

		Perhaps<String> page = Perhaps.fromMaybe(remote
				.flatMapPublisher(r -> r.receive()
				               .retain()
				               .asString()
				               .rebatchRequests(1))
				.reduce(String::concat));

		page.blockingGet(30, TimeUnit.SECONDS);
	}

	@Test
	@Ignore
	public void nonProxyHosts() throws Exception {
		HttpClient client = HttpClient.create(o -> o.proxy(ops -> ops.type(Proxy.HTTP)
		                                                             .host("127.0.0.1")
		                                                             .port(8888)
		                                                             .nonProxyHosts("spring.io")));
		Perhaps<HttpClientResponse> remote1 = client.get("https://projectreactor.io",
		                                                 c -> c.followRedirect()
		                                                       .sendHeaders());
		Perhaps<HttpClientResponse> remote2 = client.get("https://spring.io",
		                                                 c -> c.followRedirect()
		                                                       .sendHeaders());

		Perhaps<String> page1 = Perhaps.fromMaybe(remote1
				.flatMapPublisher(r -> r.receive()
				               .retain()
				               .asString()
				               .rebatchRequests(1))
				.reduce(String::concat));

		Perhaps<String> page2 = Perhaps.fromMaybe(remote2
				.flatMapPublisher(r -> r.receive()
				               .retain()
				               .asString()
				               .rebatchRequests(1))
				.reduce(String::concat));

		page1.test()
		            .assertValue(s -> s.contains("<title>Project Reactor</title>"))
		            .assertComplete()
		            .await(30, TimeUnit.SECONDS);

		page2.test()
		            .assertValue(s -> s.contains("<title>Spring</title>"))
		            .assertComplete()
		            .await(30, TimeUnit.SECONDS);
	}

	//@Test
	public void postUpload() throws Exception {
		InputStream f = getClass().getResourceAsStream("/public/index.html");
		//Path f = Paths.get("/Users/smaldini/Downloads/IMG_6702.mp4");
		int res = HttpClient.create("google.com")
		                    .put("/post",
				                    c -> Nono.fromPublisher(c.sendForm(form -> form.multipart(true)
				                                                .file("test", f)
				                                                .attr("att1",
						                                                     "attr2")
				                                                .file("test2", f))
				                          ))
		                    .flatMap(r -> Perhaps.just(r.status()
		                                          .code()))
		                    .blockingGet(30, TimeUnit.SECONDS);
		res = HttpClient.create("google.com")
		                .get("/search",
				                c -> c.followRedirect()
				                      .sendHeaders())
		                .flatMap(r -> Perhaps.just(r.status()
		                                      .code()))
		                .blockingGet(30, TimeUnit.SECONDS);

		if (res != 200) {
			throw new IllegalStateException("test status failed with " + res);
		}
	}

	@Test
	public void simpleTest404() {
		int res = HttpClient.create("google.com")
		                    .get("/unsupportedURI",
				                    c -> c.followRedirect()
				                          .sendHeaders())
		                    .flatMap(r -> Perhaps.just(r.status()
		                                          .code()))
		                    .onErrorResumeNext(e -> {
		                    	if (e instanceof HttpClientException) {
														return Perhaps.just(((HttpClientException) e).status()
																.code());
													} else {
		                    		return Perhaps.error(e);
													}
												})
		                    .blockingGet(30, TimeUnit.SECONDS);

		if (res != 404) {
			throw new IllegalStateException("test status failed with " + res);
		}
	}

	@Test
	public void disableChunkForced() throws Exception {
		HttpClientResponse r = HttpClient.create("google.com")
		                                 .get("/unsupportedURI",
				                                 c -> c.chunkedTransfer(false)
				                                       .failOnClientError(false)
				                                       .sendString(Flowable.just("hello")))
		                                 .blockingGet(30, TimeUnit.SECONDS);

		FutureNono.from(r.context()
		                 .channel()
		                 .closeFuture())
		          .blockingAwait(5, TimeUnit.SECONDS);

		Assert.assertTrue(r.status() == HttpResponseStatus.NOT_FOUND);
	}

	@Test
	public void disableChunkForced2() throws Exception {
		HttpClientResponse r = HttpClient.create("google.com")
		                                 .get("/unsupportedURI",
				                                 c -> c.chunkedTransfer(false)
				                                       .failOnClientError(false)
				                                       .keepAlive(false))
		                                 .blockingGet(30, TimeUnit.SECONDS);

		FutureNono.from(r.context()
		                 .channel()
		                 .closeFuture())
		          .blockingAwait(5, TimeUnit.SECONDS);

		Assert.assertTrue(r.status() == HttpResponseStatus.NOT_FOUND);
	}

	@Test
	public void disableChunkImplicit() throws Exception {
		PoolResources p = PoolResources.fixed("test", 1);

		HttpClientResponse r = HttpClient.create(opts -> opts.poolResources(p))
		                                 .get("http://google.com/unsupportedURI",
				                                 c -> c.failOnClientError(false)
				                                       .sendHeaders())
		                                 .blockingGet(30, TimeUnit.SECONDS);

		HttpClientResponse r2 = HttpClient.create(opts -> opts.poolResources(p))
		                                  .get("http://google.com/unsupportedURI",
				                                  c -> c.failOnClientError(false)
				                                        .sendHeaders())
		                                  .blockingGet(30, TimeUnit.SECONDS);
		Assert.assertTrue(r.context()
		                   .channel() == r2.context()
		                                   .channel());

		Assert.assertTrue(r.status() == HttpResponseStatus.NOT_FOUND);
	}

	@Test
	public void disableChunkImplicitDefault() throws Exception {
		HttpClientResponse r = HttpClient.create("google.com")
		                                 .get("/unsupportedURI",
				                                 c -> c.chunkedTransfer(false)
				                                       .failOnClientError(false))
		                                 .blockingGet(30, TimeUnit.SECONDS);

		FutureNono.from(r.context()
		                 .channel()
		                 .closeFuture())
		          .blockingAwait(5, TimeUnit.SECONDS);

		Assert.assertTrue(r.status() == HttpResponseStatus.NOT_FOUND);
	}

	@Test
	public void contentHeader() throws Exception {
		PoolResources fixed = PoolResources.fixed("test", 1);
		HttpClientResponse r = HttpClient.create(opts -> opts.poolResources(fixed))
		                                 .get("http://google.com",
				                                 c -> c.header("content-length", "1")
				                                       .failOnClientError(false)
				                                       .sendString(Flowable.just(" ")))
		                                 .blockingGet(30, TimeUnit.SECONDS);

		HttpClient.create(opts -> opts.poolResources(fixed))
		          .get("http://google.com",
				          c -> c.header("content-length", "1")
				                .failOnClientError(false)
				                .sendString(Flowable.just(" ")))
		          .blockingGet(30, TimeUnit.SECONDS);

		Assert.assertTrue(r.status() == HttpResponseStatus.BAD_REQUEST);
	}

	@Test
	public void simpleTestHttps() {

		HttpClient.create()
								.get("https://developer.chrome.com")
								.flatMap(r -> Perhaps.just(r.status().code()))
								.test()
								.awaitDone(30, TimeUnit.SECONDS)
		            .assertValue(status -> status >= 200 && status < 400)
		            .assertComplete();

		HttpClient.create()
								.get("https://developer.chrome.com")
								.flatMap(r -> Perhaps.just(r.status().code()))
								.test()
								.awaitDone(30, TimeUnit.SECONDS)
		            .assertValue(status -> status >= 200 && status < 400)
		            .assertComplete();
	}

	@Test
	public void prematureCancel() throws Exception {
		NonoProcessor signal = NonoProcessor.create();
		NettyContext x = TcpServer.create("localhost", 0)
		                          .newHandler((in, out) -> {
										signal.onComplete();
										return out.context(c -> c.addHandlerFirst(
												new HttpResponseEncoder()))
										          .sendObject(Flowable.timer(2, TimeUnit.SECONDS)
												          .map(t ->
												          new DefaultFullHttpResponse(
														          HttpVersion.HTTP_1_1,
														          HttpResponseStatus
																          .PROCESSING)))
												.neverComplete();
		                          })
		                          .blockingGet(30, TimeUnit.SECONDS);

		HttpClient.create(x.address().getHostName(), x.address().getPort())
								.get("/")
								.timeout(signal)
								.test()
		            .assertError(TimeoutException.class);
//		Thread.sleep(1000000);
	}

	@Test
	public void gzip() {
		//verify gzip is negotiated (when no decoder)
		HttpClient.create()
						.get("http://www.httpwatch.com", req -> req
								.addHeader("Accept-Encoding", "gzip")
								.addHeader("Accept-Encoding", "deflate")
						)
						.flatMap(r -> Perhaps.fromMaybe(r.receive().asString().elementAt(0).map(s -> s.substring(0, 100))
												.zipWith(Maybe.just(r.responseHeaders().get("Content-Encoding", "")),
														SimpleImmutableEntry::new)))
						.test()
						.awaitDone(30, TimeUnit.SECONDS)
						.assertValue(tuple -> !tuple.getKey().contains("<html>") && !tuple.getKey().contains("<head>")
								&& "gzip".equals(tuple.getValue()))
						.assertComplete();

		//verify decoder does its job and removes the header
		HttpClient.create()
						.get("http://www.httpwatch.com", req -> {
							req.context().addHandlerFirst("gzipDecompressor", new HttpContentDecompressor());
							return req.addHeader("Accept-Encoding", "gzip")
												.addHeader("Accept-Encoding", "deflate");
						})
						.flatMap(r -> Perhaps.fromMaybe(r.receive().asString().elementAt(0).map(s -> s.substring(0, 100))
												.zipWith(Maybe.just(r.responseHeaders().get("Content-Encoding", "")),
														SimpleImmutableEntry::new)))
						.test()
						.awaitDone(30, TimeUnit.SECONDS)
						.assertValue(tuple -> tuple.getKey().contains("<html>") && tuple.getKey().contains("<head>")
								&& "".equals(tuple.getValue()))
						.assertComplete();
	}

	@Test
	public void gzipEnabled() {
		doTestGzip(true);
	}

	@Test
	public void gzipDisabled() {
		doTestGzip(false);
	}

	private void doTestGzip(boolean gzipEnabled) {
		String expectedResponse = gzipEnabled ? "gzip" : "no gzip";
		NettyContext server = HttpServer.create(0)
		        .newHandler((req,res) -> res.sendString(
		                Flowable.just(req.requestHeaders().get(HttpHeaderNames.ACCEPT_ENCODING, "no gzip"))))
		        .blockingGet(30, TimeUnit.SECONDS);
		HttpClient.create(ops -> ops.port(server.address().getPort()).compression(gzipEnabled))
							.get("/")
							.flatMap(r -> Perhaps.fromMaybe(r.receive().asString().elementAt(0)))
							.test()
							.awaitDone(30, TimeUnit.SECONDS)
							.assertValue(str -> expectedResponse.equals(str))
							.assertComplete();
	}

	@Test
	public void testUserAgent() {
		NettyContext c = HttpServer.create(0)
		                           .newHandler((req, resp) -> {
			                           Assert.assertTrue(""+req.requestHeaders()
			                                                   .get(HttpHeaderNames.USER_AGENT),
					                           req.requestHeaders()
			                                               .contains(HttpHeaderNames.USER_AGENT) && req.requestHeaders()
			                                                                                           .get(HttpHeaderNames.USER_AGENT)
			                                                                                           .equals(HttpClient.USER_AGENT));

			                           return resp;
		                           })
		                           .blockingGet();

		HttpClient.create(c.address().getPort())
		          .get("/")
		          .blockingGet();

		c.dispose();
	}

	@Test
	public void toStringShowsOptions() {
		HttpClient client = HttpClient.create(opt -> opt.host("foo")
		                                                .port(123)
		                                                .compression(true));

		assertThat(client.toString()).isEqualTo("HttpClient: connecting to foo:123 with gzip");
	}

	@Test
	public void gettingOptionsDuplicates() {
		HttpClient client = HttpClient.create(opt -> opt.host("foo").port(123).compression(true));
		assertThat(client.options())
				.isNotSameAs(client.options)
				.isNotSameAs(client.options());
	}

	@Test
	public void sshExchangeRelativeGet() throws CertificateException, SSLException {
		SelfSignedCertificate ssc = new SelfSignedCertificate();
		SslContext sslServer = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
		                                        .build();
		SslContext sslClient = SslContextBuilder.forClient()
		                                        //make the client to trust the self signed certificate
		                                        .trustManager(ssc.cert())
		                                        .build();

		NettyContext context =
				HttpServer.create(opt -> opt.sslContext(sslServer))
				          .newHandler((req, resp) -> resp.sendString(Flowable.just("hello ", req.uri())))
				          .blockingGet();


		HttpClientResponse response = HttpClient.create(
				opt -> opt.port(context.address().getPort())
				          .sslContext(sslClient))
		                                        .get("/foo")
		                                        .blockingGet(200, TimeUnit.MILLISECONDS);
		context.dispose();
		context.onClose().blockingAwait();

		String responseString = response.receive().aggregate().asString(CharsetUtil.UTF_8).blockingGet();
		assertThat(responseString).isEqualTo("hello /foo");
	}

	@Test
	public void sshExchangeAbsoluteGet() throws CertificateException, SSLException {
		SelfSignedCertificate ssc = new SelfSignedCertificate();
		SslContext sslServer = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
		SslContext sslClient = SslContextBuilder.forClient()
		                                        .trustManager(ssc.cert()).build();

		NettyContext context =
				HttpServer.create(opt -> opt.sslContext(sslServer))
				          .newHandler((req, resp) -> resp.sendString(Flowable.just("hello ", req.uri())))
				          .blockingGet();

		HttpClientResponse response = HttpClient.create(
				opt -> opt.port(context.address().getPort())
				          .sslContext(sslClient)
		)
		                                        .get("https://localhost:" + context.address().getPort() + "/foo")
		                                        .blockingGet(200, TimeUnit.MILLISECONDS);
		context.dispose();
		context.onClose().blockingAwait();

		String responseString = response.receive().aggregate().asString(CharsetUtil.UTF_8).blockingGet();
		assertThat(responseString).isEqualTo("hello /foo");
	}

	@Test
	public void secureSendFile()
			throws CertificateException, SSLException, URISyntaxException {
		Path largeFile = Paths.get(getClass().getResource("/largeFile.txt").toURI());
		SelfSignedCertificate ssc = new SelfSignedCertificate();
		SslContext sslServer = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
		SslContext sslClient = SslContextBuilder.forClient().trustManager(ssc.cert()).build();
		AtomicReference<String> uploaded = new AtomicReference<>();

		NettyContext context =
				HttpServer.create(opt -> opt.sslContext(sslServer))
				          .newRouter(r -> r.post("/upload", (req, resp) ->
						          req.receive()
						             .aggregate()
						             .asString(StandardCharsets.UTF_8)
						             .doOnNext(uploaded::set)
												 .ignoreElement()
						             .andThen(resp.status(201).sendString(Flowable.just("Received File")).then())))
				          .blockingGet();

		HttpClientResponse response =
				HttpClient.create(opt -> opt.port(context.address().getPort())
				                            .sslContext(sslClient))
				          .post("/upload", r -> r.sendFile(largeFile))
				          .blockingGet(120, TimeUnit.SECONDS);

		context.dispose();
		context.onClose().blockingAwait();

		String responseBody = response.receive().aggregate().asString().blockingGet();
		assertThat(response.status().code()).isEqualTo(201);
		assertThat(responseBody).isEqualTo("Received File");

		assertThat(uploaded.get())
				.startsWith("This is an UTF-8 file that is larger than 1024 bytes. " + "It contains accents like é.")
				.contains("1024 mark here -><- 1024 mark here")
				.endsWith("End of File");
	}

	@Test
	public void chunkedSendFile() throws URISyntaxException {
		Path largeFile = Paths.get(getClass().getResource("/largeFile.txt").toURI());
		AtomicReference<String> uploaded = new AtomicReference<>();

		NettyContext context =
				HttpServer.create(opt -> opt.host("localhost"))
				          .newRouter(r -> r.post("/upload", (req, resp) ->
						          req
								          .receive()
								          .aggregate()
								          .asString(StandardCharsets.UTF_8)
								          .doOnNext(uploaded::set)
													.ignoreElement()
								          .andThen(resp.status(201).sendString(Flowable.just("Received File")).then())))
				          .blockingGet();

		HttpClientResponse response =
				HttpClient.create(opt -> opt.port(context.address().getPort()))
				          .post("/upload", r -> r.sendFile(largeFile))
				          .blockingGet(120, TimeUnit.SECONDS);

		context.dispose();
		context.onClose().blockingAwait();

		String responseBody = response.receive().aggregate().asString().blockingGet();
		assertThat(response.status().code()).isEqualTo(201);
		assertThat(responseBody).isEqualTo("Received File");

		assertThat(uploaded.get())
				.startsWith("This is an UTF-8 file that is larger than 1024 bytes. " + "It contains accents like é.")
				.contains("1024 mark here -><- 1024 mark here")
				.endsWith("End of File");
	}
}
