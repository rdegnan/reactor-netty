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
package reactor.ipc.netty.http;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

import io.netty.handler.codec.http.HttpHeaders;
import io.reactivex.Flowable;
import org.junit.Assert;
import org.junit.Test;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.client.HttpClientResponse;
import reactor.ipc.netty.http.server.HttpServer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author mostroverkhov
 */
public class HttpCompressionClientServerTests {

	@Test
	public void trueEnabledIncludeContentEncoding() throws Exception {

		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(true));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Flowable.just("reply")))
				      .blockingSingle();

		HttpClient client = HttpClient.create(o -> o.compression(true)
		                                            .connectAddress(() -> address(nettyContext)));
		client.get("/test", o -> {
			Assert.assertTrue(o.requestHeaders()
			                   .contains("Accept-Encoding", "gzip", true));
			return o;
		})
		      .blockingSubscribe();

		nettyContext.dispose();
		nettyContext.onClose().blockingSubscribe();
	}

	@Test
	public void serverCompressionDefault() throws Exception {
		HttpServer server = HttpServer.create(0);

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Flowable.just("reply")))
				      .blockingSingle();

		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				client.get("/test", req -> req.header("Accept-Encoding", "gzip"))
				      .blockingSingle();

		assertThat(resp.responseHeaders().get("content-encoding")).isNull();

		String reply = resp.receive()
		                   .asString()
		                   .blockingFirst();
		Assert.assertEquals("reply", reply);

		nettyContext.dispose();
		nettyContext.onClose().blockingSubscribe();
	}

	@Test
	public void serverCompressionDisabled() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(false));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Flowable.just("reply")))
				      .blockingSingle();

		//don't activate compression on the client options to avoid auto-handling (which removes the header)
		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				//edit the header manually to attempt to trigger compression on server side
				client.get("/test", req -> req.header("Accept-Encoding", "gzip"))
				      .blockingSingle();

		assertThat(resp.responseHeaders().get("content-encoding")).isNull();

		String reply = resp.receive()
		                   .asString()
		                   .blockingFirst();
		Assert.assertEquals("reply", reply);
		nettyContext.dispose();
		nettyContext.onClose().blockingSubscribe();
	}

	@Test
	public void serverCompressionAlwaysEnabled() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(true));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Flowable.just("reply")))
				      .blockingSingle();

		//don't activate compression on the client options to avoid auto-handling (which removes the header)
		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				//edit the header manually to attempt to trigger compression on server side
				client.get("/test", req -> req.header("Accept-Encoding", "gzip"))
				      .blockingSingle();

		assertThat(resp.responseHeaders().get("content-encoding")).isEqualTo("gzip");

		byte[] replyBuffer = resp.receive()
		                         .aggregate()
		                         .asByteArray()
		                         .blockingGet();

		assertThat(new String(replyBuffer)).isNotEqualTo("reply");

		GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(replyBuffer));
		byte deflatedBuf[] = new byte[1024];
		int readable = gis.read(deflatedBuf);
		gis.close();

		assertThat(readable).isGreaterThan(0);

		String deflated = new String(deflatedBuf, 0, readable);

		assertThat(deflated).isEqualTo("reply");

		nettyContext.dispose();
		nettyContext.onClose().blockingSubscribe();
	}

	@Test
	public void serverCompressionEnabledSmallResponse() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(25));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Flowable.just("reply")))
				      .blockingSingle();

		//don't activate compression on the client options to avoid auto-handling (which removes the header)
		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				//edit the header manually to attempt to trigger compression on server side
				client.get("/test", req -> req.header("Accept-Encoding", "gzip"))
				      .blockingSingle();

		//check the server didn't send the gzip header, only transfer-encoding
		HttpHeaders headers = resp.responseHeaders();
		assertThat(headers.get("transFER-encoding")).isEqualTo("chunked");
		assertThat(headers.get("conTENT-encoding")).isNull();

		//check the server sent plain text
		String reply = resp.receive()
		                   .asString()
		                   .blockingFirst();
		Assert.assertEquals("reply", reply);
		nettyContext.dispose();
		nettyContext.onClose().blockingSubscribe();
	}

	@Test
	public void serverCompressionEnabledBigResponse() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(4));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Flowable.just("reply")))
				      .blockingSingle();

		//don't activate compression on the client options to avoid auto-handling (which removes the header)
		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));
		HttpClientResponse resp =
				//edit the header manually to attempt to trigger compression on server side
				client.get("/test", req -> req.header("accept-encoding", "gzip"))
				      .blockingSingle();

		assertThat(resp.responseHeaders().get("content-encoding")).isEqualTo("gzip");

		byte[] replyBuffer = resp.receive()
		                         .aggregate()
		                         .asByteArray()
		                         .blockingGet();

		assertThat(new String(replyBuffer)).isNotEqualTo("reply");

		GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(replyBuffer));
		byte deflatedBuf[] = new byte[1024];
		int readable = gis.read(deflatedBuf);
		gis.close();

		assertThat(readable).isGreaterThan(0);

		String deflated = new String(deflatedBuf, 0, readable);

		assertThat(deflated).isEqualTo("reply");

		nettyContext.dispose();
		nettyContext.onClose().blockingSubscribe();
	}

	@Test
	public void compressionServerEnabledClientDisabledIsNone() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0)
		                                            .compression(true));

		String serverReply = "reply";
		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Flowable.just(serverReply)))
				      .blockingSingle();

		HttpClient client = HttpClient.create(o -> o.compression(false)
		                                            .connectAddress(() -> address(nettyContext)));

		HttpClientResponse resp = client.get("/test").blockingSingle();

		String reply = resp.receive()
		                   .asString()
		                   .blockingFirst();

		assertThat(resp.responseHeaders().get("Content-Encoding")).isNull();
		assertThat(reply).isEqualTo(serverReply);

		nettyContext.dispose();
		nettyContext.onClose().blockingSubscribe();
	}


	@Test
	public void compressionServerDefaultClientDefaultIsNone() throws Exception {
		HttpServer server = HttpServer.create(o -> o.port(0));

		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Flowable.just("reply")))
				      .blockingSingle();

		HttpClient client = HttpClient.create(o -> o.connectAddress(() -> address(nettyContext)));

		HttpClientResponse resp =
				client.get("/test").blockingSingle();

		String reply = resp.receive()
		                   .asString()
		                   .blockingFirst();

		assertThat(resp.responseHeaders().get("Content-Encoding")).isNull();
		assertThat(reply).isEqualTo("reply");

		nettyContext.dispose();
		nettyContext.onClose().blockingSubscribe();
	}

	@Test
	public void compressionActivatedOnClientAddsHeader() {
		AtomicReference<String> zip = new AtomicReference<>("fail");

		HttpServer server = HttpServer.create(o -> o.port(0).compression(true));
		NettyContext nettyContext =
				server.newHandler((in, out) -> out.sendString(Flowable.just("reply")))
				      .blockingSingle();
		HttpClient client = HttpClient.create(opt -> opt.compression(true)
		                                                .connectAddress(() -> address(nettyContext)));

		client.get("/test", req -> {
			zip.set(req.requestHeaders().get("accept-encoding"));
			return req;
		}).blockingSubscribe();

		assertThat(zip.get()).isEqualTo("gzip");
	}

	private InetSocketAddress address(NettyContext nettyContext) {
		return new InetSocketAddress(nettyContext.address()
		                                         .getPort());
	}
}
