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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import org.junit.Test;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.server.HttpServer;

public class FlowableReceiveTest {

	@Test
	public void testByteBufsReleasedWhenTimeout() {
		ResourceLeakDetector.setLevel(Level.PARANOID);

		byte[] content = new byte[1024*8];
		Random rndm = new Random();
		rndm.nextBytes(content);

		NettyContext server1 =
				HttpServer.create(0)
				          .newRouter(routes ->
				                     routes.get("/target", (req, res) ->
				                           res.sendByteArray(Flowable.just(content)
				                                                 .delay(100, TimeUnit.MILLISECONDS))))
				          .blockingGet();

		NettyContext server2 =
				HttpServer.create(0)
				          .newRouter(routes ->
				                     routes.get("/forward", (req, res) ->
				                           HttpClient.create(server1.address().getPort())
				                                     .get("/target")
				                                     .delay(50, TimeUnit.MILLISECONDS)
				                                     .flatMap(response -> response.receive().aggregate().asString())
				                                     .timeout(50, TimeUnit.MILLISECONDS)
				                                     .ignoreElement()))
				          .blockingGet();

		Flowable.range(0, 50)
		    .flatMapMaybe(i -> HttpClient.create(server2.address().getPort())
		                            .get("/forward")
		                            .onErrorResumeNext(t -> {
		                            	return Maybe.empty();
																}))
		    .ignoreElements()
				.blockingAwait(30, TimeUnit.SECONDS);

		ResourceLeakDetector.setLevel(Level.SIMPLE);
	}
}
