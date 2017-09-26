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
package reactor.ipc.netty.resources;

import io.reactivex.Completable;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultLoopResourcesTest {

	@Test
	public void disposeLaterDefers() {
		DefaultLoopResources loopResources = new DefaultLoopResources(
				"test", 0, false);

		Completable disposer = loopResources.disposeLater();
		assertThat(loopResources.isDisposed()).isFalse();

		disposer.subscribe();
		assertThat(loopResources.isDisposed()).isTrue();
	}

	@Test
	public void disposeLaterSubsequentIsQuick() {
		DefaultLoopResources loopResources = new DefaultLoopResources(
				"test", 0, false);

		assertThat(loopResources.isDisposed()).isFalse();

		loopResources.disposeLater()
				.test()
				.assertComplete();
		assertThat(loopResources.isDisposed()).isTrue();
		assertThat(loopResources.serverLoops.isTerminated()).isTrue();

		loopResources.disposeLater()
				.test()
				.assertComplete();
	}

}