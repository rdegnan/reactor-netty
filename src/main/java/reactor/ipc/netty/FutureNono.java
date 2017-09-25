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
package reactor.ipc.netty;

import java.util.Objects;
import java.util.function.Supplier;

import hu.akarnokd.rxjava2.basetypes.Nono;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.reactivex.internal.subscriptions.EmptySubscription;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Convert Netty Future into void {@link Nono}.
 *
 * @author Stephane Maldini
 */
public abstract class FutureNono extends Nono {

	/**
	 * Convert a {@link Future} into {@link Nono}. {@link Nono#subscribe(Subscriber)}
	 * will bridge to {@link Future#addListener(GenericFutureListener)}.
	 *
	 * @param future the future to convert from
	 * @param <F> the future type
	 *
	 * @return A {@link Nono} forwarding {@link Future} success or failure
	 */
	public static <F extends Future<Void>> Nono from(F future) {
		if(future.isDone()){
			if(!future.isSuccess()){
				return Nono.error(future.cause());
			}
			return Nono.complete();
		}
		return new ImmediateFutureNono<>(future);
	}

	/**
	 * Convert a supplied {@link Future} for each subscriber into {@link Nono}.
	 * {@link Nono#subscribe(Subscriber)}
	 * will bridge to {@link Future#addListener(GenericFutureListener)}.
	 *
	 * @param deferredFuture the future to evaluate and convert from
	 * @param <F> the future type
	 *
	 * @return A {@link Nono} forwarding {@link Future} success or failure
	 */
	public static <F extends Future<Void>> Nono deferFuture(Supplier<F> deferredFuture) {
		return new DeferredFutureNono<>(deferredFuture);
	}

	final static class ImmediateFutureNono<F extends Future<Void>> extends FutureNono {

		final F future;

		ImmediateFutureNono(F future) {
			this.future = Objects.requireNonNull(future, "future");
		}

		@Override
		protected void subscribeActual(Subscriber<? super Void> s) {
			if(future.isDone()){
				if(future.isSuccess()){
					EmptySubscription.complete(s);
				}
				else{
					EmptySubscription.error(future.cause(), s);
				}
				return;
			}

			FutureSubscription<F> fs = new FutureSubscription<>(future, s);
			s.onSubscribe(fs);
			future.addListener(fs);
		}
	}

	final static class DeferredFutureNono<F extends Future<Void>> extends FutureNono {

		final Supplier<F> deferredFuture;

		DeferredFutureNono(Supplier<F> deferredFuture) {
			this.deferredFuture =
					Objects.requireNonNull(deferredFuture, "deferredFuture");
		}

		@Override
		protected void subscribeActual(Subscriber<? super Void> s) {
			F f = deferredFuture.get();

			if (f == null) {
				EmptySubscription.error(new NullPointerException("Deferred supplied null"), s);
				return;
			}

			if(f.isDone()){
				if(f.isSuccess()){
					EmptySubscription.complete(s);
				}
				else{
					EmptySubscription.error(f.cause(), s);
				}
				return;
			}

			FutureSubscription<F> fs = new FutureSubscription<>(f, s);
			s.onSubscribe(fs);
			f.addListener(fs);
		}


	}

	final static class FutureSubscription<F extends Future<Void>> implements
	                                                GenericFutureListener<F>,
	                                                Subscription {

		final Subscriber<? super Void> s;
		final F                        future;

		FutureSubscription(F future, Subscriber<? super Void> s) {
			this.s = s;
			this.future = future;
		}

		@Override
		public void request(long n) {
			//noop
		}

		@Override
		public void cancel() {
			future.removeListener(this);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void operationComplete(F future) throws Exception {
			if (!future.isSuccess()) {
				s.onError(future.cause());
			}
			else {
				s.onComplete();
			}
		}
	}
}
