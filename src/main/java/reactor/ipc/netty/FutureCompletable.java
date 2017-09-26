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

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.EmptyDisposable;

/**
 * Convert Netty Future into {@link Completable}.
 *
 * @author Stephane Maldini
 */
public abstract class FutureCompletable extends Completable {

	/**
	 * Convert a {@link Future} into {@link Completable}. {@link Completable#subscribe(CompletableObserver)}
	 * will bridge to {@link Future#addListener(GenericFutureListener)}.
	 *
	 * @param future the future to convert from
	 * @param <F> the future type
	 *
	 * @return A {@link Completable} forwarding {@link Future} success or failure
	 */
	public static <F extends Future<Void>> Completable from(F future) {
		if(future.isDone()){
			if(!future.isSuccess()){
				return Completable.error(future.cause());
			}
			return Completable.complete();
		}
		return new ImmediateFutureCompletable<>(future);
	}

	/**
	 * Convert a supplied {@link Future} for each subscriber into {@link Completable}.
	 * {@link Completable#subscribe(CompletableObserver)}
	 * will bridge to {@link Future#addListener(GenericFutureListener)}.
	 *
	 * @param deferredFuture the future to evaluate and convert from
	 * @param <F> the future type
	 *
	 * @return A {@link Completable} forwarding {@link Future} success or failure
	 */
	public static <F extends Future<Void>> Completable deferFuture(Supplier<F> deferredFuture) {
		return new DeferredFutureCompletable<>(deferredFuture);
	}

	final static class ImmediateFutureCompletable<F extends Future<Void>> extends Completable {

		final F future;

		ImmediateFutureCompletable(F future) {
			this.future = Objects.requireNonNull(future, "future");
		}

		@Override
		protected void subscribeActual(CompletableObserver s) {
			if(future.isDone()){
				if(future.isSuccess()){
					EmptyDisposable.complete(s);
				}
				else{
					EmptyDisposable.error(future.cause(), s);
				}
				return;
			}

			FutureSubscription<F> fs = new FutureSubscription<>(future, s);
			s.onSubscribe(fs);
			future.addListener(fs);
		}
	}

	final static class DeferredFutureCompletable<F extends Future<Void>> extends FutureCompletable {

		final Supplier<F> deferredFuture;

		DeferredFutureCompletable(Supplier<F> deferredFuture) {
			this.deferredFuture =
					Objects.requireNonNull(deferredFuture, "deferredFuture");
		}

		@Override
		protected void subscribeActual(CompletableObserver s) {
			F f = deferredFuture.get();

			if (f == null) {
				EmptyDisposable.error(new NullPointerException("Deferred supplied null"), s);
				return;
			}

			if(f.isDone()){
				if(f.isSuccess()){
					EmptyDisposable.complete(s);
				}
				else{
					EmptyDisposable.error(f.cause(), s);
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
	                                                Disposable {

		final CompletableObserver s;
		final F                   future;

		FutureSubscription(F future, CompletableObserver s) {
			this.s = s;
			this.future = future;
		}

		@Override
		public void dispose() {
			future.removeListener(this);
		}

		@Override
		public boolean isDisposed() {
			return false;
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
