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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufHolder;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.functions.Function;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * A decorating {@link Flowable} {@link NettyInbound} with various {@link ByteBuf} related
 * operations.
 *
 * @author Stephane Maldini
 */
public final class ByteBufFlowable extends Flowable<ByteBuf> {

	/**
	 * Decorate as {@link ByteBufFlowable}
	 *
	 * @param source publisher to decorate
	 *
	 * @return a {@link ByteBufFlowable}
	 */
	public static ByteBufFlowable fromInbound(Publisher<?> source) {
		return fromInbound(source, ByteBufAllocator.DEFAULT);
	}

	/**
	 * Decorate as {@link ByteBufFlowable}
	 *
	 * @param source publisher to decorate
	 * @param allocator the channel {@link ByteBufAllocator}
	 *
	 * @return a {@link ByteBufFlowable}
	 */
	public static ByteBufFlowable fromInbound(Publisher<?> source,
																						ByteBufAllocator allocator) {
		Objects.requireNonNull(allocator, "allocator");
		return new ByteBufFlowable(Flowable.unsafeCreate(source)
		                           .map(bytebufExtractor), allocator);
	}

	/**
	 * Open a {@link java.nio.channels.FileChannel} from a path and stream
	 * {@link ByteBuf} chunks with a default maximum size of 500K into
	 * the returned {@link ByteBufFlowable}
	 *
	 * @param path the path to the resource to stream
	 *
	 * @return a {@link ByteBufFlowable}
	 */
	public static ByteBufFlowable fromPath(Path path) {
		return fromPath(path, MAX_CHUNK_SIZE);
	}

	/**
	 * Open a {@link java.nio.channels.FileChannel} from a path and stream
	 * {@link ByteBuf} chunks with a given maximum size into the returned {@link ByteBufFlowable}
	 *
	 * @param path the path to the resource to stream
	 * @param maxChunkSize the maximum per-item ByteBuf size
	 *
	 * @return a {@link ByteBufFlowable}
	 */
	public static ByteBufFlowable fromPath(Path path, int maxChunkSize) {
		return fromPath(path, maxChunkSize, ByteBufAllocator.DEFAULT);
	}

	/**
	 * Open a {@link java.nio.channels.FileChannel} from a path and stream
	 * {@link ByteBuf} chunks with a default maximum size of 500K into the returned
	 * {@link ByteBufFlowable}, using the provided {@link ByteBufAllocator}.
	 *
	 * @param path the path to the resource to stream
	 * @param allocator the channel {@link ByteBufAllocator}
	 *
	 * @return a {@link ByteBufFlowable}
	 */
	public static ByteBufFlowable fromPath(Path path, ByteBufAllocator allocator) {
		return fromPath(path, MAX_CHUNK_SIZE, allocator);
	}

	/**
	 * Open a {@link java.nio.channels.FileChannel} from a path and stream
	 * {@link ByteBuf} chunks with a given maximum size into the returned
	 * {@link ByteBufFlowable}, using the provided {@link ByteBufAllocator}.
	 *
	 * @param path the path to the resource to stream
	 * @param maxChunkSize the maximum per-item ByteBuf size
	 * @param allocator the channel {@link ByteBufAllocator}
	 *
	 * @return a {@link ByteBufFlowable}
	 */
	public static ByteBufFlowable fromPath(Path path,
																				 int maxChunkSize,
																				 ByteBufAllocator allocator) {
		Objects.requireNonNull(path, "path");
		Objects.requireNonNull(allocator, "allocator");
		if (maxChunkSize < 1) {
			throw new IllegalArgumentException("chunk size must be strictly positive, " + "was: " + maxChunkSize);
		}
		return new ByteBufFlowable(Flowable.generate(() -> FileChannel.open(path), (fc, sink) -> {
			try {
				ByteBuf buf = allocator.buffer();
				long pos;
				if ((pos = buf.writeBytes(fc, maxChunkSize)) < 0) {
					sink.onComplete();
				}
				else {
					sink.onNext(buf);
				}
			}
			catch (IOException e) {
				sink.onError(e);
			}
			return fc;
		}), allocator);
	}

	/**
	 * Convert to a {@link ByteBuffer} inbound {@link Flowable}
	 *
	 * @return a {@link ByteBuffer} inbound {@link Flowable}
	 */
	public final Flowable<ByteBuffer> asByteBuffer() {
		return map(ByteBuf::nioBuffer);
	}

	/**
	 * Convert to a {@literal byte[]} inbound {@link Flowable}
	 *
	 * @return a {@literal byte[]} inbound {@link Flowable}
	 */
	public final Flowable<byte[]> asByteArray() {
		return map(bb -> {
			byte[] bytes = new byte[bb.readableBytes()];
			bb.readBytes(bytes);
			return bytes;
		});
	}

	/**
	 * Convert to a {@link InputStream} inbound {@link Flowable}
	 *
	 * @return a {@link InputStream} inbound {@link Flowable}
	 */
	public Flowable<InputStream> asInputStream() {
		return map(ByteBufMaybe.ReleasingInputStream::new);
	}

	/**
	 * Convert to a {@link String} inbound {@link Flowable} using the default {@link Charset}.
	 *
	 * @return a {@link String} inbound {@link Flowable}
	 */
	public final Flowable<String> asString() {
		return asString(Charset.defaultCharset());
	}

	/**
	 * Convert to a {@link String} inbound {@link Flowable} using the provided {@link Charset}.
	 *
	 * @param charset the decoding charset
	 *
	 * @return a {@link String} inbound {@link Flowable}
	 */
	public final Flowable<String> asString(Charset charset) {
		return map(s -> s.toString(charset));
	}

	/**
	 * Aggregate subsequent byte buffers into a single buffer.
	 *
	 * @return {@link ByteBufMaybe} of aggregated {@link ByteBuf}
	 */
	public ByteBufMaybe aggregate() {
		return Maybe.using(alloc::compositeBuffer,
				b -> this.reduce(b, (prev, next) -> prev.addComponent(next.retain()))
				         .doOnSuccess(cbb -> cbb.writerIndex(cbb.capacity()))
				         .filter(ByteBuf::isReadable),
				ByteBuf::release, false).to(ByteBufMaybe::new);
	}

	/**
	 * Allow multiple consumers downstream of the flux while also disabling auto memory
	 * release on each buffer published (retaining in order to prevent premature recycling).
	 *
	 * @return {@link ByteBufMaybe} of retained {@link ByteBuf}
	 */
	public ByteBufMaybe multicast() {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	/**
	 * Disable auto memory release on each buffer published, retaining in order to prevent
	 * premature recycling when buffers are accumulated downstream (async).
	 *
	 * @return {@link ByteBufFlowable} of retained {@link ByteBuf}
	 */
	public ByteBufFlowable retain() {
		return new ByteBufFlowable(doOnNext(ByteBuf::retain), alloc);
	}

	final Flowable<ByteBuf> source;
	final ByteBufAllocator alloc;

	ByteBufFlowable(Flowable<ByteBuf> source, ByteBufAllocator allocator) {
		this.source = source;
		this.alloc = allocator;
	}

	@Override
	protected void subscribeActual(Subscriber<? super ByteBuf> s) {
		source.subscribe(s);
	}

	/**
	 * A channel object to {@link ByteBuf} transformer
	 */
	final static Function<Object, ByteBuf> bytebufExtractor = o -> {
		if (o instanceof ByteBuf) {
			return (ByteBuf) o;
		}
		if (o instanceof ByteBufHolder) {
			return ((ByteBufHolder) o).content();
		}
		throw new IllegalArgumentException("Object " + o + " of type " + o.getClass() + " " + "cannot be converted to ByteBuf");
	};

	final static int MAX_CHUNK_SIZE = 1024 * 512; //500k
}
