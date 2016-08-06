/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.heliosapm.streams.collector.groovy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.codehaus.groovy.control.Janitor;
import org.codehaus.groovy.control.io.ReaderSource;

import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.utils.io.NIOHelper;
import com.heliosapm.utils.url.URLHelper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ByteProcessor;

/**
 * <p>Title: ByteBufReaderSource</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ByteBufReaderSource</code></p>
 */

public class ByteBufReaderSource implements ReaderSource {
	/** The original source file which will be null if this is injected code  */
	final File sourceFile;
	/** The byte buf we're going to store the source in */
	final ByteBuf sourceBuffer;
	/** The URI for this reader */
	final URI sourceURI;
	
	
	/** The UTF character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	

	/**
	 * Creates a new ByteBufReaderSource
	 * @param sourceFile the original source file
	 */
	public ByteBufReaderSource(final File sourceFile) {
		this.sourceFile = sourceFile;
		sourceBuffer = BufferManager.getInstance().directBuffer((int)this.sourceFile.length());
		ByteBuffer bb = null;
		try {
			bb = NIOHelper.load(sourceFile, false);						
			sourceBuffer.writeBytes(bb);
			sourceURI = sourceFile.toURI();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read source from source file [" + sourceFile + "]", ex);
		} finally {
			if(bb!=null) try { NIOHelper.clean(bb); } catch (Exception x) {/* No Op */}
		}
	}
	
	/**
	 * Creates a new ByteBufReaderSource which has been modified from the original source
	 * @param injectedSource the source to inject at the beginning of the source buffer
	 * @param fileSource the original unmodified code reader
	 */
	public ByteBufReaderSource(final CharSequence injectedSource, final ByteBufReaderSource fileSource) {
		final ByteBuf fsourceBuffer = fileSource.sourceBuffer.duplicate().resetReaderIndex();
		sourceBuffer = BufferManager.getInstance().directBuffer(injectedSource.length(), fsourceBuffer.readableBytes());
		ByteBufUtil.writeUtf8(sourceBuffer, injectedSource);
		sourceBuffer.writeBytes(fsourceBuffer);
		sourceFile = null;
		sourceURI = URLHelper.toURI(fileSource.sourceFile.toURI().toString() + "-modified");
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see org.codehaus.groovy.control.io.ReaderSource#getReader()
	 */
	@Override
	public Reader getReader() throws IOException {
		final InputStream is = new ByteBufInputStream(sourceBuffer.duplicate());
		return new InputStreamReader(is, UTF8); 
	}
	
	/**
	 * Releases the source buffer
	 */
	public void close() {
		try { sourceBuffer.release(); } catch (Exception x) {/* No Op */}
	}

	/**
	 * {@inheritDoc}
	 * @see org.codehaus.groovy.control.io.ReaderSource#canReopenSource()
	 */
	@Override
	public boolean canReopenSource() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 * @see org.codehaus.groovy.control.io.ReaderSource#getLine(int, org.codehaus.groovy.control.Janitor)
	 */
	@Override
	public String getLine(int lineNumber, Janitor janitor) {
		int start = 0;
		int eol = 0;
		final ByteBuf b = sourceBuffer.duplicate().resetReaderIndex();
		for(int i = 0; i < lineNumber; i++) {
			eol = findEndOfLine(b);
			if(eol==-1) return null;
			start = eol+1;
		}
		return b.slice(start, eol).toString(UTF8);
	}
	
	
	/**
     * Returns the index in the buffer of the end of line found.
     * Returns -1 if no end of line was found in the buffer.
     */
    private static int findEndOfLine(final ByteBuf buffer) {
        int i = buffer.forEachByte(ByteProcessor.FIND_LF);
        if (i > 0 && buffer.getByte(i - 1) == '\r') {
            i--;
        }
        return i;
    }	

	/**
	 * {@inheritDoc}
	 * @see org.codehaus.groovy.control.io.ReaderSource#cleanup()
	 */
	@Override
	public void cleanup() {
		/* No Op */
	}

	/**
	 * {@inheritDoc}
	 * @see org.codehaus.groovy.control.io.ReaderSource#getURI()
	 */
	@Override
	public URI getURI() {
		return sourceFile.toURI();
	}

}
