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

import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.Janitor;
import org.codehaus.groovy.control.io.ReaderSource;

import com.heliosapm.streams.buffers.BufferManager;
import com.heliosapm.streams.collector.cache.GlobalCacheService;
import com.heliosapm.utils.collections.Props;
import com.heliosapm.utils.enums.Primitive;
import com.heliosapm.utils.lang.StringHelper;
import com.heliosapm.utils.url.URLHelper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.ByteProcessor;

/**
 * <p>Title: ByteBufReaderSource</p>
 * <p>Description: A functional groovy source code container wrapping and enhancing the original source file</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.ByteBufReaderSource</code></p>
 */

public class ByteBufReaderSource implements ReaderSource {
	/** The original source file  */
	final File sourceFile;
	/** The class name that is expected to be compiled from this source */
	final String className;
	/** The byte buf containing the original source */
	final ByteBuf sourceBuffer;
	/** The byte buf containing the prejected source */
	final ByteBuf prejectedBuffer;
	/** Indicates if any preject code was generated */
	final boolean prejected;
	/** The bindings map */
	final Map<String, Object> bindingMap = new LinkedHashMap<String, Object>();
	/** The merged local and linked script properties */
	final Properties scriptProperties;
	/** A reference to the global cache service */
	final GlobalCacheService cache = GlobalCacheService.getInstance(); 
	
	/** The URI for this reader */
	final URI sourceURI;
	
	
	
	/** Static class logger */
	private static final Logger log = LogManager.getLogger(ManagedScriptFactory.class);	
	/** The UTF character set */
	public static final Charset UTF8 = Charset.forName("UTF8");
	/** Cache injection substitution pattern */
	public static final Pattern CACHE_PATTERN = Pattern.compile("\\$cache\\{(.*?)(?::(\\d+))?(?::(nanoseconds|microseconds|milliseconds|seconds|minutes|hours|days))??\\}");
	/** Typed value substitution pattern */
	public static final Pattern TYPED_PATTERN = Pattern.compile("\\$typed\\{(.*?):(.*)\\}");
	/** Injected field template */
	public static final String INJECT_TEMPLATE = "@Dependency(value=\"%s\", timeout=%s, unit=%s) def %s;"; 
	/** The platform end of line character */
	public static final String EOL = System.getProperty("line.separator");
	

	/**
	 * Creates a new ByteBufReaderSource
	 * @param sourceFile the original source file
	 * @param scriptRootDirectory The script root directory
	 */
	public ByteBufReaderSource(final File sourceFile, final Path scriptRootDirectory) {
		this.sourceFile = sourceFile;		
    	final Path sourcePath = sourceFile.getAbsoluteFile().toPath().normalize();
    	className = scriptRootDirectory.normalize().relativize(sourcePath).toString().replace(".groovy", "").replace(File.separatorChar, '.');
		scriptProperties = readConfig(sourceFile);
		sourceBuffer = loadSourceFile();
		
		final StringBuilder b = processDependencies();
		if(b.length()==0) {
			prejectedBuffer = sourceBuffer;
			prejected = false;
			sourceURI = sourceFile.toURI();
		} else {
			b.insert(0, EOL + "// ===== Injected Code =====" + EOL);
			b.append(EOL + "// =========================" + EOL);
			prejectedBuffer = BufferManager.getInstance().directBuffer(b.length() + sourceBuffer.readableBytes());
			prejectedBuffer.writeCharSequence(b, UTF8);
			prejectedBuffer.writeBytes(sourceBuffer.duplicate().resetReaderIndex());
			prejected = true;
			sourceURI = URLHelper.toURI(sourceFile.toURI().toString() + "-prejected");
		}
	}
	

	/**
	 * Loads the source file into a ByteBuf and returns it as a read only buffer
	 * @return the loaded ByteBuf
	 */
	protected ByteBuf loadSourceFile() {
		ByteBuf buf = BufferManager.getInstance().directBuffer((int)this.sourceFile.length());
		FileReader fr = null;
		BufferedReader br = null;
		String line = null;
		try {
			fr = new FileReader(sourceFile);
			br = new BufferedReader(fr);
			while((line = br.readLine())!=null) {
				final String enrichedLine = StringHelper.resolveTokens(line, scriptProperties);
				buf.writeCharSequence(enrichedLine + EOL, UTF8);
			}
			return buf.asReadOnly();
		} catch (Exception ex) {
			throw new RuntimeException("Failed to read source from source file [" + sourceFile + "]", ex);
		} finally {
			if(br!=null) try { br.close(); } catch (Exception x) {/* No Op */}
			if(fr!=null) try { fr.close(); } catch (Exception x) {/* No Op */}
		}		
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.codehaus.groovy.control.io.ReaderSource#getReader()
	 */
	@Override
	public Reader getReader() throws IOException {
		final InputStream is = new ByteBufInputStream(prejectedBuffer.duplicate());
		return new InputStreamReader(is, UTF8); 
	}
	
	/**
	 * Releases the source buffer
	 */
	public void close() {
		try { sourceBuffer.release(); } catch (Exception x) {/* No Op */}
		if(prejected) try { prejectedBuffer.release(); } catch (Exception x) {/* No Op */}
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
		final ByteBuf b = prejectedBuffer.duplicate().resetReaderIndex();
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
		return sourceURI;
	}
	
	/**
	 * Processes any found dependencies and returns the prejected code
	 * @return the [possibly empty] prejected code
	 */
	protected StringBuilder processDependencies() {
		final StringBuilder b = new StringBuilder();
		final Properties p = readConfig(sourceFile);
		if(p.isEmpty()) return b;
		for(final String key: p.stringPropertyNames()) {
			final String value = p.getProperty(key, "").trim();
			if(value.isEmpty()) continue;
			try {				
				final Matcher m = CACHE_PATTERN.matcher(value);
				if(m.matches()) {
					final String t = m.group(2);
					final String u = m.group(3);
				    final String cacheKey = m.group(1).trim();
				    final long timeout = (t==null || t.trim().isEmpty()) ? 0 : Long.parseLong(t.trim());			    
				    final TimeUnit unit = (u==null || u.trim().isEmpty()) ? TimeUnit.HOURS : TimeUnit.valueOf(u.trim().toUpperCase());
				    b.append(String.format(INJECT_TEMPLATE, cacheKey, timeout, unit, key)).append(EOL);
				    continue;
				}
			} catch (Exception ex) {
				log.warn("Failed to process injected property [{}]:[{}]", key, value, ex);
				continue;
			}
			try {				
				final Matcher m = TYPED_PATTERN.matcher(value);
				if(m.matches()) {
					final String type = m.group(1);
					final String val = m.group(2);
					if(type==null || type.trim().isEmpty() || val==null || val.trim().isEmpty()) continue;
				    if(Primitive.ALL_CLASS_NAMES.contains(type.trim())) {
				    	Class<?> clazz = Primitive.PRIMNAME2PRIMCLASS.get(type.trim());
				    	PropertyEditor pe = PropertyEditorManager.findEditor(clazz);
				    	pe.setAsText(val.trim());
				    	bindingMap.put(key, pe.getValue());
				    	continue;
				    }
				}
			} catch (Exception ex) {
				log.warn("Failed to process injected property [{}]:[{}]", key, value, ex);
				continue;
			}
		}
		return b;
	}
	
	/** Regex pattern to match a schedule directive built into the source file name */
	public static final Pattern PERIOD_PATTERN = Pattern.compile("\\-(\\d++[s|m|h|d]){1}\\.groovy$", Pattern.CASE_INSENSITIVE);
	
	
	/**
	 * Groovy source files may have an accompanying config properties file which by convention is
	 * the same file, except with an extension of <b><code>.properties</code></b> instead of <b><code>.groovy</code></b>.
	 * Groovy source files may also be symbolic links to a template, which in turn may have it's own configuration properties, similarly named.
	 * This method attempts to read from both, merges the output with the local config overriding the template's config
	 * and returns the result.
	 * @param sourceFile this script's source file
	 * @return the merged properties
	 */
	protected static Properties readConfig(final File sourceFile) {
		final Path sourcePath = sourceFile.toPath();
		final Properties p = new Properties();
		try {
			final Path linkedSourcePath = sourceFile.toPath().toRealPath();
			if(!linkedSourcePath.equals(sourcePath)) {
				final File linkedProps = new File(linkedSourcePath.toFile().getAbsolutePath().replace(".groovy", ".properties"));
				if(linkedProps.canRead()) {
					final Properties linkedProperties = Props.strToProps(StringHelper.resolveTokens(URLHelper.getStrBuffFromURL(URLHelper.toURL(linkedProps))), UTF8);
					p.putAll(linkedProperties);
				}
			}
		} catch (Exception x) {/* No Op */} 
		final File exactMatchFileProps = new File(sourcePath.toFile().getAbsolutePath().replace(".groovy", ".properties"));
		final File noSchedFileProps = new File(sourcePath.toFile().getAbsolutePath().replace(sourceFile.getName(), PERIOD_PATTERN.matcher(sourceFile.getName()).replaceAll(".properties")));
		if(noSchedFileProps.canRead()) {
			final Properties rp = Props.strToProps(StringHelper.resolveTokens(URLHelper.getStrBuffFromURL(URLHelper.toURL(noSchedFileProps))), UTF8);
			p.putAll(rp);
		}
		
		if(exactMatchFileProps.canRead()) {
			final Properties localProperties = Props.strToProps(StringHelper.resolveTokens(URLHelper.getStrBuffFromURL(URLHelper.toURL(exactMatchFileProps))), UTF8);
			p.putAll(localProperties);
		}
		
		return p;
	}
	
	
	/**
	 * Returns the original source, modified only for inline property tokens
	 * @return the original source
	 */
	public String getOriginalSource() {
		return sourceBuffer.toString(UTF8);
	}
	
	/**
	 * Returns the prejected source if prejection was performed, 
	 * otherwise returns the original source, modified only for inline property tokens
	 * @return the prejected or original source
	 */
	public String getPrejectedSource() {
		return prejectedBuffer.toString(UTF8);
	}
	


	/**
	 * Returns the original source file
	 * @return the original source file
	 */
	public File getSourceFile() {
		return sourceFile;
	}


	/**
	 * Indicates if the original code was modified for prejection
	 * @return true if the original code was modified for prejection, false otherwise
	 */
	public boolean isPrejected() {
		return prejected;
	}


	/**
	 * Returns the script binding map
	 * @return the script binding map
	 */
	public Map<String, Object> getBindingMap() {
		return bindingMap;
	}


	/**
	 * Returns the script initialization properties
	 * @return the script initialization properties
	 */
	public Properties getScriptProperties() {
		return scriptProperties;
	}


	/**
	 * Returns the class name that is expected to be compiled from this source
	 * @return the class name
	 */
	public String getClassName() {
		return className;
	}
	
	

}
