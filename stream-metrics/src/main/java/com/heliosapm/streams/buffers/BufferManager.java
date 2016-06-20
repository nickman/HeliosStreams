/**
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */
package com.heliosapm.streams.buffers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.management.ManagementFactory;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: BufferManager</p>
 * <p>Description: Manages and monitors buffer allocation</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.buffers.BufferManager</code></p>
 */

public class BufferManager implements BufferManagerMBean, ByteBufAllocator {
	/** The singleton instance */
	private static volatile BufferManager instance = null;
	/** The singleton instance ctor lock */
	private static final Object lock = new Object();
	
	/** The pooled buffer allocator default number of heap arenas */
	public static final int DEFAULT_NUM_HEAP_ARENA = PooledByteBufAllocator.defaultNumHeapArena();
	/** The pooled buffer allocator default number of direct arenas */
	public static final int DEFAULT_NUM_DIRECT_ARENA = PooledByteBufAllocator.defaultNumDirectArena();
	/** The pooled buffer allocator default page size */
	public static final int DEFAULT_PAGE_SIZE = PooledByteBufAllocator.defaultPageSize();
	/** The pooled buffer allocator default max order */
	public static final int DEFAULT_MAX_ORDER = PooledByteBufAllocator.defaultMaxOrder();
	/** The pooled buffer allocator default tiny buffer cache size */
	public static final int DEFAULT_TINY_CACHE_SIZE = PooledByteBufAllocator.defaultTinyCacheSize();
	/** The pooled buffer allocator default small buffer cache size */
	public static final int DEFAULT_SMALL_CACHE_SIZE = PooledByteBufAllocator.defaultSmallCacheSize();
	/** The pooled buffer allocator default normal buffer cache size */
	public static final int DEFAULT_NORMAL_CACHE_SIZE = PooledByteBufAllocator.defaultNormalCacheSize();
	

	/** Indicates if we're using pooled or unpooled byteBuffs in the child channels */
	protected final boolean pooledBuffers;
	/** Indicates if we prefer using direct byteBuffs in the child channels */
	protected final boolean directBuffers;
	
	/** The number of pooled buffer heap arenas */
	protected final int nHeapArena;
	/** The number of pooled buffer direct arenas */
	protected final int nDirectArena;
	/** The pooled buffer page size */
	protected final int pageSize;
	/** The pooled buffer max order */
	protected final int maxOrder;
	/** The pooled buffer cache size for tiny allocations */
	protected final int tinyCacheSize;
	/** The pooled buffer cache size for small allocations */
	protected final int smallCacheSize;
	/** The pooled buffer cache size for normal allocations */
	protected final int normalCacheSize;	
	/** Instance logger */
	private final Logger log = LoggerFactory.getLogger(getClass());
	/** The pooled buffer allocator */
	protected final PooledByteBufAllocator pooledBufferAllocator;
	/** The child channel buffer allocator, which will be the same instance as the pooled allocator if pooling is enabled */
	protected final ByteBufAllocator childChannelBufferAllocator;
	/** The JMX ObjectName for the BufferManager's MBean */
	protected ObjectName objectName;
	
	/** The buffer arena monitor for direct buffers */
	protected final BufferArenaMonitor directMonitor;
	/** The buffer arena monitor for heap buffers */
	protected final BufferArenaMonitor heapMonitor;
	
	
	/**
	 * Acquires and returns the BufferManager singleton instance
	 * @return the BufferManager
	 */
	public static BufferManager getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {
					instance = new BufferManager();
				}
			}
		}
		return instance;
	}
	
	
	/**
	 * Creates a new BufferManager
	 */
	private BufferManager() {
		pooledBuffers = getBoolean("buffers.pooled", true);
		directBuffers = getBoolean("buffers.direct", true);
		nHeapArena = getInt("buffers.heaparenas", DEFAULT_NUM_HEAP_ARENA);
		nDirectArena = getInt("buffers.directarenas", DEFAULT_NUM_DIRECT_ARENA);
		pageSize = getInt("buffers.pagesize", DEFAULT_PAGE_SIZE);
		maxOrder = getInt("buffers.maxorder", DEFAULT_MAX_ORDER);
		tinyCacheSize = getInt("buffers.tcachesize", DEFAULT_TINY_CACHE_SIZE);
		smallCacheSize = getInt("buffers.scachesize", DEFAULT_SMALL_CACHE_SIZE);
		normalCacheSize = getInt("buffers.ncachesize", DEFAULT_NORMAL_CACHE_SIZE);			
		pooledBufferAllocator = new PooledByteBufAllocator(directBuffers, nHeapArena, nDirectArena, pageSize, maxOrder, tinyCacheSize, smallCacheSize, normalCacheSize);
		if(pooledBuffers) {
			childChannelBufferAllocator = pooledBufferAllocator;
		} else {
			childChannelBufferAllocator = new UnpooledByteBufAllocator(directBuffers);
		}		
		try {
			objectName = new ObjectName(OBJECT_NAME);
			ManagementFactory.getPlatformMBeanServer().registerMBean(this, objectName);
			log.info("Registered BufferManager management interface: [{}]", objectName);
		} catch (Exception ex) {
			log.warn("Failed to register the BufferManager management interface. Continuing without.", ex);
		}
		directMonitor = new BufferArenaMonitor(pooledBufferAllocator, true);
		heapMonitor = new BufferArenaMonitor(pooledBufferAllocator, false);
		log.info("Created BufferManager. Pooled: [{}], Direct:[{}]", pooledBuffers, directBuffers);
	}

	public static boolean getBoolean(final String key, final boolean defaultValue) {
		final String v = System.getProperty(key);
		return v==null ? defaultValue : "true".equalsIgnoreCase(v.trim());
	}
	
	public static int getInt(final String key, final int defaultValue) {
		final String v = System.getProperty(key);
		try {
			return Integer.parseInt(v.trim());
		} catch (Exception x) {
			return defaultValue;
		}
	}
	
	/**
	 * Returns the child channel buffer allocator
	 * @return the child channel buffer allocator
	 */
	public ByteBufAllocator getChildChannelBufferAllocator() {
		return childChannelBufferAllocator;
	}
	

	@Override
	public boolean isPooledBuffers() {
		return pooledBuffers;
	}

	@Override
	public boolean isDirectBuffers() {
		return directBuffers;
	}

	@Override
	public int getHeapArenas() {
		return nHeapArena;
	}

	@Override
	public int getDirectArenas() {
		return nDirectArena;
	}

	@Override
	public int getPageSize() {
		return pageSize;
	}

	@Override
	public int getMaxOrder() {
		return maxOrder;
	}

	@Override
	public int getTinyCacheSize() {
		return tinyCacheSize;
	}

	@Override
	public int getSmallCacheSize() {
		return smallCacheSize;
	}

	@Override
	public int getNormalCacheSize() {
		return normalCacheSize;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.opentsdb.buffers.BufferManagerMBean#printStats()
	 */
	@Override
	public String printStats() {
		final StringBuilder b = new StringBuilder("\n===================== ByteBuf Statistics ===================== ");
		b.append("\n\tDirectArenas\n");
		for(PoolArenaMetric pam: pooledBufferAllocator.directArenas()) {
			b.append(pam.toString());
		}
		log.info(b.toString());
		return b.toString();
	}
	

	/**
	 * Returns the server buffer allocator for child channels
	 * @return the server buffer allocator for child channels
	 */
	public PooledByteBufAllocator getPooledBufferAllocator() {
		return pooledBufferAllocator;
	}

  /**
   * Allocate a {@link ByteBuf}. If it is a direct or heap buffer
   * depends on the actual implementation.
   * @return The allocated ByteBuff
   * @see io.netty.buffer.ByteBufAllocator#buffer()
   */	
	public ByteBuf buffer() {
		return childChannelBufferAllocator.buffer();
	}

  /**
   * Allocate a {@link ByteBuf}. If it is a direct or heap buffer
   * depends on the actual implementation.
   * @param initialCapacity The initial capacity of the allocated buffer in bytes
   * @return The allocated ByteBuff
   * @see io.netty.buffer.ByteBufAllocator#buffer(int)
   */	
	public ByteBuf buffer(final int initialCapacity) {
		return childChannelBufferAllocator.buffer(initialCapacity);
	}

  /**
   * Allocate a {@link ByteBuf}. If it is a direct or heap buffer
   * depends on the actual implementation.
   * @param initialCapacity The initial capacity of the allocated buffer in bytes
   * @param maxCapacity The maximum capacity of the allocated buffer in bytes
   * @return The allocated ByteBuff
   * @see io.netty.buffer.ByteBufAllocator#buffer(int, int)
   */	
	public ByteBuf buffer(final int initialCapacity, final int maxCapacity) {
		return childChannelBufferAllocator.buffer(initialCapacity, maxCapacity);
	}

  /**
   * Allocate a {@link ByteBuf} suitable for IO, preferably a direct buffer./
   * @return The allocated ByteBuff
   * @see io.netty.buffer.ByteBufAllocator#ioBuffer(int, int)
   */	
	public ByteBuf ioBuffer() {
		return childChannelBufferAllocator.ioBuffer();
	}

  /**
   * Allocate a {@link ByteBuf} suitable for IO, preferably a direct buffer./
   * @param initialCapacity The initial capacity of the allocated buffer in bytes
   * @return The allocated ByteBuff
   * @see io.netty.buffer.ByteBufAllocator#ioBuffer(int)
   */	
	public ByteBuf ioBuffer(final int initialCapacity) {
		return childChannelBufferAllocator.ioBuffer(initialCapacity);
	}

  /**
   * Allocate a {@link ByteBuf} suitable for IO, preferably a direct buffer./
   * @param initialCapacity The initial capacity of the allocated buffer in bytes
   * @param maxCapacity The maximum capacity of the allocated buffer in bytes
   * @return The allocated ByteBuff
   * @see io.netty.buffer.ByteBufAllocator#ioBuffer(int, int)
   */	
	public ByteBuf ioBuffer(final int initialCapacity, final int maxCapacity) {
		return childChannelBufferAllocator.ioBuffer(initialCapacity, maxCapacity);
	}

	/**
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#heapBuffer()
	 */
	public ByteBuf heapBuffer() {
		return childChannelBufferAllocator.heapBuffer();
	}

	/**
	 * @param initialCapacity
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#heapBuffer(int)
	 */
	public ByteBuf heapBuffer(int initialCapacity) {
		return childChannelBufferAllocator.heapBuffer(initialCapacity);
	}

	/**
	 * @param initialCapacity
	 * @param maxCapacity
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#heapBuffer(int, int)
	 */
	public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
		return childChannelBufferAllocator.heapBuffer(initialCapacity, maxCapacity);
	}

	/**
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#directBuffer()
	 */
	public ByteBuf directBuffer() {
		return childChannelBufferAllocator.directBuffer();
	}

	/**
	 * @param initialCapacity
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#directBuffer(int)
	 */
	public ByteBuf directBuffer(int initialCapacity) {
		return childChannelBufferAllocator.directBuffer(initialCapacity);
	}

	/**
	 * @param initialCapacity
	 * @param maxCapacity
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#directBuffer(int, int)
	 */
	public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
		return childChannelBufferAllocator.directBuffer(initialCapacity, maxCapacity);
	}

	/**
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#compositeBuffer()
	 */
	public CompositeByteBuf compositeBuffer() {
		return childChannelBufferAllocator.compositeBuffer();
	}

	/**
	 * @param maxNumComponents
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#compositeBuffer(int)
	 */
	public CompositeByteBuf compositeBuffer(int maxNumComponents) {
		return childChannelBufferAllocator.compositeBuffer(maxNumComponents);
	}

	/**
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#compositeHeapBuffer()
	 */
	public CompositeByteBuf compositeHeapBuffer() {
		return childChannelBufferAllocator.compositeHeapBuffer();
	}

	/**
	 * @param maxNumComponents
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#compositeHeapBuffer(int)
	 */
	public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
		return childChannelBufferAllocator.compositeHeapBuffer(maxNumComponents);
	}

	/**
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#compositeDirectBuffer()
	 */
	public CompositeByteBuf compositeDirectBuffer() {
		return childChannelBufferAllocator.compositeDirectBuffer();
	}

	/**
	 * @param maxNumComponents
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#compositeDirectBuffer(int)
	 */
	public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
		return childChannelBufferAllocator.compositeDirectBuffer(maxNumComponents);
	}

	/**
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#isDirectBufferPooled()
	 */
	public boolean isDirectBufferPooled() {
		return childChannelBufferAllocator.isDirectBufferPooled();
	}

	/**
	 * @param minNewCapacity
	 * @param maxCapacity
	 * @return
	 * @see io.netty.buffer.ByteBufAllocator#calculateNewCapacity(int, int)
	 */
	public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
		return childChannelBufferAllocator.calculateNewCapacity(minNewCapacity, maxCapacity);
	}
	
	
	/**
	 * Reads a UTF string from the passed ByteBuff
	 * @param in The ByteBuf to read from
	 * @return the read string
	 */
	public final static String readUTF(final ByteBuf in) {
		try {
	        int utflen = in.readUnsignedShort();
	        byte[] bytearr = new byte[utflen];
	        char[] chararr = new char[utflen];
	        int c, char2, char3;
	        int count = 0;
	        int chararr_count=0;
	        in.readBytes(bytearr, 0, utflen);
	
	        while (count < utflen) {
	            c = bytearr[count] & 0xff;
	            if (c > 127) break;
	            count++;
	            chararr[chararr_count++]=(char)c;
	        }
	
	        while (count < utflen) {
	            c = bytearr[count] & 0xff;
	            switch (c >> 4) {
	                case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
	                    /* 0xxxxxxx*/
	                    count++;
	                    chararr[chararr_count++]=(char)c;
	                    break;
	                case 12: case 13:
	                    /* 110x xxxx   10xx xxxx*/
	                    count += 2;
	                    if (count > utflen)
	                        throw new UTFDataFormatException(
	                            "malformed input: partial character at end");
	                    char2 = bytearr[count-1];
	                    if ((char2 & 0xC0) != 0x80)
	                        throw new UTFDataFormatException(
	                            "malformed input around byte " + count);
	                    chararr[chararr_count++]=(char)(((c & 0x1F) << 6) |
	                                                    (char2 & 0x3F));
	                    break;
	                case 14:
	                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
	                    count += 3;
	                    if (count > utflen)
	                        throw new UTFDataFormatException(
	                            "malformed input: partial character at end");
	                    char2 = bytearr[count-2];
	                    char3 = bytearr[count-1];
	                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
	                        throw new UTFDataFormatException(
	                            "malformed input around byte " + (count-1));
	                    chararr[chararr_count++]=(char)(((c     & 0x0F) << 12) |
	                                                    ((char2 & 0x3F) << 6)  |
	                                                    ((char3 & 0x3F) << 0));
	                    break;
	                default:
	                    /* 10xx xxxx,  1111 xxxx */
	                    throw new UTFDataFormatException(
	                        "malformed input around byte " + count);
	            }
	        }
	        // The number of chars produced may be less than utflen
	        return new String(chararr, 0, chararr_count);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
    }

	
	
    /**
     * Writes a UTF string to the passed ByteBuff
     * @param str The string to write
     * @param out The ByteBuf to write to
     * @return the number of bytes written
     */
    public static int writeUTF(final String str, final ByteBuf out) {
    	try { 
	        int strlen = str.length();
	        int utflen = 0;
	        int c, count = 0;
	
	        /* use charAt instead of copying String to char array */
	        for (int i = 0; i < strlen; i++) {
	            c = str.charAt(i);
	            if ((c >= 0x0001) && (c <= 0x007F)) {
	                utflen++;
	            } else if (c > 0x07FF) {
	                utflen += 3;
	            } else {
	                utflen += 2;
	            }
	        }
	
	        if (utflen > 65535)
	            throw new UTFDataFormatException(
	                "encoded string too long: " + utflen + " bytes");
	
	        byte[] bytearr = new byte[utflen+2];
	
	        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
	        bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);
	
	        int i=0;
	        for (i=0; i<strlen; i++) {
	           c = str.charAt(i);
	           if (!((c >= 0x0001) && (c <= 0x007F))) break;
	           bytearr[count++] = (byte) c;
	        }
	
	        for (;i < strlen; i++){
	            c = str.charAt(i);
	            if ((c >= 0x0001) && (c <= 0x007F)) {
	                bytearr[count++] = (byte) c;
	
	            } else if (c > 0x07FF) {
	                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
	                bytearr[count++] = (byte) (0x80 | ((c >>  6) & 0x3F));
	                bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
	            } else {
	                bytearr[count++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
	                bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
	            }
	        }
	        
	        out.writeBytes(bytearr, 0, utflen+2);
	        return utflen + 2;
    	} catch (Exception ex) {
    		throw new RuntimeException(ex);
    	}
    }

	
}
