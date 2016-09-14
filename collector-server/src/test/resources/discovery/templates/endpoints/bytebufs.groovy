
/*
	ByteBuf Manager Collection Script
	Whitehead, 2016
*/

/* =========================================================================
	Populate one timers
   ========================================================================= */	
@Field
hostTag = host;
@Field
appTag = app;

@Field
bufferManager = jmxHelper.objectName("com.heliosapm.streams.buffers:service=BufferManager");
@Field
byteBuffPattern = null;
@Field
byteBuffAttributes = ["Allocations","TinyAllocations","SmallAllocations","NormalAllocations","HugeAllocations","Deallocations","TinyDeallocations","SmallDeallocations","NormalDeallocations","HugeDeallocations","ActiveAllocations","ActiveTinyAllocations","ActiveSmallAllocations","ActiveNormalAllocations","ActiveHugeAllocations","ChunkLists","SmallSubPages","TinySubPages","TotalChunkSize","ChunkFreeBytes","ChunkUsedBytes","ChunkUsage"] as String[];
@Field
allocType = null;


if(byteBuffPattern==null) {
	log.info("Initializing...");
	allocType = jmxHelper.getAttribute(jmxClient, bufferManager, "DirectBuffers") ? "Direct" : "Heap";
	log.info("Alloc Type: {}", allocType);
	byteBuffPattern = jmxHelper.objectName("com.heliosapm.streams.buffers:service=BufferManagerStats,type=$allocType");
	log.info("ON: {}", byteBuffPattern); 
}

tracer.reset().tags([host : hostTag, app : appTag, service : "bytebufs", type : allocType]);

tracer {
	try {									
		attributeValues = jmxHelper.getAttributes(byteBuffPattern, jmxClient, byteBuffAttributes);
		ts = System.currentTimeMillis();
		attributeValues.each() { k, v ->
			tracer.pushSeg(k).trace(v, ts).popSeg();
			log.info("ByteBuff Stat: [{}]: {}", k, v);
		}
	} catch (x) {
		log.error("Failed to collect on [{}]", on, x);					
	}				
}

tracer.flush();

log.info("Completed");



