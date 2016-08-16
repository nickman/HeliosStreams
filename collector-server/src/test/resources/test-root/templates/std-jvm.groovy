
def jmxClient = JMXClient.newInstance(this, "$jmxurl");

@Field
perf = null;
@Field
long ticksPerSec = 0;
@Field
Closure ticksToNs = null;

if(ticksPerSec==0) {
	try {
		perf = _perf;
		ticksPerSec = perf.highResFrequency();
		ticksToNs = { t ->
		    return TimeUnit.SECONDS.toNanos(t) / ticksPerSec;
		}		
	} catch (x) {
		ticksPerSec = -1L;
	}
}

runtimeName = jmxClient.getAttribute(jmxHelper.MXBEAN_RUNTIME_ON, "Name");
log.info("JMX Remote Runtime Name: [$runtimeName]");

priorTime = get('priorTime', {
	return jmxHelper.getStartTime(jmxClient);
});
thisTime = jmxHelper.getStartTime(jmxClient);
if(thisTime!=priorTime) {
	log.error("StartTime Changed. Was [$priorTime] but is now [${thisTime}]. Flushing Cache");
	flushCache();
	resetDeltas();
}

	/* =========================================================================
		Populate one timers
	   ========================================================================= */	
	hostTag = get('hostTag', {
		return jmxClient.getHostName();
	});
	appTag = get('appTag', {
		return jmxClient.getAppName();
	});

	garbageCollectors = get('garbageCollectors', {		
		return jmxHelper.getGCMXBeans(jmxClient);
	});
	memoryPools = get('memoryPools', {		
		return jmxHelper.getMemPoolMXBeans(jmxClient);
	});
	poolMaxMems = get('poolMaxMems', {		
		return jmxHelper.getPoolMaxMems(jmxClient);
	});


	maxMems = get('maxMems', {		
		return jmxHelper.getMaxMems(jmxClient);
	});
	maxFileDescriptors = get('maxFileDescriptors', {		
		return jmxClient.getAttribute(jmxHelper.MXBEAN_OS_ON, "MaxFileDescriptorCount");
	});
	totalSwapSpaceSize = get('totalSwapSpaceSize', {		
		return jmxClient.getAttribute(jmxHelper.MXBEAN_OS_ON, "TotalSwapSpaceSize");
	});
	totalPhysicalMemory = get('totalPhysicalMemory', {		
		return jmxClient.getAttribute(jmxHelper.MXBEAN_OS_ON, "TotalPhysicalMemorySize");
	});
	cpuCount = get('cpuCount', {
		return jmxClient.getAttribute(jmxHelper.MXBEAN_OS_ON, "AvailableProcessors");
	});	

	hotSpotMBeans = get('hotSpotMBeans', {
		boolean registered = jmxHelper.registerHotspotInternal(jmxClient);
		log.info("HotSpot Registered for [$jmxurl]: $registered");
		return registered;
	});

// =======================================================================================

tracer.reset().tags([host : hostTag, app : appTag]);


tracer {
	tracer.pushSeg("jvm")


	/* trace and tracer are technically the same object, but
		that could change */

	/* =========================================================================
		Classloading Stats
	   ========================================================================= */	  
	tracer { 
		tracer.pushSeg("classloading");
		attrs = jmxHelper.getAttributes(jmxHelper.MXBEAN_CL_ON, jmxClient, "LoadedClassCount", "TotalLoadedClassCount", "UnloadedClassCount");
		ts = System.currentTimeMillis();
		attrs.each() {k,v -> 
			tracer.pushSeg(k).trace(v, ts).popSeg();
		}
		delta("loadrate", attrs.get("TotalLoadedClassCount"), { k, v ->					
			tracer.pushSeg(k).trace(v, ts).popSeg();
		});
		delta("unloadrate", attrs.get("UnloadedClassCount"), { k, v ->			
			tracer.pushSeg(k).trace(v, ts).popSeg();
		});
	}

	/* =========================================================================
		Base Memory Stats
	   ========================================================================= */	
	
	tracer { 
		tracer.pushSeg("memory");
		["Heap", "NonHeap"].eachWithIndex() { type, idx ->
			tracer {
				tracer.pushTag("type", type);
				//log.info("Tracing Mem Type: [$type]");
				jmxHelper.getAttribute(jmxHelper.MXBEAN_MEM_ON, jmxClient,  "${type}MemoryUsage").each() { usage ->						
					ts = System.currentTimeMillis();
					long comm = usage.get("committed");
					long used = usage.get("used");
					tracer.pushSeg("committed").trace(comm, ts).popSeg();
					tracer.pushSeg("used").trace(used, ts).popSeg();
					if(maxMems[idx]>0) {
						int commp = comm/maxMems[idx]*100;
						int usedp = used/maxMems[idx]*100;
						tracer.pushSeg("pctcommitted").trace(commp, ts).popSeg();
						tracer.pushSeg("pctused").trace(usedp, ts).popSeg();
					}
				}
				//log.info("Tracing Complete on Mem Type: [$type]");
			}
		}
		long pendingFin = jmxClient.getAttribute(jmxHelper.MXBEAN_MEM_ON, "ObjectPendingFinalizationCount");
		tracer.pushSeg("pendingfinalizers ").trace(pendingFin, ts).popSeg();
	}
	/* =========================================================================
		Threading Stats
	  ========================================================================= */		
	tracer { 
		tracer.pushSeg("threads");
		attrs = jmxHelper.getAttributes(jmxHelper.MXBEAN_THREADING_ON, jmxClient, "TotalStartedThreadCount","PeakThreadCount","ThreadCount","DaemonThreadCount");
		ts = System.currentTimeMillis();
		attrs.each() {k,v -> 
			String m = k.replace("Thread", "");
			tracer.pushSeg(m.toLowerCase()).trace(v, ts).popSeg();
		}
		tracer.pushSeg("nondaemoncount").trace(attrs.get('ThreadCount')-attrs.get('DaemonThreadCount'), ts).popSeg();
		jmxHelper.getThreadStateCounts(jmxClient).each() { state, cnt ->
			tracer.pushSeg("threadstate").pushTag("state", state).trace(cnt,ts).popSeg().popTag();
		}
	}
	/* =========================================================================
		OS Stats
	  ========================================================================= */	
	tracer {
		tracer.pushSeg("os");
		osnames = [
			"OpenFileDescriptorCount" : "openfiledescs",
			"CommittedVirtualMemorySize" : "commitedvirtualmem",
			"FreeSwapSpaceSize" : "freeSwapSpace",
			"ProcessCpuTime" : "processCpuTime",
			"FreePhysicalMemorySize" : "freePhysicalMem",
			"SystemCpuLoad" : "systemCpuLoad",					// Not On J6
			"ProcessCpuLoad" : "processCpuLoad",				// Not on J6
			"SystemLoadAverage" : "systemLoadAvg"
		];
		attrs = jmxHelper.getAttributes(jmxHelper.MXBEAN_OS_ON, jmxClient, osnames.keySet());
		log.info("OS ATTRS: {}", attrs);
		ts = System.currentTimeMillis();
		osnames.each() {k,v -> 
			if(attrs.containsKey(k)) {
				tracer.pushSeg(v).trace(attrs.get(k), ts).popSeg();
			}
			//println "java.lang.os.$v $ts ${attrs.get(k)} $hostTag $appTag".toLowerCase();		
		}
		tracer.pushSeg("usedswapspace").trace(totalSwapSpaceSize - attrs.get('FreeSwapSpaceSize'), ts).popSeg();
		tracer.pushSeg("pctusedswapspace").trace((totalSwapSpaceSize - attrs.get('FreeSwapSpaceSize'))/totalSwapSpaceSize*100, ts).popSeg();
		log.info("MAX FD: [{}], OPEN FD: [{}]", maxFileDescriptors, attrs.get('OpenFileDescriptorCount'));
		tracer.pushSeg("pctusedfiledescs").trace((maxFileDescriptors - attrs.get('OpenFileDescriptorCount'))/maxFileDescriptors*100, ts).popSeg();
		tracer.pushSeg("usedPhysicalMem").trace((totalPhysicalMemory - attrs.get('FreePhysicalMemorySize')), ts).popSeg();
		tracer.pushSeg("pctusedPhysicalMem").trace((totalPhysicalMemory - attrs.get('FreePhysicalMemorySize'))/totalPhysicalMemory*100, ts).popSeg();
		if(attrs.containsKey('ProcessCpuLoad') && attrs.containsKey('SystemCpuLoad')) {
			tracer.pushSeg("pctprocessload").trace((attrs.get('ProcessCpuLoad')/attrs.get('SystemCpuLoad'))*100, ts).popSeg();
		}
		//println "java.lang.os.usedswapspace $ts ${totalSwapSpaceSize - attrs.get('FreeSwapSpaceSize')} $hostTag $appTag".toLowerCase();		
		//println "java.lang.os.pctusedswapspace $ts ${(totalSwapSpaceSize - attrs.get('FreeSwapSpaceSize'))/totalSwapSpaceSize*100} $hostTag $appTag".toLowerCase();		
		//println "java.lang.os.pctusedfiledescs $ts ${(maxFileDescriptors - attrs.get('OpenFileDescriptorCount'))/maxFileDescriptors*100} $hostTag $appTag".toLowerCase();		
		//println "java.lang.os.usedPhysicalMem $ts ${(totalPhysicalMemory - attrs.get('FreePhysicalMemorySize'))} $hostTag $appTag".toLowerCase();		
		//println "java.lang.os.pctusedPhysicalMem $ts ${(totalPhysicalMemory - attrs.get('FreePhysicalMemorySize'))/totalPhysicalMemory*100} $hostTag $appTag".toLowerCase();		
		//println "java.lang.os.pctprocessload $ts ${(attrs.get('ProcessCpuLoad')/attrs.get('SystemCpuLoad'))*100} $hostTag $appTag".toLowerCase();	
		//Long elapsedTimeNanos = delta("${_jmx}-nanotime", System.currentTimeMillis());
		double nt = System.nanoTime();
		double pct = attrs.get("ProcessCpuTime").doubleValue();
		Double elapsedTimeNanos = delta("${_jmx}-nano-elapsed-time", (double)nt);
		Double elpasedCpuTimeNanos = delta("${_jmx}-nano-cpu-time", (double)pct);
		if(elapsedTimeNanos!=null && elpasedCpuTimeNanos !=null) {
			double totalCpuTime =  cpuCount * elapsedTimeNanos;
			
			//long totalCpuTime =  elapsedTimeNanos;
			double pctCpuUsage = (elpasedCpuTimeNanos/totalCpuTime)*100D;
			int p = (int)pctCpuUsage;
			// I don't understand what this number is .....
			tracer.pushSeg("pctcpu").trace(p, System.currentTimeMillis()).popSeg();
		}
	}
	/* =========================================================================
		Compilation Stats
	  ========================================================================= */		
	tracer { 
		deltaComp = delta("TotalCompilationTime", jmxHelper.getAttribute(jmxHelper.MXBEAN_COMP_ON, "TotalCompilationTime"));
		if(deltaComp!=null) {
			tracer.pushSeg("compiler.time").trace(deltaComp).popSeg();
		}
	}

	/* =========================================================================
		GC Stats
	  ========================================================================= */		
	tracer { 
		tracer.pushSeg("gc");
		double totalGcTime = 0;
		Long e = delta("${_jmx}-gcelapsedtime", System.currentTimeMillis());
		garbageCollectors.each() { gc ->
			attrs = jmxHelper.getAttributes(gc, jmxClient, "CollectionCount", "CollectionTime");
			ts = System.currentTimeMillis();
			String name = gc.getKeyProperty("name");
			String sname = name.replace(" ", "");
			Long c = delta("${sname}-collcnt", attrs.get('CollectionCount'));
			Long t = delta("${sname}-colltime", attrs.get('CollectionTime'));
			tracer {
				if(c!=null) {
					tracer {
						tracer.pushSeg("collcount").pushTag("gcname", sname).trace(c, ts).popTag().popSeg();
					}
				}
				if(t!=null) {
					totalGcTime += t;
					tracer {
						tracer.pushSeg("colltime").pushTag("gcname", sname).trace(t, ts).popTag().popSeg();
					}
				}

			}
		}
		if(e!=null) {
			double elapsedCpuTime = cpuCount * e;
			double gctime = totalGcTime/elapsedCpuTime*100D;
			tracer.pushSeg("pctcputimeingc").trace(gctime, ts).popSeg();
		}
	}

	/* =========================================================================
		HotSpot Internals
	  ========================================================================= */		

	if(hotSpotMBeans) {
		tracer.pushSeg("hotspot")
		// =========== Runtime Safepoints ===========
		tracer {

			Map safepoints = jmxHelper.getAttributes(jmxHelper.HOTSPOT_INTERNAL_RUNTIME, jmxClient, jmxHelper.HOTSPOT_RUNTIME_ATTRS);
			ts = System.currentTimeMillis();
			tracer.pushSeg("safepoint");
			safepoints.each() { k,v ->
				Long c = delta(k, v);
				if(c!=null) {
					tracer.pushSeg(k.toLowerCase()).trace(c).popSeg();
				}
			}
		}
		// =========== Class loading ===========
		tracer {

			Map clstats = jmxHelper.getAttributes(jmxHelper.HOTSPOT_INTERNAL_CLASSLOAD, jmxClient, jmxHelper.HOTSPOT_CLASSLOAD_ATTRS);
			ts = System.currentTimeMillis();
			tracer.pushSeg("classload");
			clstats.each() { k,v ->
				Long c = delta(k, v);
				if(c!=null) {
					tracer.pushSeg(k.replace("Class", "").replace("Initialization", "Init").replace("Initialized", "Inited").toLowerCase()).trace(c).popSeg();
				}
			}
		}
		// =========== Compilation ===========
		tracer {

			Map compstats = jmxHelper.getAttributes(jmxHelper.HOTSPOT_INTERNAL_COMPILATION, jmxClient, jmxHelper.HOTSPOT_COMPILATION_ATTRS);
			ts = System.currentTimeMillis();
			tracer.pushSeg("compiler");
			compstats.each() { k,v ->
				Long c = delta(k, v);
				if(c!=null) {
					tracer.pushSeg(k.replace("Compiler", "").replace("Compiled", "").replace("Compile", "").toLowerCase()).trace(c).popSeg();
				}
			}
		}

		// =========== Memory ===========
		tracer {

			List memstats = jmxHelper.getAttribute(jmxHelper.HOTSPOT_INTERNAL_MEMORY, jmxClient, "InternalMemoryCounters");
			ts = System.currentTimeMillis();
			tracer.pushSeg("gc");
			memstats.each() { counter ->
				String cname = counter.getName().toLowerCase();
				if(cname.startsWith("sun.gc.policy.")) {
					cname = cname.replace("sun.gc.policy.", "");
					String unitName = counter.getUnits().toString().toLowerCase();
					if(!"string".equals(unitName)) {
						if(!"ticks".equals(unitName)) {
							tracer.pushSeg(cname).pushTag("unit", unitName).trace(counter.getValue(), ts).popSeg().popTag();
						} else if(ticksPerSec!=-1) {
							tracer.pushSeg(cname).pushTag("unit", "ns").trace(ticksToNs(counter.getValue()), ts).popSeg().popTag();
						}
					}
				}
			}
		}

	}
}

try { log.info("Total Remoting Time: ${jmxClient.getJMXRemotingTime()} ms."); } catch (x) {}

