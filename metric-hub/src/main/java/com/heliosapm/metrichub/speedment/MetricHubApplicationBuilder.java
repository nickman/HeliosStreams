package com.heliosapm.metrichub.speedment;

import java.util.Optional;

import com.heliosapm.metrichub.speedment.generated.GeneratedMetricHubApplicationBuilder;
import com.heliosapm.metrichub.speedment.tsdb.public_.tsd_fqn_tagpair.TsdFqnTagpairManager;
import com.heliosapm.metrichub.speedment.tsdb.public_.tsd_metric.TsdMetricManager;
import com.heliosapm.metrichub.speedment.tsdb.public_.tsd_tagk.TsdTagkManager;
import com.heliosapm.metrichub.speedment.tsdb.public_.tsd_tagpair.TsdTagpairManager;
import com.heliosapm.metrichub.speedment.tsdb.public_.tsd_tagv.TsdTagvManager;
import com.heliosapm.metrichub.speedment.tsdb.public_.tsd_tsmeta.TsdTsmeta;
import com.heliosapm.metrichub.speedment.tsdb.public_.tsd_tsmeta.TsdTsmetaManager;

/**
 * The default {@link com.speedment.runtime.core.ApplicationBuilder}
 * implementation class for the {@link com.speedment.runtime.config.Project}
 * named metric-hub.
 * <p>
 * This file is safe to edit. It will not be overwritten by the code generator.
 * 
 * @author Helios APM
 */
public final class MetricHubApplicationBuilder extends GeneratedMetricHubApplicationBuilder {
    
	public static void main(String[] args) {
		log("MetricHubApplication Test");
		final MetricHubApplication app = new MetricHubApplicationBuilder()
			    .withPassword("tsdb")
			    .build();		
		
		
		TsdTsmetaManager tsMetaManager = app.getOrThrow(TsdTsmetaManager.class);
		TsdMetricManager metricManager = app.getOrThrow(TsdMetricManager.class);
		TsdTagkManager tagKManager = app.getOrThrow(TsdTagkManager.class);
		TsdTagvManager tagVManager = app.getOrThrow(TsdTagvManager.class);
		TsdTagpairManager tagPairManager = app.getOrThrow(TsdTagpairManager.class);
		TsdFqnTagpairManager fqnTagPairManager = app.getOrThrow(TsdFqnTagpairManager.class);
		
		log("App Ready: [%s]", app);
		
		Optional<TsdTsmeta> tsMeta;
		
	}
	
	public static void log(final Object fmt, final Object...args) {
		System.out.println(String.format(fmt.toString(), args));
	}
    
}