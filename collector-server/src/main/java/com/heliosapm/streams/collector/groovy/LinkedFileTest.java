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

import com.heliosapm.streams.collector.links.LinkFile;
import com.heliosapm.utils.file.FileFilterBuilder;
import com.heliosapm.utils.file.FileFinder;
import com.heliosapm.utils.file.Filters.FileMod;
import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: LinkedFileTest</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.groovy.LinkedFileTest</code></p>
 */

public class LinkedFileTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		log("LinkedFileTest");
		FileFinder linkedSourceFinder = FileFinder.newFileFinder("/tmp/links")
				.maxDepth(20)
				.filterBuilder()
					.linkedFile(
							FileFilterBuilder.newBuilder()
							.caseInsensitive(false)
							.endsWithMatch(".groovy")
							.fileAttributes(FileMod.READABLE)
							.shouldBeFile()
							.build()
					)
				.fileFinder();
		final File[] files = linkedSourceFinder.find();
		log("Found [" + files.length + "] files");
		for(File f: files) {
			log("File: " + f);
			final LinkFile lf = new LinkFile(f);
			log("\tPoints to: " + lf);
			log("\tContent:" + URLHelper.getTextFromFile(lf));
			
		}

	}
	
	public static void log(Object msg) {
		System.out.println(msg);
	}

}
