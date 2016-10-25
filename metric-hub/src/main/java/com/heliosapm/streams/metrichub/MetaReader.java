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
package com.heliosapm.streams.metrichub;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;

import com.heliosapm.streams.metrichub.impl.IndexProvidingIterator;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId.UniqueIdType;



/**
 * <p>Title: MetaReader</p>
 * <p>Description: Defines a class that can instantiate OpenTSDB meta objects from a result set</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.metrichub.impl.MetaReader</code></p>
 */

public interface MetaReader {

	/**
	 * Returns a collection of {@link UIDMeta}s read from the passed {@link ResultSet}.
	 * @param rset The result set to read from
	 * @param uidType The UIDMeta type name we're reading
	 * @return a [possibly empty] collection of UIDMetas
	 */
	public List<UIDMeta> readUIDMetas(ResultSet rset, String uidType);
	
	
	/**
	 * Returns a collection of {@link UIDMeta}s read from the passed {@link ResultSet}.
	 * @param rset The result set to read from
	 * @param uidType The UIDMeta type we're reading
	 * @return a [possibly empty] collection of UIDMetas
	 */
	public List<UIDMeta> readUIDMetas(ResultSet rset, UniqueIdType uidType);
	
	/**
	 * Returns a collection of shallow (no UIDMetas for the metric or tags) {@link TSMeta}s read from the passed {@link ResultSet}.
	 * @param rset The result set to read from
	 * @return a [possibly empty] collection of TSMetas
	 */
	public List<TSMeta> readTSMetas(ResultSet rset);
	
	/**
	 * Returns a collection of {@link TSMeta}s read from the passed {@link ResultSet}.
	 * @param rset The result set to read from
	 * @param includeUIDs If true, the metric and tags will be loaded, otherwise shallow TSMetas will be returned
	 * @return a [possibly empty] collection of TSMetas
	 */
	public List<TSMeta> readTSMetas(ResultSet rset, boolean includeUIDs);
	
	/**
	 * Returns a TSMeta iterator for the passed result set (no UIDMetas for the metric or tags)
	 * @param rset The result set to read from
	 * @return a TSMeta iterator
	 */
	public IndexProvidingIterator<TSMeta> iterateTSMetas(ResultSet rset);
	
	/**
	 * Returns a TSMeta iterator for the passed result set 
	 * @param rset The result set to read from
	 * @param includeUIDs true to load UIDs, false otherwise
	 * @return the TSMeta iterator
	 */
	public IndexProvidingIterator<TSMeta> iterateTSMetas(ResultSet rset, boolean includeUIDs);
	
	
	/**
	 * Returns a UIDMeta iterator for the passed result set 
	 * @param rset The result set to read from
	 * @param uidType THe UID type to iterate 
	 * @return the UIDMeta iterator
	 */
	public IndexProvidingIterator<UIDMeta> iterateUIDMetas(ResultSet rset, UniqueIdType uidType);	
	
	
	/**
	 * Returns a collection of {@link Annotation}s read from the passed {@link ResultSet}.
	 * @param rset The result set to read from
	 * @return a [possibly empty] collection of Annotations
	 */
	public List<Annotation> readAnnotations(ResultSet rset);
	

}

