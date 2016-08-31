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
package com.heliosapm.streams.discovery;

import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.heliosapm.streams.json.JSONOps;

/**
 * <p>Title: JsonInstanceSerializer</p>
 * <p>Description: Replacement for curator's serializer which still uses codehaus jackson</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.discovery.JsonInstanceSerializer</code></p>
 */

/**
 * A serializer that uses Jackson to serialize/deserialize as JSON. IMPORTANT: The instance
 * payload must support Jackson
 * @param <T> The ServiceInstance payload type
 */
public class JsonInstanceSerializer<T> implements InstanceSerializer<T> {

    private final TypeReference<ServiceInstance<T>>  type = new TypeReference<ServiceInstance<T>>(){/* No Op */};


    /**
     * {@inheritDoc}
     * @see org.apache.curator.x.discovery.details.InstanceSerializer#deserialize(byte[])
     */    
    @Override
    public ServiceInstance<T> deserialize(final byte[] bytes) throws Exception {
    	return JSONOps.parseToObject(bytes, type);
    }

    /**
     * {@inheritDoc}
     * @see org.apache.curator.x.discovery.details.InstanceSerializer#serialize(org.apache.curator.x.discovery.ServiceInstance)
     */
    @Override
    public byte[] serialize(final ServiceInstance<T> instance) throws Exception {
        return JSONOps.serializeToBytes(instance);
    }
}
