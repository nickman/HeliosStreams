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
package com.heliosapm.streams.agent.endpoint;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.heliosapm.utils.enums.TimeUnitSymbol;

/**
 * <p>Title: Endpoint</p>
 * <p>Description: Defines an endpoint that indicates to an endpoint listener what resources are available for monitoring.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.endpoint.publisher.Endpoint</code></p>
 */
@JsonSerialize(using=Endpoint.EndpointSerializer.class)
@JsonDeserialize(using=Endpoint.EndpointDeserializer.class)
public class Endpoint {
	/** The endpoint name */
	private final String name;
	/** The endpoint polling period */
	private final long period;
	/** The endpoint polling period unit */
	private final TimeUnitSymbol unit;
	/** Custom processing indicator */
	private final String processor;
	
	/** Regex pattern to parse a endpoint string */
	public static final Pattern ENDPOINT_PATTERN = Pattern.compile("(.*?)(?:\\-(\\d++)([s|m|h|d]){1})?(?:/(.*))?$", Pattern.CASE_INSENSITIVE);
	
	

	
	/**
	 * Creates an endpoint from a string
	 * @param value The string to parse
	 * @return the endpoint
	 */
	public static Endpoint fromString(final String value) {
		if(value==null || value.trim().isEmpty()) throw new IllegalArgumentException("The passed value was null or empty");		
		final Matcher m = ENDPOINT_PATTERN.matcher(value.trim());
		if(!m.matches()) throw new IllegalArgumentException("The passed value [" + value + "] could not be parsed");
		final String name = m.group(1);
		final String period = m.group(2);
		final String unit = m.group(3);
		final String processor = m.group(4);
		if(unit!=null && !unit.isEmpty()) {
			return new Endpoint(name, Long.parseLong(period), TimeUnitSymbol.fromShortName(unit), (processor==null || processor.isEmpty()) ? null : processor);
		} else {
			return new Endpoint(name, -1L, null, (processor==null || processor.isEmpty()) ? null : processor);
		}
	}
	
	/**
	 * Creates an array of endpoints from an array of strings
	 * @param values The strings to parse
	 * @return the endpoints
	 */
	public static Endpoint[] fromStrings(final String... values) {
		final LinkedHashSet<Endpoint> endpoints = new LinkedHashSet<Endpoint>();
		for(String s: values) {
			if(s==null || s.trim().isEmpty()) continue;
			endpoints.add(fromString(s));
		}
		return endpoints.toArray(new Endpoint[endpoints.size()]);
		
	}
	
	
	/**
	 * Creates a new Endpoint
	 * @param name The mandatory name
	 * @param period An optional period (use <b><code>&lt; 1</code></b> and a null {@code unit} for no period)
	 * @param unit The optional period unit
	 * @param processor The optional processor name
	 */
	public Endpoint(final String name, final long period, final TimeUnitSymbol unit, final String processor) {
		if(name==null || name.trim().isEmpty()) throw new IllegalArgumentException("The passed name was null or empty");		
		this.name = name.trim();
		if(unit!=null) {
			if(period < 1L) throw new IllegalArgumentException("Invalid period [" + period + "] for unit [" + unit + "]");
		} else {
			if(period > 0L) throw new IllegalArgumentException("Invalid period [" + period + "] for unit [" + unit + "]");
		}		
		this.period = period;
		this.unit = unit;
		this.processor = (processor==null || processor.trim().isEmpty()) ? null : processor.trim();
	}
	
	/**
	 * Creates a new Endpoint
	 * @param name The mandatory name
	 * @param period An optional period (use <b><code>&lt; 1</code></b> or a null {@code unit} for no period)
	 * @param unit The optional period unit
	 */
	public Endpoint(final String name, final long period, final TimeUnitSymbol unit) {		
		this(name, period, unit, null);
	}
	
	/**
	 * Creates a new Endpoint
	 * @param name The mandatory name
	 */
	public Endpoint(final String name) {
		this(name, -1L, null, null);
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final StringBuilder b =  new StringBuilder(name);
		if(unit!=null) {
			b.append("-").append(period).append(unit.shortName);
		}
		if(processor!=null) {
			b.append("/").append(processor);
		}
		return b.toString();
	}
	
	/**
	 * Renders the groovy source file name
	 * @return the groovy source file name
	 */
	public String toSource() {
		final StringBuilder b =  new StringBuilder(name);
		if(unit!=null) {
			b.append("-").append(period).append(unit.shortName);
		}
		b.append(".groovy");
		return b.toString();				
	}
	
	/**
	 * Indicates if this endpoint has a processor
	 * @return true if this endpoint has a processor, false otherwise
	 */
	public boolean hasProcessor() {
		return processor!=null;
	}
	
	/**
	 * Indicates if this endpoint has an execution period
	 * @return true if this endpoint has an execution period, false otherwise
	 */
	public boolean hasSchedule() {
		return unit!=null;
	}

	/**
	 * Returns the endpoint name
	 * @return the endpoint name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the polling period
	 * @return the polling period
	 */
	public long getPeriod() {
		return period;
	}

	/**
	 * Returns the polling period unit or null if no polling is defined
	 * @return the polling period unit
	 */
	public TimeUnitSymbol getUnit() {
		return unit;
	}

	/**
	 * Returns the custom processor descriptor or null if one is not defined
	 * @return the custom processor descriptor 
	 */
	public String getProcessor() {
		return processor;
	}
	
	
	
	/**
	 * <p>Title: EndpointSerializer</p>
	 * <p>Description: The endpoint json serializer</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.endpoint.publisher.Endpoint.EndpointSerializer</code></p>
	 */
	public static class EndpointSerializer extends JsonSerializer<Endpoint> {

		/**
		 * {@inheritDoc}
		 * @see com.fasterxml.jackson.databind.JsonSerializer#serialize(java.lang.Object, com.fasterxml.jackson.core.JsonGenerator, com.fasterxml.jackson.databind.SerializerProvider)
		 */
		@Override
		public void serialize(final Endpoint value, final JsonGenerator jgen, final SerializerProvider serializers) throws IOException, JsonProcessingException {
			jgen.writeStartObject();
			jgen.writeStringField("name", value.name);
			if(value.unit!=null) {
				jgen.writeNumberField("p", value.period);
				jgen.writeStringField("u", value.unit.shortName);
			}
			if(value.processor!=null) {
				jgen.writeStringField("pr", value.processor);
			}
			jgen.writeEndObject();
		}
		
	}
	
	/**
	 * <p>Title: EndpointDeserializer</p>
	 * <p>Description: Json deserializer for Endpoints.</p> 
	 * <p>Company: Helios Development Group LLC</p>
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>com.heliosapm.endpoint.publisher.Endpoints.EndpointDeserializer</code></p>
	 */
	public static class EndpointDeserializer extends JsonDeserializer<Endpoint> {

		/**
		 * {@inheritDoc}
		 * @see com.fasterxml.jackson.databind.JsonDeserializer#deserialize(com.fasterxml.jackson.core.JsonParser, com.fasterxml.jackson.databind.DeserializationContext)
		 */
		@Override
		public Endpoint deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
			final JsonNode node = p.getCodec().readTree(p);
			final String name = node.get("name").textValue();
			final String processor = node.has("pr") ?  node.get("pr").textValue() : null;
			final TimeUnitSymbol tus = node.has("u") ? TimeUnitSymbol.fromShortName(node.get("u").textValue()) : null;
			final long period = tus==null ? -1L : node.get("p").asLong();
			return new Endpoint(name, period, tus, processor);
		}
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + (int) (period ^ (period >>> 32));
		result = prime * result + ((processor == null) ? 0 : processor.hashCode());
		result = prime * result + ((unit == null) ? 0 : unit.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Endpoint other = (Endpoint) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (period != other.period)
			return false;
		if (processor == null) {
			if (other.processor != null)
				return false;
		} else if (!processor.equals(other.processor))
			return false;
		if (unit != other.unit)
			return false;
		return true;
	}
	
	
}
