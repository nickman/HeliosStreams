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
package com.heliosapm.streams.collector.links;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.heliosapm.utils.url.URLHelper;

/**
 * <p>Title: LinkFile</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.streams.collector.links.LinkFile</code></p>
 */

public class LinkFile extends File {
	/**  */
	private static final long serialVersionUID = 1804771241267574170L;
	/** Platform line separator */
	public static final String EOL = System.getProperty("line.separator");
	/** The pattern to extract the target link file from */
	public static final Pattern LINK_PATTERN = Pattern.compile("LINK:(.*?)(?:$EOL)*?", Pattern.CASE_INSENSITIVE);
	/** The pattern to match a link file */
	public static final Pattern LINK_FILE_PATTERN = Pattern.compile(".*\\.lnk$", Pattern.CASE_INSENSITIVE);
	
	
	
	
	/**
	 * Returns the target file for the passed file name
	 * @param linkFile The file name or URL of a link file
	 * @return the target file
	 */
	protected static File readLink(final CharSequence linkFile) {
		if(linkFile==null) throw new IllegalArgumentException("The passed link file name was null");
		final String line = URLHelper.getTextFromURL(linkFile).trim();
		final Matcher m = LINK_PATTERN.matcher(line);
		if(!m.matches()) throw new IllegalArgumentException("Failed to find link pattern in link file [" + linkFile + "]");
		final String target = m.group(1);
		final File f = new File(target);
		if(f.exists() && f.isFile()) return f;
		throw new IllegalArgumentException("Link file [" + linkFile + "] pointed to unreadable target [" + f + "]");
	}
	
	/**
	 * Indicates if the passed file exists and is a valid link file
	 * @param file The file to test
	 * @return true if the passed file exists and is a valid link file
	 */
	public static boolean isLinkFile(final File file) {
		if(file==null) return false;
		try {
			 return readLink(file.getAbsolutePath())!=null;
		} catch (IllegalArgumentException iax) {
			return false;
		}
	}
	
	/** An all files file filter */
	public static final FileFilter ALL_FILES_FILTER = new FileFilter() {
		@Override
		public boolean accept(final File pathname) {
			return true;
		}
	};
	
	/**
	 * Returns a file filter where the full file name matches the passed regex 
	 * @param expression The regex expression
	 * @return the filter
	 */
	public static final FileFilter fileNameFilter(final String expression) {
		if(expression==null || expression.trim().isEmpty()) throw new IllegalArgumentException("The passed expression was null or empty");
		final Pattern p = Pattern.compile(expression.trim());
		return new FileFilter() {		
			@Override
			public boolean accept(final File file) {
				return p.matcher(file.getAbsoluteFile().toPath().toFile().getAbsolutePath()).matches();
			}
		};
	}
	
	
	/**
	 * Returns an array of all the link files found in the passed parent directory
	 * @param parentDir The parent directory to search
	 * @param recursive true to search recursively, false otherwise
	 * @param targetFilter An optional file filter applied to the underlying target file
	 * @return a possibly empty array of link files
	 */
	public static File[] listLinkFiles(final File parentDir, final boolean recursive, final FileFilter targetFilter) {
		if(parentDir==null) throw new IllegalArgumentException("The passed directory was null");
		if(!parentDir.exists()) throw new IllegalArgumentException("The passed directory [" + parentDir + "] does not exist");
		if(!parentDir.isDirectory()) throw new IllegalArgumentException("The passed directory [" + parentDir + "] is a file");
		final FileFilter ff = targetFilter==null ? ALL_FILES_FILTER : targetFilter;
		if(!recursive) {
			return parentDir.listFiles(new FileFilter(){
				@Override
				public boolean accept(final File file) {					
					return isLinkFile(file) && ff.accept(file);
				}
			});
		}
		final Path p = Paths.get(URLHelper.toURI(parentDir.getAbsolutePath()));
		try {
			final DirectoryStream<Path> stream = Files.newDirectoryStream(p, new DirectoryStream.Filter<Path>() {
				@Override
				public boolean accept(final Path entry) throws IOException {
					final File f = entry.toFile();
					return isLinkFile(f) && ff.accept(f);
				}
			});
			final Set<File> files = new HashSet<File>();
			for(final Iterator<Path> iter = stream.iterator(); iter.hasNext();) {
				files.add(iter.next().toFile());
			}
			return files.toArray(new File[files.size()]);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	/**
	 * Examines each file in the passed array and converts an non-linked files
	 * which are actually linked files into LinkedFiles
	 * @param files the array to convert
	 * @return the updated array
	 */
	public static File[] convertLinkedFiles(final File...files) {
		if(files!=null) {
			for(int i = 0; i < files.length; i++) {
				File f = files[i];
				if(f==null) continue;
				if(f instanceof LinkFile) continue;
				if(isLinkFile(f)) {
					files[i] = new LinkFile(f);
				}
			}
		}
		return files;
	}
	
	/**
	 * Creates a new LinkFile
	 * @param file the link file
	 */
	public LinkFile(final File file) {		
		super(readLink(file.getAbsolutePath()).getAbsolutePath());
	}
	
	
	/**
	 * Creates a new LinkFile
	 * @param pathname the path of a link file
	 */
	public LinkFile(final String pathname) {
		super(readLink(pathname).getAbsolutePath());
	}

	/**
	 * Creates a new LinkFile
	 * @param uri the uri of a link file
	 */
	public LinkFile(final URI uri) {
		super(readLink(uri.toString()).getAbsolutePath());
	}

	/**
	 * Creates a new LinkFile
	 * @param parent the link file's parent directory
	 * @param child the link file name
	 */
	public LinkFile(final String parent, final String child) {
		super(readLink(new File(parent, child).getAbsolutePath()).getAbsolutePath());
	}

	/**
	 * Creates a new LinkFile
	 * @param parent the link file's parent directory
	 * @param child the link file name
	 */
	public LinkFile(File parent, String child) {
		super(readLink(new File(parent, child).getAbsolutePath()).getAbsolutePath());
	}

}
