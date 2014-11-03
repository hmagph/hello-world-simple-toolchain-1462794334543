/*******************************************************************************
 * Licensed Materials - Property of IBM
 * (c) Copyright IBM Corporation 2013. All Rights Reserved.
 * Note to U.S. Government Users Restricted Rights: Use,
 * duplication or disclosure restricted by GSA ADP Schedule
 * Contract with IBM Corp.
 *******************************************************************************/
package com.ibm.team.integration.sample.filetrs;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TrackedFileSet {

	// Change events
	static class Event  {
		public static Event fromMemento(String line) throws IOException {
			int start = EVENT_PREFIX.length();
			EventType type;
			switch(line.charAt(start)) {
				case '+' : 
					type = EventType.ADDED; 
					break;
				case '*' : 
					type = EventType.MODIFIED; 
					break;
				case '-' : 
					type = EventType.REMOVED; 
					break;
				default :
	        		throw new IOException("ill-formed file name: "+line.substring(0,1)+" in line:"+line); // ill-formed
			}
        	int sep = line.indexOf(SEP, start+1);
        	if (sep <= 0) {
        		throw new IOException("ill-formed file name: "+line.substring(start+1)+" in line:"+line); // ill-formed
        	}
        	String filename = line.substring(start+1, sep);
        	int sep2 = line.indexOf(SEP, sep+SEP.length() );
        	if (sep2 <= 0) {
        		throw new IOException("ill-formed file name: "+line.substring(sep+SEP.length())+" in line:"+line); // ill-formed
        	}
        	String id = line.substring(sep+SEP.length(), sep2);
        	long timestamp = Long.parseLong(line.substring(sep2+SEP.length()));
			return new Event(type, filename, id, timestamp);
		}
		protected EventType type;
		private String resourceName;
		private String id;
		
		protected long timestamp;
		
		public Event(EventType type, String resourceName, String id, long timestamp) {
			this.type = type;
			this.resourceName = resourceName;
			this.id = id;
			this.timestamp = timestamp;
		}
		
		String getMemento() {
			switch (this.type) {
				case ADDED : 
					return EVENT_PREFIX + "+" + this.resourceName + SEP + this.id + SEP + this.timestamp; 
				case MODIFIED : 
					return EVENT_PREFIX + "*" + this.resourceName + SEP + this.id + SEP + this.timestamp; 
				case REMOVED :
				default : 
					return EVENT_PREFIX + "-" + this.resourceName + SEP + this.id + SEP + this.timestamp;
			}
		}
		
		public String getResourceName() {
			return this.resourceName;
		}

		public String getURN() {
			return "urn:change:"+ this.id;
		}
		
		@Override
		public String toString() {
			switch (this.type) {
				case ADDED : 
					return "[+] " + this.resourceName + " - " + this.id + " @" + this.timestamp;
				case MODIFIED : 
					return "[*] " + this.resourceName + " - " + this.id + " @" + this.timestamp;
				case REMOVED :
				default : 
					return "[-] " + this.resourceName + " - " + this.id + " @" + this.timestamp;
			}
		}
	}

	// Change Event type
	static enum EventType { 
		ADDED, MODIFIED, REMOVED
	}
	
	// Consistent state between change events and file map, for atomic updates
	static class State {
		public static State restore() {
			State restoredState = new State();
		    restoredState.fileMap = Collections.<String,String>emptyMap();
		    restoredState.events = Collections.<Event>emptyList();
			
			BufferedReader br = null;
			Map<String,String> restoredFiles = new HashMap<String,String>(3);
			List<Event> restoredEvents = new ArrayList<Event>();
		    try {
				br = new BufferedReader(new FileReader(FILEMAP));
		        String line = br.readLine();
		        while (line != null) {
		        	if (line.startsWith(FILE_PREFIX)) {
			        	int sep = line.indexOf(SEP, FILE_PREFIX.length());
			        	if (sep <= 0) {
			        		throw new IOException("ill-formed file map: "+line); // ill-formed
			        	}
			        	restoredFiles.put(line.substring(FILE_PREFIX.length(), sep), line.substring(sep+SEP.length()));		
		        	} else if (line.startsWith(EVENT_PREFIX)) {
		        		Event event = Event.fromMemento(line);
				        restoredEvents.add(event);
		        	}
		            line = br.readLine();
		        }
		        restoredState.fileMap = restoredFiles;
		        restoredState.events = restoredEvents;
		    } catch (FileNotFoundException e) {
		    	// ignore (first start)
		    } catch (IOException e) {
		    	e.printStackTrace();
		    } finally {
		        if (br != null) {
		        	try {
		        		br.close();
		        	} catch(IOException e) {
		        		// ignore
		        	}
		        }
		    }
			return restoredState;
		}
		private Map<String,String> fileMap;
		private List<Event> events;
		
		private String eTag;
		
		public State() {
			this.eTag = UUID.randomUUID().toString();
		}
		
		public String getETag() {
			return this.eTag;
		}
		
		public List<Event> getEvents() {
			return this.events;
		}
		
		public Map<String, String> getFileMap() {
			return this.fileMap;
		}
		
		public void persist() {
			BufferedWriter bw = null;
		    try {
				bw = new BufferedWriter(new FileWriter(FILEMAP));
				for (Map.Entry<String,String> entry : this.fileMap.entrySet()) {
					bw.write(FILE_PREFIX);
					bw.write(entry.getKey());
					bw.write(SEP);
					bw.write(entry.getValue());
					bw.write('\n');
				}
				for (Event event: this.events) {
					bw.write(event.getMemento());
					bw.write('\n');
				}
		    } catch (IOException e) {
		    	// exit
		    } finally {
		        if (bw != null) {
		        	try {
		        		bw.close();
		        	} catch(IOException e) {
		        		// ignore
		        	}
		        }
		    }	
		}		
		
		public void setEvents(List<Event> events) {
			this.events = events;
		}
		
		public void setFileMap(Map<String, String> fileMap) {
			this.fileMap = fileMap;
		}
	}

	private static boolean DEBUG = true;
	
	protected static final File RESOURCE_ROOT_FOLDER = new File("resources");
	protected static final File FILEMAP = new File("metadata/state.dat");
	private static final String FILE_PREFIX = "file::";
	private static final String EVENT_PREFIX = "event::";
	private static final String SEP = "//";
	protected static final Charset UTF8 = Charset.forName("UTF-8");
	private static final long WEEK_DURATION = 7*24*60*60*1000; // week duration in ms

	private static void collectFiles(File folder, Map<String,String> filemap) throws IOException {
		if (folder.isDirectory()) {
			for (File file : folder.listFiles()) {
				if (file.isDirectory()) {
					collectFiles(file, filemap);
				} else {
					filemap.put(file.getPath().replace("\\","/"), String.valueOf(file.lastModified()));
				}
			}
		}
	}
	public static void main(String[] args) throws IOException {
		TrackedFileSet tfs = new TrackedFileSet();
		tfs.startReconciling();
	}
	// thread for reconciling with file system in background
	private Thread reconcilerThread;
	
	boolean reconcilerStopped = false;
	
	final Object reconcilerLock = new Object();
	
	private State currentState;
	
    TrackedFileSet() {
		this.currentState = State.restore();
		startReconciling();
	}	
	
	public void deleteResource(String resourceLocation) throws IOException {
		File file = new File(RESOURCE_ROOT_FOLDER + resourceLocation);
		boolean ok = file.delete();
		if (!ok) throw new IOException("Unable to delete existing resource: "+ resourceLocation);
	}
	
	public String getETag() {
    	if (this.currentState == null) return null;
    	return this.currentState.getETag();
    }
	
	public String getResourceURL(String rootServerURL, String resourceName) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder(10);
		sb.append(rootServerURL);
		for (String segment: resourceName.split("/")) {
			sb.append('/').append(URLEncoder.encode(segment, UTF8.name()));
		}
		return sb.toString();
	}
	
	public void publishBase(String rootServerURL, OutputStream output) throws IOException {
		State publishedState = this.currentState;
		Map<String,String> fileMap = publishedState.getFileMap();
		output.write(
				("# Resource: "+rootServerURL+"/base\n" +
				"# (root folder: " +  RESOURCE_ROOT_FOLDER.getAbsolutePath() +")\n\n" +
				"@prefix ldp: <http://www.w3.org/ns/ldp#> .\n" +
				"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
				"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n" +
				"@prefix trs: <http://open-services.net/ns/core/trs#> .\n\n" +
				"<"+rootServerURL+"/base>\n" +
				"  rdf:type ldp:Container ;\n").getBytes(UTF8));
		if (fileMap.isEmpty()) {
			output.write(("  trs:cutoffEvent rdf:nil").getBytes(UTF8));
		} else {
			output.write(("  trs:cutoffEvent <"+publishedState.getEvents().get(0).getURN()+">").getBytes(UTF8));
		}
		for (String filename: publishedState.getFileMap().keySet()) {
			output.write((" ;\n  rdfs:member <"+getResourceURL(rootServerURL, filename)+">").getBytes(UTF8));
		}
		output.write((" .\n\n").getBytes(UTF8));
		output.write(("<"+rootServerURL+"/base/page1>\n" +
				"  rdf:type ldp:Page ;\n" +
				"  ldp:pageOf <"+rootServerURL+"/base> ;\n" +
			    "  ldp:nextPage rdf:nil .\n").getBytes(UTF8));
	}
	
	public State publishChangeLog(String rootServerURL, OutputStream output) throws IOException {
		State publishedState = this.currentState;
		String eTag = null;
		output.write(
				("# Resource: "+rootServerURL+"/trs\n" +
				"# (root folder: " +  RESOURCE_ROOT_FOLDER.getAbsolutePath() +")\n\n" +
				"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
				"@prefix trs: <http://open-services.net/ns/core/trs#>.\n" +
				"@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n" +
				"<"+rootServerURL+"/trs>\n" +
				"  rdf:type trs:TrackedResourceSet ;\n"+
				"  trs:base <"+rootServerURL+"/base> ;\n" +
				"  trs:changeLog [\n" +
				"    rdf:type trs:ChangeLog").getBytes(UTF8));
		List<Event> events = publishedState.getEvents();
		boolean isEmpty = events.isEmpty();
		if (isEmpty) {
			output.write((" \n  ] .\n").getBytes(UTF8));
		} else {
			for (Event event : events) {
				output.write((" ;\n    trs:change <"+event.getURN()+">").getBytes(UTF8));
			}
			output.write((" \n  ] .\n").getBytes(UTF8));
			int trsOrder = events.size()-1;
			for (Event event : events) {
				output.write(("\n<"+event.getURN()+">\n").getBytes(UTF8));
				switch (event.type) {
					case ADDED : 
						output.write("  rdf:type trs:Creation ;\n".getBytes(UTF8));
						break;
					case MODIFIED : 
						output.write("  rdf:type trs:Modification ;\n".getBytes(UTF8));
						break;
					case REMOVED :
					default : 
						output.write("  rdf:type trs:Deletion ;\n".getBytes(UTF8));
						break;
				}
				output.write(("  trs:changed <"+getResourceURL(rootServerURL, event.getResourceName())+"> ;\n").getBytes(UTF8));
				output.write(("  trs:order \""+(trsOrder--)+"\"^^xsd:integer .\n").getBytes(UTF8));
			}
		}
		return publishedState;
	}
		
	public boolean publishResource(String resourceLocation, OutputStream output) throws IOException {
		
		File file = new File(RESOURCE_ROOT_FOLDER + resourceLocation);
		if (!file.exists()) return false;
	    int buflen = 8192;
	    byte[] buffer = new byte[buflen];
	    InputStream in = null;
	    try {
	    	in = new BufferedInputStream(new FileInputStream(file));
	    	while(true) {
	            int read = in.read(buffer, 0, buflen);
	            if (read > 0) {
	            	output.write(buffer, 0, read);
	            } else if(read < 0) {
	                break;
	            }
	        }
	    } finally {
	    	if (in != null) {
	    		try {
	    			in.close();
	    		} catch (IOException e) {
	    			// ignore
	    		}
	    	}
	    }
	    return true;
	}
	
	private State reconcileChanges(State currentState, Map<String,String> newfiles) throws IOException {
		Map<String,String> oldfiles = currentState.getFileMap();
		List<Event> oldEvents = currentState.getEvents();
		List<Event> updatedEvents = null;
		Map<String,String> remainingNewFiles = new HashMap<String,String>(newfiles);
		for (Map.Entry<String,String> oldentry : oldfiles.entrySet()) {
			remainingNewFiles.remove(oldentry.getKey());
			String oldfileEtag = oldentry.getValue();
			String newfileEtag = newfiles.get(oldentry.getKey());
			if (newfileEtag == null) {
				if (updatedEvents == null) { // copy since could be iterated by publish in parallel
					updatedEvents = new ArrayList<Event>(oldEvents); 
				}
				recordChangeEvent(updatedEvents, EventType.REMOVED, oldentry.getKey());
				continue;
			}
			if (!oldfileEtag.equals(newfileEtag)) {
				if (updatedEvents == null) { // copy since could be iterated by publish in parallel
					updatedEvents = new ArrayList<Event>(oldEvents); 
				}
				recordChangeEvent(updatedEvents, EventType.MODIFIED, oldentry.getKey());
			}
		}
		for (Map.Entry<String,String> remainingNewEntry : remainingNewFiles.entrySet()) {
			if (updatedEvents == null) { // copy since could be iterated by publish in parallel
				updatedEvents = new ArrayList<Event>(oldEvents); 
			}
			recordChangeEvent(updatedEvents, EventType.ADDED, remainingNewEntry.getKey());
		}
		if (updatedEvents != null) {
			State newState = new State();
			newState.setEvents(updatedEvents);
			newState.setFileMap(newfiles);
			return newState; // found changes
		}
		return currentState; // no change
	}	

	public void reconcileLoop() {
		if (DEBUG) {
			if (DEBUG) {
				try {
					System.out.println("======TRS:ChangeLog (startup)=======");
					publishChangeLog("http://localhost/tfs", System.out);
					System.out.println("========TRS:Base (startup)=========");
					publishBase("http://localhost/tfs", System.out);
				} catch(IOException e) {
					//ignore
				}
			}
		}
		int iteration = 0;
		while (!this.reconcilerStopped) {
			try {
				State oldState = this.currentState;
				Map<String, String> newfilemap = new HashMap<String, String>(oldState.getFileMap().size());
				collectFiles(RESOURCE_ROOT_FOLDER, newfilemap);
				State newState = reconcileChanges(this.currentState, newfilemap);
				if (newState != oldState) {
					newState.persist();
					this.currentState = newState;
					if (DEBUG) {
						try {
							System.out.println("======TRS:ChangeLog (#"+iteration+")=======");
							publishChangeLog("http://localhost/tfs", System.out);
							System.out.println("========TRS:Base (#"+iteration+")=========");
							publishBase("http://localhost/tfs", System.out);
						} catch(IOException e) {
							//ignore
						}
					}
					iteration++;
				}
				Thread.sleep(1000); // reconcile every 1s
			} catch(Exception e) {
				// ignore
			}
		}
	}	
	
	public Event recordChangeEvent(List<Event> events, EventType eventType, String resourceName) {
		long now = System.currentTimeMillis();
		Event event = new Event(eventType, resourceName, UUID.randomUUID().toString(), now);
		events.add(0, event); // most recent first
		int length = events.size();
		if (length > 500) { 		// trim older ones if the queue is getting long (older than one week)
			for (int last = length-1; events.get(last).timestamp < now-WEEK_DURATION; last--) {
				events.remove(last);
			}
		}
		return event;
	}
	
	public void startReconciling() {
		System.out.println("Reconciling resources in: "+ RESOURCE_ROOT_FOLDER.getAbsolutePath());
		synchronized (this.reconcilerLock) {
			if (this.reconcilerThread != null) return; // already started
			this.reconcilerStopped = false;
			this.reconcilerThread = new Thread(
					new Runnable() {
						@Override
						public void run() {
							TrackedFileSet.this.reconcileLoop();
						}
					},
					"filesystem reconciler");
			this.reconcilerThread.start();
		}
	}
	
	public void stopReconciling() {
		synchronized (this.reconcilerLock) {
			this.reconcilerStopped = true; // reconciling loop will exit
			try {
				this.reconcilerThread.join(); // wait until it ends
			} catch(InterruptedException e) {
				// ignore
			}
			this.reconcilerThread = null;
		}
	}

	public void writeResource(String resourceLocation, InputStream input) throws IOException {
		File root = RESOURCE_ROOT_FOLDER;
		if (!root.exists()) {
			root.mkdir();
		}
		File file = new File(RESOURCE_ROOT_FOLDER, resourceLocation);
		if (file.exists()) {
			boolean ok = file.delete();
			if (!ok) throw new IOException("Unable to delete existing resource: "+ resourceLocation);
		}
	    int buflen = 8192;
	    byte[] buffer = new byte[buflen];
	    InputStream in = null;
	    FileOutputStream output = new FileOutputStream(file);
	    try {
	    	in = new BufferedInputStream(input);
	    	while(true) {
	            int read = in.read(buffer, 0, buflen);
	            if (read > 0) {
	            	output.write(buffer, 0, read);
	            } else if(read < 0) {
	                break;
	            }
	        }
	    } finally {
	    	if (output != null) {
	    		try {
	    			output.close();
	    		} catch (IOException e) {
	    			// ignore
	    		}
	    	}
	    }
	}
}
