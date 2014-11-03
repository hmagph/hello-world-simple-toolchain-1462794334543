/*******************************************************************************
 * Licensed Materials - Property of IBM
 * (c) Copyright IBM Corporation 2014. All Rights Reserved.
 * Note to U.S. Government Users Restricted Rights: Use,
 * duplication or disclosure restricted by GSA ADP Schedule
 * Contract with IBM Corp.
 *******************************************************************************/
package com.ibm.team.integration.sample.filetrs;

import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class TrackedFileSetServlet
 */
@WebServlet("/*")
public class TrackedFileSetServlet extends HttpServlet {
	private static final String TEXT_PLAIN = "text/plain";
	private static final long serialVersionUID = 1L;
	private static final String TURTLE = "text/turtle";
	private static final String ETAG = "ETag";
	private static final String IF_NONE_MATCH = "If-None-Match";
	private static final String RESOURCES_PREFIX = "/resources/";
	private static final String TRS = "/trs";
	private static final String BASE = "/base";
	TrackedFileSet trackedFileSet = new TrackedFileSet();

    /**
     * @see HttpServlet#HttpServlet()
     */
    public TrackedFileSetServlet() {
        super();
    }

	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println(request.getMethod() + " "+ request.getRequestURI());
		Enumeration<String> headers = request.getHeaderNames();
		while (headers.hasMoreElements()) {
			String h = headers.nextElement();
			System.out.println(h + " = " + request.getHeader(h));
		}
		String pathInfo = request.getPathInfo();
		if (pathInfo.startsWith(RESOURCES_PREFIX)) {
			String resourcePath = pathInfo.substring(RESOURCES_PREFIX.length()-1, pathInfo.length());
			this.trackedFileSet.deleteResource(resourcePath);
		} else {
			response.getOutputStream().write(("Unhandled URL: " + pathInfo).getBytes(TrackedFileSet.UTF8));
			response.setStatus(404);
		}
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println(request.getMethod() + " "+ request.getRequestURI());
		Enumeration<String> headers = request.getHeaderNames();
		while (headers.hasMoreElements()) {
			String h = headers.nextElement();
			System.out.println(h + " = " + request.getHeader(h));
		}
		String pathInfo = request.getPathInfo();
		if (pathInfo.startsWith(RESOURCES_PREFIX)) {
			response.setCharacterEncoding(TrackedFileSet.UTF8.name());
			String resourcePath = pathInfo.substring(RESOURCES_PREFIX.length()-1, pathInfo.length());
			if (resourcePath.endsWith(".ttl")) {
				response.setContentType(TURTLE);
			} else {
				response.setContentType(TEXT_PLAIN);
			}
			boolean found = this.trackedFileSet.publishResource(resourcePath, response.getOutputStream());
			if (!found) {
				response.getOutputStream().write(("Resource "+resourcePath+" not found in "+TrackedFileSet.RESOURCE_ROOT_FOLDER.getAbsolutePath()).getBytes(TrackedFileSet.UTF8));
				response.setStatus(404); // not found
			}
		} else if (pathInfo.equals(TRS)) {
			String matchETag = request.getHeader(IF_NONE_MATCH);
			if (matchETag != null) {
				if (matchETag.equals(this.trackedFileSet.getETag())) {
					response.setStatus(304); // not modified
					return;
				}
			}
			response.setContentType(TURTLE);
			response.setCharacterEncoding(TrackedFileSet.UTF8.name());
			String rootServerURL = request.getRequestURL().substring(0, request.getRequestURL().indexOf(pathInfo));
			this.trackedFileSet.publishChangeLog(rootServerURL, response.getOutputStream());
			String eTag = this.trackedFileSet.getETag();
			if (eTag != null) {
				response.setHeader(ETAG, eTag);
			}
		} else if (pathInfo.equals(BASE)) {
			response.setContentType(TURTLE);
			response.setCharacterEncoding(TrackedFileSet.UTF8.name());
			String rootServerURL = request.getRequestURL().substring(0, request.getRequestURL().indexOf(pathInfo));
			this.trackedFileSet.publishBase(rootServerURL, response.getOutputStream());
		} else {
			response.getOutputStream().write(("Unhandled URL: " + pathInfo).getBytes(TrackedFileSet.UTF8));
			response.setStatus(404);
		}
	}
	
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String action = request.getParameter("action");
		if ("stop".equals(action)) {
			this.trackedFileSet.stopReconciling();
		} else if ("start".equals(action)) {
			this.trackedFileSet.startReconciling();
		}
	}

	@Override
	protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println(request.getMethod() + " "+ request.getRequestURI());
		Enumeration<String> headers = request.getHeaderNames();
		while (headers.hasMoreElements()) {
			String h = headers.nextElement();
			System.out.println(h + " = " + request.getHeader(h));
		}
		String pathInfo = request.getPathInfo();
		if (pathInfo.startsWith(RESOURCES_PREFIX)) {
			String resourcePath = pathInfo.substring(RESOURCES_PREFIX.length()-1, pathInfo.length());
			this.trackedFileSet.writeResource(resourcePath, request.getInputStream());
		} else {
			response.getOutputStream().write(("Unhandled URL: " + pathInfo).getBytes(TrackedFileSet.UTF8));
			response.setStatus(404);
		}
	}

}
