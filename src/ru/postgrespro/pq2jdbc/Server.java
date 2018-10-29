package ru.postgrespro.pq2jdbc;

import java.net.*;
import java.io.*;

public class Server
{
	ServerSocket serverSocket;
	String jdbcDriver;
	String jdbcUrl;
	boolean verbose;
	boolean translate;

	public Server(int proxyPort, String driver, String url, boolean translateCommands, boolean verboseMode) throws Exception
	{
		serverSocket = new ServerSocket(proxyPort);
		jdbcDriver = driver;
		jdbcUrl = url;
		verbose = verboseMode;
		translate = translateCommands;
		Class.forName("org." + driver + ".Driver");
	}

	void start() throws Exception
	{
		while (true) { 
			Socket clientSocket = serverSocket.accept();
			clientSocket.setTcpNoDelay(true);
			Session session = new Session(this, clientSocket);
			Thread thread = new Thread(session);
			thread.start();
		}
	}
}
