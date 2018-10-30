package ru.postgrespro.pq2jdbc;

public class Main
{
	public static void main(String[] args) throws Exception
	{
		int port = 5433;
		String url = "postgresql:";
		String driver = "org.postgresql.Driver";
		boolean verbose = false;
		boolean translate = false;

		for (int i = 0; i < args.length; i++) {
			if (args[i].charAt(0) == '-') {
				switch (args[i].charAt(1)) {
				case 'p':
					port = Integer.parseInt(args[++i]);
					continue;
				case 'c':
					url = args[++i];
					continue;
				case 'd':
					driver = args[++i];
					continue;
				case 'v':
					verbose = true;
					continue;
				case 't':
					translate = true;
					continue;
				default:
					break;
				}
			}
			System.err.println("Usage: java Server {options}\n" +
							   "Options:\n" +
							   "\t-v\tverbose mode\n" +
							   "\t-t\ttranslate Postgres specific commands\n" +
							   "\t-p PORT\tserver port\n" +
							   "\t-c URL\tJDBC URL\n" +
							   "\t-d URL\tJDBC driver");
			return;
		}
		Server server = new Server(port, driver, url, translate, verbose);
		server.start();
	}
}
