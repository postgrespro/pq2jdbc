package ru.postgrespro.pq2jdbc;

import java.net.*;
import java.io.*;
import java.sql.*;
import java.util.HashMap;

public class Session implements Runnable
{
	Server server;
	DataInputStream inputStream;
	DataOutputStream outputStream;
	HashMap<String,PreparedStatement> prepared;
	HashMap<String,PreparedStatement> portals;
	Connection con;

	public Session(Server thisServer, Socket socket) throws Exception
	{
		server = thisServer;
		inputStream = new DataInputStream(socket.getInputStream());
		outputStream = new DataOutputStream(socket.getOutputStream());
		prepared = new HashMap<String,PreparedStatement>();
		portals = new HashMap<String,PreparedStatement>();
	}

	static int strchr(byte[] buf, int offset, char ch)
	{
		byte b = (byte)ch;
		for (int i = offset; i < buf.length; i++) {
			if (buf[i] == b) {
				return i;
			}
		}
		return -1;
	}

	// Read startup packet and establish JDBC connection
	Connection establishConnection() throws Exception
	{
		String database = null;
		String user = "postgres";
		String password = "postgres";
		String options = "";

		int len = inputStream.readInt() - 4;
		byte[] buf = new byte[len];
		inputStream.readFully(buf);
		int offset = 4; // skip protocol version
		int eos;

		while (offset < len) {
			int end = strchr(buf, offset, '\0');
			if (end < 0 || end == offset) {
				break;			/* found packet terminator */
			}
			String name = new String(buf, offset, end - offset);
			offset = end + 1;
			end = strchr(buf, offset, '\0');
			String value = new String(buf, offset, end - offset);
			offset = end + 1;
			switch (name) {
			case "database":
				database = value;
				break;
			case "user":
				user = value;
				break;
			case "options":
				options = value;
				break;
			}
		}
		if (database == null) {
			database = user;
		}

		// Authentication request
		outputStream.writeByte('R');
		outputStream.writeInt(8); /* message length  */
		outputStream.writeInt(0); /* User is authenticated  */
		outputStream.flush();

		outputStream.writeByte('K');
		outputStream.writeInt(12); /* message length  */
		outputStream.writeInt(-1); // backend pid
		outputStream.writeInt(-1); // cancel key
		outputStream.flush();

		return DriverManager.getConnection("jdbc:" + server.jdbcDriver + ":" + server.jdbcUrl + database, user, password);
	}

	void putMessage(char op, byte[] buf) throws IOException
	{
		outputStream.writeByte((byte)op);
		outputStream.writeInt(buf.length+4);
		outputStream.write(buf, 0, buf.length);
		outputStream.flush();
	}

	void putMessage(char op, String msg) throws IOException
	{
		byte[] body = msg.getBytes();
		outputStream.writeByte((byte)op);
		outputStream.writeInt(body.length+5);
		outputStream.write(body, 0, body.length);
		outputStream.writeByte('\0');
		outputStream.flush();
	}

	void putMessage(char op, char arg) throws IOException
	{
		byte[] buf = new byte[1];
		buf[0] = (byte)arg;
		putMessage(op, buf);
	}

	void putMessage(char op) throws IOException
	{
		outputStream.writeByte((byte)op);
		outputStream.writeInt(4);
		outputStream.flush();
	}

	static int getTypeOid(int type)
	{
		switch (type) {
		case Types.BIT:
			return 1560;
		case Types.BIGINT:
			return 20;
		case Types.BOOLEAN:
			return 16;
		case Types.DATE:
			return 1082;
		case Types.DOUBLE:
			return 701;
		case Types.FLOAT:
		case Types.REAL:
			return 700;
		case Types.INTEGER:
			return 23;
		case Types.LONGVARBINARY:
		case Types.VARBINARY:
		case Types.BINARY:
		case Types.BLOB:
			return 17;
		case Types.LONGVARCHAR:
		case Types.VARCHAR:
		case Types.CHAR:
		case Types.NCHAR:
		case Types.NCLOB:
			return 25;
		case Types.NUMERIC:
			return 1700;
		case Types.TIME:
			return 1083;
		case Types.TIMESTAMP:
			return 1114;
		case Types.TINYINT:
			return 21;
		default:
			return 0;
		}
	}

	void sendRowDescription(ResultSetMetaData meta) throws Exception
	{
		ByteArrayOutputStream buf = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(buf);
		int nColumns = meta.getColumnCount();
		out.writeShort(nColumns);

		for (int i = 1; i <= nColumns; i++) {
			byte[] attName = meta.getColumnName(i).getBytes();
			int attTypeId = getTypeOid(meta.getColumnType(i));
			int attTypeMod = -1;
			out.write(attName);
			out.writeByte('\0');
			out.writeInt(0); // resorigtbl
			out.writeShort(0); // resorigcol
			out.writeInt(attTypeId);
			out.writeShort(meta.getColumnDisplaySize(i));
			out.writeInt(attTypeMod);
			out.writeShort(0); // format
		}
		out.flush();
		putMessage('T', buf.toByteArray());
	}

	boolean sendResult(Statement stmt, long maxRows) throws Exception
	{
		ResultSet result = stmt.getResultSet();
		String commandTag;
		boolean completed = true;
		if (result != null) {
			long nResults = 0;
			ResultSetMetaData meta = result.getMetaData();
			sendRowDescription(meta);
			int nColumns = meta.getColumnCount();
			while (result.next()) {
				ByteArrayOutputStream buf = new ByteArrayOutputStream();
				DataOutputStream out = new DataOutputStream(buf);
				out.writeShort(nColumns);
				for (int i = 1; i <= nColumns; i++) {
					String val = result.getString(i);
					if (val == null) {
						out.writeInt(-1);
					} else {
						out.writeInt(val.length());
						out.write(val.getBytes());
					}
				}
				out.flush();
				putMessage('D', buf.toByteArray());
				nResults += 1;
				if (--maxRows == 0) {
					completed = false;
					break;
				}
			}
			commandTag = "SELECT " + nResults;
		} else {
			commandTag = "INSERT 0 " + stmt.getUpdateCount();
		}
		if (completed) {
			// end commands
			putMessage('C', commandTag); // command tag
			return true;
		} else {
			putMessage('s'); /* Portal run not complete, so send PortalSuspended */
			return false;
		}
	}

	static int unpackInt(byte[] buf, int offs)
	{
		return (buf[offs] << 24) | ((buf[offs+1] & 0xFF) << 16) | ((buf[offs+2] & 0xFF) << 8) | (buf[offs+3] & 0xFF);
	}

	static int unpackShort(byte[] buf, int offs)
	{
		return ((buf[offs] & 0xFF) << 8) | (buf[offs+1] & 0xFF);
	}


	static String replacePlaceholders(String sql) throws SQLException
	{
		StringBuffer buf = new StringBuffer();
		int from = 0;
		int paramBeg;
		for (int i = 1; (paramBeg = sql.indexOf('$', from)) >= 0; i++) {
			int paramEnd;
			for (paramEnd = paramBeg+1; paramEnd < sql.length() && Character.isDigit(sql.charAt(paramEnd)); paramEnd++);
			if (Integer.parseInt(sql.substring(paramBeg+1, paramEnd)) != i) {
				throw new SQLException("Out-of-order prepared statement parameter " + i + ": '" + sql + "'");
			}
			buf.append(sql.substring(from, paramBeg));
			buf.append('?');
			from = paramEnd;
		}
		buf.append(sql.substring(from));
		return buf.toString();
	}

	public void run()
	{
		try	{
			Connection con = establishConnection();
			boolean sendReadyForQuery = true;
			boolean verbose = server.verbose;

			while (true) {
				if (sendReadyForQuery) {
					// Ready for query
					putMessage('Z', 'I');
					sendReadyForQuery = false;
				}
				byte op = inputStream.readByte();
				int len = inputStream.readInt() - 4;
				byte[] body = new byte[len];
				inputStream.readFully(body);
				if (verbose) {
					System.out.println("Receive message " + (char)op);
				}
				switch (op)	{
				case 'Q': 	/* simple query */
				{
					String sql = new String(body, 0, body.length-1);
					if (sql.startsWith("DEALLOCATE ")) {
						String stmtName = sql.substring(11);
						PreparedStatement pstmt = prepared.remove(stmtName);
						if (pstmt != null) {
							pstmt.close();
						}
						putMessage('C', "DEALLOCATE 0 1");
					} else {
						Statement stmt = con.createStatement();
						if (verbose) {
							System.out.println("Receive query '" + sql + "'");
						}
						stmt.execute(sql);
						sendResult(stmt, Long.MAX_VALUE);
					}
					sendReadyForQuery = true;
					break;
				}
				case 'X':				/* terminate */
					inputStream.close();
					con.close();
					return;
				case 'B':				/* bind */
				{
					int beg = 0;
					int end = strchr(body, beg, '\0');
					String portal = new String(body, beg, end - beg);
					beg = end + 1;
					end = strchr(body, beg, '\0');
					String stmtName = new String(body, beg, end - beg);
					beg = end + 1;
					int numFormats = unpackShort(body,  beg);
					beg += 2 + numFormats*2;
					int numParams = unpackShort(body,  beg);
					beg += 2;
					PreparedStatement pstmt = prepared.get(stmtName);
					ParameterMetaData meta = pstmt.getParameterMetaData();
					if (verbose) {
						System.out.println("Bind statement '" + stmtName + "', portal '" + portal + "'");
					}
					for (int i = 1; i <= numParams; i++) {
						len = unpackInt(body, beg);
						beg += 4;
						if (len < 0) {
							pstmt.setNull(i, Types.VARCHAR);
						} else {
							String value = new String(body, beg, len);
							beg += len;
							switch (meta.getParameterType(i)) {
							case Types.BIGINT:
								pstmt.setLong(i, Long.parseLong(value));
								break;
							case Types.BOOLEAN:
								pstmt.setBoolean(i, Boolean.parseBoolean(value));
								break;
							case Types.DATE:
								pstmt.setDate(i, Date.valueOf(value));
								break;
							case Types.DOUBLE:
								pstmt.setDouble(i, Double.parseDouble(value));
								break;
							case Types.FLOAT:
							case Types.REAL:
								pstmt.setFloat(i, Float.parseFloat(value));
								break;
							case Types.INTEGER:
								pstmt.setInt(i, Integer.parseInt(value));
								break;
							case Types.TIME:
								pstmt.setTime(i, Time.valueOf(value));
								break;
							case Types.TIMESTAMP:
								pstmt.setTimestamp(i, Timestamp.valueOf(value));
								break;
							case Types.TINYINT:
								pstmt.setShort(i, (short)Integer.parseInt(value));
								break;
							default:
								pstmt.setString(i, value);
								break;
							}
						}
					}
					portals.put(portal, pstmt);
					putMessage('2');
					pstmt.execute();
					break;
				}
				case 'E':				/* execute */
				{
					int beg = 0;
					int end = strchr(body, beg, '\0');
					String portal = new String(body, beg, end - beg);
					beg = end + 1;
					int maxRows = unpackInt(body, beg);
					PreparedStatement pstmt = portals.get(portal);
					sendResult(pstmt, maxRows);
					break;
				}
				case 'P':				/* parse */
				{
					int beg = 0;
					int end = strchr(body, beg, '\0');
					String name = new String(body, beg, end - beg);
					beg = end + 1;
					end = strchr(body, beg, '\0');
					String sql = new String(body, beg, end - beg);
					sql = replacePlaceholders(sql);
					if (verbose) {
						System.out.println("Prepare statement " + name + ": '" + sql + "'");
					}
					PreparedStatement pstmt =  con.prepareStatement(sql);
					prepared.put(name, pstmt);
					putMessage('1');
					break;
				}
				case 'C':				/* close */
				{
					int end = strchr(body, 1, '\0');
					int closeType = body[0];
					String closeTarget = new String(body, 1, end-1);
					switch (closeType) {
					case 'S':
					{
						PreparedStatement pstmt = prepared.remove(closeTarget);
						if (pstmt != null) {
							pstmt.close();
						}
						break;
					}
					case 'P':
					{
						PreparedStatement pstmt = portals.remove(closeTarget);
						if (pstmt != null) {
							pstmt.close();
						}
						break;
					}
					default:
						throw new RuntimeException("Invalid close message subtype " + closeType);
					}
					break;
				}
				case 'D':				/* describe */
				{
					int end = strchr(body, 1, '\0');
					int describeType = body[0];
					String describeTarget = new String(body, 1, end-1);
					switch (describeType) {
					case 'S':
					{
						PreparedStatement pstmt = prepared.get(describeTarget);
						if (pstmt != null) {
							ByteArrayOutputStream buf = new ByteArrayOutputStream();
							DataOutputStream out = new DataOutputStream(buf);
							ParameterMetaData paramDesc = pstmt.getParameterMetaData();
							int nParams = paramDesc.getParameterCount();
							// first describe parameters
							out.writeShort(nParams);
							for (int i = 1; i <= nParams; i++) {
								int type = paramDesc.getParameterType(i);
								out.writeInt(getTypeOid(type));
							}
							out.flush();
							putMessage('t', buf.toByteArray());

							ResultSetMetaData resultDesc = pstmt.getMetaData();
							if (resultDesc == null) {
								putMessage('n');
							} else {
								sendRowDescription(resultDesc);
							}
						}
						break;
					}
					case 'P':
					{
						PreparedStatement pstmt = portals.get(describeTarget);
						if (pstmt != null) {
							ResultSetMetaData resultDesc = pstmt.getMetaData();
							if (resultDesc == null) {
								putMessage('n');
							} else {
								sendRowDescription(resultDesc);
							}
						}
						break;
					}
					default:
						throw new RuntimeException("Invalid describe message subtype " + describeType);
					}
					break;
				}
				case 'H':				/* flush */
					break;
				case 'S':				/* sync */
					sendReadyForQuery = true;
					break;
				case 'd':				/* copy data */
				case 'c':				/* copy done */
				case 'f':				/* copy fail */
				case 'F':				/* fastpath function call */
				default:
					throw new RuntimeException("Unknown message " + op);
				}
			}
		}
		catch (Exception x)	{
			x.printStackTrace();
			System.err.println("Session error: " + x);
		}
	}
}
