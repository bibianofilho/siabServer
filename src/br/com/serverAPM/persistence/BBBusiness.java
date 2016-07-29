package br.com.serverAPM.persistence;

import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ResourceBundle;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * Classe de persistência
 * 
 * @version 1.0 28/11/2005
 * @author Manoel Bibiano ( bibianofilho@gmail.com )
 * 
 */
public class BBBusiness {

	public static final ResourceBundle bundle = ResourceBundle
			.getBundle("Configuracoes");

	public final String delimiter = "|";

	/**
	 * Obtem uma conexão com o banco de dados
	 * 
	 * @return Connection
	 * @throws BaseException
	 */
	public Connection getConnection() throws Exception {
		Connection connection = null;
		String datasource = bundle.getString("datasource");

		try {
			Context initContext = new InitialContext();

			Context envContext = (Context) initContext.lookup("java:/comp/env");
			DataSource ds = (DataSource) envContext.lookup(datasource);

			connection = ds.getConnection();

			return connection;
		} catch (NamingException e) {
			throw new Exception("O contexto da aplicação web é inválido.");

		} catch (SQLException e) {
			throw new Exception(
					"Os parâmetros para conexão com o banco de dados estão incorretos.");
		}
		/*
		 * Connection connection = null; try {
		 * 
		 * Class.forName("org.postgresql.Driver"); connection =
		 * DriverManager.getConnection("jdbc:postgresql://localhost:5432/SIAB_V4","postgres","postgres");
		 * 
		 * return connection; } catch (SQLException e) { e.printStackTrace();
		 * throw new Exception( "Os parâmetros para conexão com o banco de dados
		 * estão incorretos."); }
		 */
	}

	/**
	 * Inicia uma transação
	 * 
	 * @return connection
	 * @throws Exception
	 */
	public Connection beginTransaction() throws Exception {
		Connection connection = getConnection();
		if (connection != null) {
			connection.setAutoCommit(false);
		}

		return connection;
	}

	/**
	 * Efetiva uma transação
	 * 
	 * @param connection
	 * @throws SQLException
	 */
	public void commitTransaction(Connection connection) throws SQLException {
		try {
			if (connection != null) {
				connection.commit();
			}
		} catch (SQLException e) {
			throw e;
		} /*finally {
			if (connection != null) {
				connection.close();
			}
		}*/
	}

	/**
	 * Aborta uma transação
	 * 
	 * @param connection
	 * @throws Exception
	 */
	public void rollbackTransaction(Connection connection) throws Exception {
		try {
			if (connection != null) {
				connection.rollback();
			}
		} catch (SQLException e) {
			throw e;
		} /*finally {
			closeConnection(connection);
		}*/
	}

	/**
	 * Método utilizado para liberar uma conexão
	 * 
	 * @param connection
	 * @throws BaseException
	 */
	public void closeConnection(Connection connection) throws Exception {
		try {
			if (connection == null) {
				return;
			}
			connection.setAutoCommit(true);
			connection.close();
		} catch (SQLException e) {
			throw new Exception(e);
		}
	}

	protected static void setInteger(int index, PreparedStatement ps, Integer i)
			throws SQLException {
		if (i == null) {
			ps.setNull(index, Types.INTEGER);
		} else {
			ps.setInt(index, i.intValue());
		}
	}

	protected static void setInt(int index, PreparedStatement ps, int i)
			throws SQLException {
		ps.setInt(index, i);
	}

	protected static void setLong(int index, PreparedStatement ps, Long i)
			throws SQLException {
		if (i == null) {
			ps.setNull(index, Types.BIGINT);
		} else {
			ps.setLong(index, i.longValue());
		}
	}

	protected static void setInteger(int index, CallableStatement cstmt,
			Integer i) throws SQLException {
		if (i == null) {
			cstmt.setNull(index, Types.INTEGER);
		} else {
			cstmt.setInt(index, i.intValue());
		}
	}

	protected static void setDate(int index, CallableStatement cstmt,
			java.util.Date date) throws SQLException {
		if (date == null) {
			cstmt.setNull(index, Types.DATE);
		} else {
			cstmt.setDate(index, new java.sql.Date(date.getTime()));
		}
	}

	protected static void setDate(int index, PreparedStatement ps,
			java.util.Date date) throws SQLException {
		if (date == null) {
			ps.setNull(index, Types.DATE);
		} else {
			ps.setDate(index, new java.sql.Date(date.getTime()));
		}
	}

	protected static void setString(int index, CallableStatement cstmt,
			String str) throws SQLException {
		if (str == null) {
			cstmt.setNull(index, Types.VARCHAR);
		} else {
			cstmt.setString(index, str);
		}
	}

	protected static void setString(int index, PreparedStatement ps, String str)
			throws SQLException {
		if (str == null) {
			ps.setNull(index, Types.VARCHAR);
		} else {
			ps.setString(index, str);
		}
	}

	protected static void setDouble(int index, CallableStatement cstmt, Double d)
			throws SQLException {
		if (d == null) {
			cstmt.setNull(index, Types.DOUBLE);
		} else {
			cstmt.setDouble(index, d.doubleValue());
		}
	}

	protected static void setDouble(int index, PreparedStatement ps, double val)
			throws SQLException {
		ps.setDouble(index, val);
	}

	protected static void setObject(int index, PreparedStatement ps,
			Object object) throws SQLException {
		if (object == null) {
			ps.setNull(index, Types.JAVA_OBJECT);
		} else {
			ps.setObject(index, object);
		}
	}

	protected static void setBinaryStream(int index, PreparedStatement ps,
			InputStream inputStream) throws SQLException {
		if (inputStream == null) {
			ps.setNull(index, Types.BLOB);
		} else {
			try {
				ps.setBinaryStream(index, inputStream, inputStream.available());
			} catch (Exception e) {
				throw new SQLException(e.getMessage());
			}
		}
	}

	protected static void setShort(int index, PreparedStatement ps, Short object)
			throws SQLException {
		if (object == null) {
			ps.setNull(index, Types.SMALLINT);
		} else {
			ps.setShort(index, object.shortValue());
		}
	}

	public static void main(String[] args) {

		BBBusiness baseDAO = new BBBusiness();
		try {
			Connection connection = baseDAO.beginTransaction();

			Statement stm = connection.createStatement();
			stm.setQueryTimeout(10);
			/*
			 * stm.executeUpdate("insert into teste values(1)");
			 * stm.executeUpdate("insert into teste values(2)");
			 */
			ResultSet rs = stm.executeQuery("select * from agente");
			int reg = 0;
			while (rs.next()) {
				reg++;
				System.out.println(reg + " > " + rs.getString(1));
			}
			rs.close();
			stm.close();
			baseDAO.closeConnection(connection);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
