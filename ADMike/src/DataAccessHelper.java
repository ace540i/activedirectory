/* ===================================================================*/
/* © 2015 Fresenius Medical Care Holdings, Inc. All rights reserved.  */
/* ===================================================================*/
package com.spectra.symfonie.dataaccess.framework;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.spectra.symfonie.dataaccess.connection.ConnectionManager;
import com.spectra.symfonie.dataaccess.connection.DWConnectionManager;
import com.spectra.symfonie.framework.util.ApplicationProperties;


/**
 * This is a helper class which has methods to create and execute prepare
 * statements to get the required objects from the result set.
 */
public final class DataAccessHelper {

	/**
	 * Private constructor to avoid instantiation.
	 */
	private DataAccessHelper() {
	}

	private static int fetchDataSize = 0;

	static {
		String fetchSize = ApplicationProperties
				.getProperty("DATA_ACCESS_FETCH_SIZE");
		if (fetchSize != null) {
			fetchDataSize = Integer.parseInt(fetchSize);
		}
	}

	/**
	 * Executes a simple select query with where clause.
	 *
	 * @param <T> - Holds the type of entity object to be retrieved.
	 * @param sql - Holds the query to be executed.
	 * @param paramMapper - Holds the parameter mapping for the prepared
	 *            statement to be excuted.
	 * @param resultMapper - Holds the column to attribute mapping for the
	 *                      exceuted query.
	 * @return - List of mapped objects.
	 * @throws SQLException
	 */
	public static <T> List<T> executeQuery(final String sql,
            final ParamMapper paramMapper, final ResultMapper<T> resultMapper)
            throws SQLException {
        final List<T> list = new ArrayList<T>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = ConnectionManager.getInstance().getConnection();
            preparedStatement = connection.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setFetchSize(fetchDataSize);
            paramMapper.mapParam(preparedStatement);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                T object = resultMapper.map(resultSet);
                list.add(object);
            }
        } finally {
            closeConnection(resultSet, preparedStatement, null, null,
                    connection);
        }
       return list;
	}

	/**
	 * Executes a update with parameters.
	 *
	 * @param sql - Holds the query to be executed.
	 * @param paramMapper - Holds the parameter mapping for the prepared
	 *            statement to be excuted.
	 * @return - Number of rows updated.
	 * @throws SQLException
	 */
	public static int executeUpdate(final String sql,
			final ParamMapper paramMapper) throws SQLException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        int updatedRows;

        try {
            connection = ConnectionManager.getInstance().getConnection();
            preparedStatement = connection.prepareStatement(sql);
            paramMapper.mapParam(preparedStatement);
            updatedRows = preparedStatement.executeUpdate();
        } finally {
            closeConnection(null, preparedStatement, null, null, connection);
        }
        return updatedRows;
    }

	/**
	 * Executes a update query in the batch.
	 *
	 * @param <T> - Holds the type of entity object to be retrieved.
	 * @param sql - Holds the query to be executed.
	 * @param paramMapper - Holds the parameter mapping for the prepared
	 *            statement to be excuted.
	 * @return - int array containing the number of rows updated.
	 * @throws SQLException
	 */
	public static <T> int[] executeBatch(final String sql,
			final ParamMapper paramMapper) throws SQLException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        int[] intArray;
        try {
            connection = ConnectionManager.getInstance().getConnection();
            preparedStatement = connection.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            paramMapper.mapParam(preparedStatement);
            preparedStatement.setFetchSize(fetchDataSize);
            intArray = preparedStatement.executeBatch();
        } finally {
            closeConnection(null, preparedStatement, null, null, connection);
        }
        return intArray;
    }

	/**
	 * Executes a simple select query with where clause.
	 *
	 * @param <T> - Holds the type of entity object to be retrieved.
	 * @param sql - Holds the query to be executed.
	 * @param resultMapper - Holds the column to attribute mapping for the
	 *                      exceuted query.
	 * @param connection - Holds the connection.
	 * @return - List of mapped objects.
	 * @throws SQLException
	 */
	public static <T> List<T> executeQuery(final String sql,
			final ResultMapper<T> resultMapper) throws SQLException {
        final List<T> list = new ArrayList<T>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {

            connection = ConnectionManager.getInstance().getConnection();
            preparedStatement = connection.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setFetchSize(fetchDataSize);
            resultSet = preparedStatement.executeQuery(sql);
            while (resultSet.next()) {
                T object = resultMapper.map(resultSet);
                list.add(object);
            }
        } finally {
            closeConnection(resultSet, preparedStatement, null, null,
                    connection);
        }
        return list;
    }

	
	/**
	 * Executes a stored procedure and retuns a List.
	 *
	 * @param sql - Holds the query to be executed.
	 * @param parameter
	 * @throws SQLException
	 */
	public static <T> List<T> executeProcedure(final String sql,final CallableParamMapper paramMapper,final ResultMapper<T> resultMapper)
			throws SQLException {
		 final List<T> list = new ArrayList<T>();
        Connection connection = null;
       CallableStatement callableStatement = null;
        ResultSet resultSet = null;

        try {
            connection = ConnectionManager.getInstance().getConnection();
            callableStatement = connection.prepareCall(sql);
            paramMapper.mapParam(callableStatement);
            callableStatement.execute();
            resultSet = (ResultSet)callableStatement.getObject(1); 
            while(resultSet.next()) {                                         
                T object = resultMapper.map(resultSet);
                list.add(object);      
             }		 
        } finally {
            closeConnection(null, null, callableStatement, null, connection);
        }
        return list;
    }
	
	
	
	
	
	
	
	
	
	
	
	/**
	 * Executes a update only.
	 *
	 * @param sql - Holds the query to be executed.
	 * @return - Number of rows updated.
	 * @throws SQLException
	 */
	public static int executeUpdate(final String sql) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        int updatedRows;
        try {
            connection = ConnectionManager.getInstance().getConnection();
            statement = connection.createStatement();
            statement.setFetchSize(fetchDataSize);
            updatedRows = statement.executeUpdate(sql);
        } finally {
            closeConnection(null, null, null, statement, connection);
        }
        return updatedRows;
    }

	/**
	 * Executes a stored procedure.
	 *
	 * @param sql - Holds the query to be executed.
	 * @param parameter
	 * @throws SQLException
	 */
	public static void executeProcedure(final String sql, final String parameter)
			throws SQLException {
        Connection connection = null;
        CallableStatement callableStatement = null;

        try {
            connection = ConnectionManager.getInstance().getConnection();
            callableStatement = connection.prepareCall(sql);
            callableStatement.setString(1, parameter);
            callableStatement.execute();
        } finally {
            closeConnection(null, null, callableStatement, null, connection);
        }
    }


	
	
	
	
	/**
     * Executes a stored procedure.
     * 
     * @param sql -
     *            Holds the query to be executed.
     * @param paramMapper -
     *            Holds the parameter mapping for the prepared statement to be
     *            excuted.
     * @throws SQLException
     */
	public static void executeProcedure(final String sql,
            final CallableParamMapper paramMapper) throws SQLException {
        Connection connection = null;
        CallableStatement callableStatement = null;
        try {
            connection = ConnectionManager.getInstance().getConnection();
            callableStatement = connection.prepareCall(sql);
            paramMapper.mapParam(callableStatement);
            callableStatement.execute();
        } finally {
            closeConnection(null, null, callableStatement, null, connection);
        }
    }


	/**
	 * Executes a simple select query with where clause.
	 *
	 * @param <T> - Holds the type of entity object to be retrieved.
	 * @param sql - Holds the query to be executed.
	 * @param paramMapper - Holds the parameter mapping for the prepared
	 *            statement to be excuted.
	 * @param resultMapper - Holds the column to attribute mapping for the
	 *                      exceuted query.
	 * @param hashkey - Holds the key for the HashMap.
	 * @return - HashMap that holds UserInfo object.
	 * @throws SQLException
	 */
	public static <T>HashMap<Long, T> executeUserQuery(final String sql,
			final ParamMapper paramMapper, final ResultMapper<T> resultMapper,
			final String hashkey)
			throws SQLException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        HashMap<Long, T> userMap = new HashMap<Long, T>();
        try {
            connection = ConnectionManager.getInstance().getConnection();
            preparedStatement = connection.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setFetchSize(fetchDataSize);
            paramMapper.mapParam(preparedStatement);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                T object = resultMapper.map(resultSet);
                userMap.put(resultSet.getLong(hashkey), object);
            }
        } finally {
            closeConnection(resultSet, preparedStatement, null, null,
                    connection);
        }
        return userMap;
    }
    
    /**
     * Closes the connection.
     *
     * @param resultSet - Holds the ResultSet object.
     * @param preparedStatement - Holds the PreparedStatement object.
     * @param callableStatement - Holds the CallableStatement object.
     * @param statement - Holds the Statement object.
     * @param connection - Holds the Connection object.
     * @throws SQLException
     */
    private static void closeConnection(final ResultSet resultSet,
            final PreparedStatement preparedStatement,
            final CallableStatement callableStatement,
            final Statement statement, final Connection connection)
            throws SQLException {
        if (resultSet != null) {
            resultSet.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (statement != null) {
            statement.close();
        }
        if (callableStatement != null) {
            callableStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
    
    /**
     * Executes a simple select query with where clause.
     *
     * @param <T> - Holds the type of entity object to be retrieved.
     * @param sql - Holds the query to be executed.
     * @param paramMapper - Holds the parameter mapping for the prepared
     *            statement to be excuted.
     * @param resultMapper - Holds the column to attribute mapping for the
     *                      exceuted query.
     * @return - List of mapped objects.
     * @throws SQLException
     */
    public static <T> List<T> executeQueryForDW(final String sql,
            final ParamMapper paramMapper, final ResultMapper<T> resultMapper)
            throws SQLException {
        final List<T> list = new ArrayList<T>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = DWConnectionManager.getInstance().getConnection();
            preparedStatement = connection.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setFetchSize(fetchDataSize);
            paramMapper.mapParam(preparedStatement);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                T object = resultMapper.map(resultSet);
                list.add(object);
            }
        } finally {
            closeConnection(resultSet, preparedStatement, null, null,
                    connection);
        }
        return list;
    }
    
    
    /**
	 * Executes a simple select query with where clause.
	 *
	 * @param <T> - Holds the type of entity object to be retrieved.
	 * @param sql - Holds the query to be executed.
	 * @param resultMapper - Holds the column to attribute mapping for the
	 *                      exceuted query.
	 * @return - List of mapped objects.
	 * @throws SQLException
	 */
	public static <T> List<T> executeQueryForDW(final String sql,
			final ResultMapper<T> resultMapper) throws SQLException {
		final List<T> list = new ArrayList<T>();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			connection = DWConnectionManager.getInstance().getConnection();
			preparedStatement = connection.prepareStatement(sql,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			preparedStatement.setFetchSize(fetchDataSize);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				T object = resultMapper.map(resultSet);
				list.add(object);
			}
		} finally {
			closeConnection(resultSet, preparedStatement, null, null,
					connection);
		}
		return list;
	}
	
	/**
	 * Executes a Batch for a stored  procedure.
	 * 
	 * @param sql -
	 *            Holds the query to be executed.
	 * @param paramMapper -
	 *            Holds the parameter mapping for the prepared statement to be
	 *            excuted.
	 * @throws SQLException
	 */
	public static void executeBatchProcedure(final String sql,
			final CallableParamMapper paramMapper) throws SQLException {
		Connection connection = null;
		CallableStatement callableStatement = null;
		try {
			connection = ConnectionManager.getInstance().getConnection();
			callableStatement = connection.prepareCall(sql);
			paramMapper.mapParam(callableStatement);
			callableStatement.executeBatch();
		} finally {
			closeConnection(null, null, callableStatement, null, connection);
		}
	}
}
