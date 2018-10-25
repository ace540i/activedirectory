/* ===================================================================*/
/* © 2015 Fresenius Medical Care Holdings, Inc. All rights reserved.     */
/* ===================================================================*/
package com.spectra.symfonie.dataaccess.framework;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This is an interface which has methods to map the each row of the result set.
 *
 * @param <T> - Holds the type of domain object.
 */
public interface ResultMapper<T> {

	/**
	 * This method maps the row of the result set to the attributes of the
	 * domain object.
	 *
	 * @param resultSet Holds the set of the domain object that is to be mapped.
	 * @return Object domainObject whose attributes are mapped.
	 * @throws SQLException
	 */
	T map(ResultSet resultSet) throws SQLException;
}
