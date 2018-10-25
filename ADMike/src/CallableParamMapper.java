/* ===================================================================*/
/* © 2015 Fresenius Medical Care Holdings, Inc. All rights reserved.     */
/* ===================================================================*/
package com.spectra.symfonie.dataaccess.framework;

import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * This is a interface which contains the method to map parameters.
 * 
 */
public interface CallableParamMapper {

	void mapParam(CallableStatement callableStatement) throws SQLException;
}
