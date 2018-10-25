/* ===================================================================*/
/* � 2015 Fresenius Medical Care Holdings, Inc. All rights reserved.  */
/* ===================================================================*/
package com.spectra.symfonielabs.dao;

import com.spectra.symfonie.dataaccess.framework.DataAccessHelper;
import com.spectra.symfonie.dataaccess.framework.ParamMapper;
import com.spectra.symfonie.dataaccess.framework.ResultMapper;
import com.spectra.symfonie.framework.constants.ExceptionConstants;
import com.spectra.symfonie.framework.exception.ApplicationException;
import com.spectra.symfonie.framework.logging.ApplicationRootLogger;
import com.spectra.symfonielabs.domainobject.Patient;
import com.spectra.symfonielabs.domainobject.Search;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This interface contains methods that is used to have patient related
 * activities.
 * 
 */
public class PatientDAOImpl implements PatientDAO {

	/**
	 * Logger for this class.
	 */
	private static final Logger LOGGER = ApplicationRootLogger
			.getLogger(PatientDAOImpl.class.getName());
	
	/**
	 * Result mapper to map the result set to search results object.
	 */
	private static final ResultMapper<Patient> PAT_DET_MAPPER = new ResultMapper<Patient>() {
		public Patient map(final ResultSet resultSet) throws SQLException {
			final Patient searchResultLst = new Patient(
					resultSet.getString("FULL_NAME"),
					resultSet.getString("GENDER"),
					resultSet.getString("physician_name"),
					resultSet.getDate("DOB"),
					resultSet.getString("facility_name"),
					resultSet.getLong("FACILITY_FK"),
					resultSet.getString("FACILITY_ID"),
					resultSet.getLong("SPECTRA_MRN"),
					resultSet.getString("facility_name"),
					resultSet.getLong("FACILITY_FK"),
					resultSet.getString("MODALITY"),
					resultSet.getString("SPECTRA_MRN"),
					resultSet.getString("EXTERNAL_MRN"),
					resultSet.getString("lab"),
					resultSet.getString("corp_name"), 0l, "", "", 0l, 0l, "",
					"", resultSet.getLong("total_entity_cnt"),
					resultSet.getDate("last_general_lab_date"),resultSet.getString("patient_type"));
			searchResultLst.setEntityIndex(resultSet.getLong("sl_no"));
			return searchResultLst;
		}
	};
	/**
	 * Query to get the patient details.
	 */
	public static final String GET_PAT_DET_SQL = 
		    "select sl_no, total_entity_cnt, corp_name, "
		  	+ "facility_name, facility_id, fmc_number, full_name, dob, "
		  	+ "gender, last_general_lab_date, modality, spectra_mrn, "
		  	+ "external_mrn, physician_name, lab, patient_type,FACILITY_FK from ( "
		  	+ "SELECT ROWNUM sl_no, total_entity_cnt, corp_name, "
		  	+ "facility_name, facility_id, fmc_number, full_name, dob, "
		  	+ "gender, last_general_lab_date, modality, spectra_mrn, "
		  	+ "external_mrn, physician_name, lab,patient_type, FACILITY_FK from ( "
		  	+ "SELECT dc.name corp_name, "
		  	+ "dc.corporate_acronym || ' - ' || df.display_name facility_name, "
		  	+ "df.facility_id, df.fmc_number, dp.full_name, dp.dob, dp.gender, "
			+ "dd.day AS last_general_lab_date,patient_type, "
			+ "NVL(dm.modality_description, dp.modality_code ) modality, "
			+ "dp.spectra_mrn, dp.external_mrn, dph.physician_name, "
			+ "DECODE(df.east_west_flag,'SE','Rockleigh','SW','Milpitas',df.east_west_flag) "
			+ "AS lab, DP.FACILITY_FK, COUNT (DP.SPECTRA_MRN) OVER ("
			+ "PARTITION BY null) total_entity_cnt FROM ih_dw.dim_patient dp "
			+ "JOIN ih_dw.agg_patient_max_general_lab_mv amax ON "
			+ "dp.patient_pk = amax.patient_fk JOIN ih_dw.dim_date dd "
			+ "ON dd.date_pk = amax.collection_date_fk JOIN "
			+ "ih_dw.dim_facility df ON df.facility_pk = "
			+ "dp.facility_fk JOIN ih_dw.dim_client dc ON "
			+ "dc.client_pk = df.client_fk LEFT OUTER JOIN "
			+ "ih_dw.dim_modality dm ON dm.modality_code = "
			+ "dp.modality_code LEFT OUTER JOIN "
			+ "ih_dw.dim_physician dph ON dph.physician_pk = "
			+ "dp.physician_fk "
			+ "JOIN ih_dw.spectra_mrn_master sm "
			+ " ON dp.spectra_mrn_fk = sm.spectra_mrn_pk ";


	/**
	 * Gets the search results for the patient.
	 * 
	 * @param facilityId
	 *            - Holds the facility Id.
	 * @param patName
	 *            - Holds the patient name.
	 * @return list of SearchResult - Holds the search details.
	 * 
	 */
	public List<Patient> getSearchResult(final Search patientSearch) {
		
		//timc
		//System.out.println(new java.util.Date()+" - #### PatientDAOImpl.getSearchResult @@@@   #-> patientSearch - "+patientSearch.toString());
		//System.out.println(new java.util.Date()+" - #### PatientDAOImpl.getSearchResult @@@@   #-> patientSearch.getSearchValue() - "+patientSearch.getSearchValue());
		//System.out.println(new java.util.Date()+" - #### PatientDAOImpl.getSearchResult @@@@   #-> patientSearch.getFacilityId() - "+patientSearch.getFacilityId());
		//System.out.println(new java.util.Date()+" - #### PatientDAOImpl.getSearchResult @@@@   #-> patientSearch.getSortField() - "+patientSearch.getSortField());
		//System.out.println(new java.util.Date()+" - #### PatientDAOImpl.getSearchResult @@@@   #-> patientSearch.getpatientDOB() - "+patientSearch.getpatientDOB());
		
		List<Patient> patientList;
		ParamMapper paramMapper = new ParamMapper() {

			public void mapParam(PreparedStatement preparedStatement)
					throws SQLException {
				int i = 0;
				if (patientSearch.getSearchValue().length() > 0) {
					
					// timc 2/24/2016  :  replaceAll("\\s+", " ").replaceAll("\\s", "%")
					//	preparedStatement.setString(++i, patientSearch.getSearchValue().trim().toUpperCase().concat("%"));
					String sp = patientSearch.getSearchValue().replaceAll("\\s+", " ").replaceAll("\\s", "%").trim().toUpperCase();
					if (!sp.endsWith("%")) {
						sp = sp.concat("%");
					}
					preparedStatement.setString(++i, sp);					
					//// System.out.println("@@ PatientDAOImpl: (1) getSearchResult() : "+ patientSearch.getSearchValue().replaceAll("\\s+", " ").replaceAll("\\s", "%").trim().toUpperCase().concat("%"));
					// timc 2/24/2016 
					
				}
				if (patientSearch.getFacilityId() > 0) {
					preparedStatement.setLong(++i,
							patientSearch.getFacilityId());
				}
				// timc
				if (null != patientSearch.getpatientDOB() && patientSearch.getpatientDOB()  != "" ) {
					preparedStatement.setString(++i, patientSearch.getpatientDOB());
				}
				// timc
			}

		};

		StringBuffer query = new StringBuffer();
		query.append(GET_PAT_DET_SQL);
		
		StringBuffer whereClause = new StringBuffer();
		if (patientSearch.getSearchValue().length() > 0) {
			whereClause.append("UPPER(DP.FULL_NAME) LIKE ?");
		}

		if (patientSearch.getFacilityId() > 0) {
			if(null != whereClause && whereClause.length() > 0){
				whereClause.append(" AND ");
			}
			whereClause.append(" DP.FACILITY_FK = ? ");
		}
// timc
		if (null != patientSearch.getpatientDOB() && patientSearch.getpatientDOB()  != "") {
			
			//System.out.println(new java.util.Date()+" - #### PatientDAOImpl.getSearchResult @@@@   IN->> null != patientSearch.getpatientDOB() - "+patientSearch.getpatientDOB());

			if(null != whereClause && whereClause.length() > 0){
				whereClause.append(" AND ");
			}
			whereClause.append(" to_char(DP.DOB,'mmddyyyy') = ? ");
		}
// timc     
		
		
		if(null != whereClause && whereClause.length() > 0){
			query.append(" WHERE "+whereClause +" AND sm.mrn_status = 'ACTIVE' ");
		}
		query.append(" AND EXISTS (SELECT 1 FROM ih_dw.dim_account da "
				+ "WHERE da.facility_fk = df.facility_pk AND "
				+ "da.account_type IN "
				+ "('STUDY','DRAW STATION','IN-HOUSE EAST', 'IN-HOUSE WEST','SPECTRA WEST', "
				+ "'SPECTRA EAST') and upper(da.account_status) IN ('ACTIVE','IN PROGRESS','TRANSFERRED')) ");
		String sortDirection = " ASC ";
		if (patientSearch.getSortDirection() != null
				&& patientSearch.getSortDirection().equalsIgnoreCase("DESC")) {
			sortDirection = " DESC ";
		}

		if (patientSearch.getSortField() != null) {
			if (patientSearch.getSortField().equalsIgnoreCase("fullName")) {
				query.append(" ORDER BY UPPER(DP.FULL_NAME) ").append(sortDirection) .append(", to_date(last_general_lab_date,'dd-mon-yy') DESC ");
			} else if (patientSearch.getSortField().equalsIgnoreCase(
					"dateOfBirth")) {
				query.append(" ORDER BY dob ").append(sortDirection)
					.append(" , UPPER(DP.FULL_NAME) ").append(" ASC ");
			} else if (patientSearch.getSortField().equalsIgnoreCase(
					"lastDrawDate")) {
				query.append(" order by to_date(last_general_lab_date,'dd-mon-yy')").append(sortDirection)
					.append(" , UPPER(DP.FULL_NAME) ").append(" ASC ");		
			}
			 else if (patientSearch.getSortField().equalsIgnoreCase(
						"gender")) {
					query.append(" ORDER BY UPPER(gender) ").append(sortDirection)
						.append(" , UPPER(DP.FULL_NAME) ").append(" ASC ");				
			} else if (patientSearch.getSortField().equalsIgnoreCase(
						"modality")) {
					query.append(" ORDER BY UPPER(modality) ").append(sortDirection)
						.append(" , UPPER(DP.FULL_NAME) ").append(" ASC ");
					
			} else if (patientSearch.getSortField().equalsIgnoreCase(
						"facilityName")) {
					query.append(" ORDER BY UPPER(facility_name) ").append(sortDirection)
						.append(" , UPPER(DP.FULL_NAME) ").append(" ASC ");
					
			} else if (patientSearch.getSortField().equalsIgnoreCase(
						"spectraMRN")) {
					query.append(" ORDER BY TO_NUMBER(spectra_mrn) ").append(sortDirection)
						.append(" , UPPER(DP.FULL_NAME) ").append(" ASC ");
					
			}		

		} else {
			// System.out.println("$$$$$$$$$$$$$$$ sortDirection = "+sortDirection);
			// // System.out.println("$$$$$$$$$$$$$$$ patientSearch.getSortField() = "+patientSearch.getSortField());
//			query.append(" "+patientSearch.getSortField()+ " ").append(sortDirection);
//			// System.out.println(query);
			
//			query.append(" ORDER BY UPPER(DP.FULL_NAME)").append(sortDirection)
			query.append(" ORDER BY UPPER(DP.LAST_NAME),UPPER(DP.FIRST_NAME)").append(sortDirection)
//			.append(",UPPER(last_general_lab_date) DESC " );
			.append(",to_date(last_general_lab_date,'dd-mon-yy') DESC ");
		}
		
		query.append(" )) ");
		// System.out.println("Patient query "+query);
//		if (patientSearch.getStartIndex() != 0
//                && patientSearch.getEndIndex() != 0) {
//			query.append(" WHERE SL_NO BETWEEN "
//                    + patientSearch.getStartIndex() + " AND  "
//                    + patientSearch.getEndIndex());
//        }
		
		//System.out.println(new java.util.Date()+" - #### PatientDAOImpl.getSearchResult @@@@  query - "+query.toString());
		// timc
		String sp = patientSearch.getSearchValue().replaceAll("\\s+", " ").replaceAll("\\s", "%").trim().toUpperCase();
		if (sp.length()>0 && (!sp.endsWith("%"))) {
			sp = sp.concat("%");
		}
		try {
			LOGGER.log(Level.FINE, "patientSearch: with Patient Name = " + patientSearch.getSearchValue() + ", is null: " + (null != patientSearch.getSearchValue()) + ", length: " + patientSearch.getSearchValue().length() + ",  formatted to: '" + sp + "'" + 
						", Facility ID = " + patientSearch.getFacilityId() + ", DOB = " + patientSearch.getpatientDOB() +
						"\n Query = " + query.toString());
			patientList = DataAccessHelper.executeQuery(query.toString(),
					paramMapper, PAT_DET_MAPPER);
			LOGGER.log(Level.FINE,"patientSearch => got results");
		} catch (SQLException exception) {
			exception.printStackTrace();
			// Construct a HashMap holding parameters of this method and
			// pass it to ApplicationException for logging purpose.
			Map<String, String> hashMap = new HashMap<String, String>();
			hashMap.put("patientSearch", patientSearch.toString());
			throw new ApplicationException(ExceptionConstants.SYSTEM_ERROR,
					exception, 0L, 0L, hashMap);
		}
		return patientList;
	}

	/**
	 * Result mapper to map the result set to search results object.
	 */
	private static final ResultMapper<Patient> PATIENT_RESULT_DETAILS_MAPPER = new ResultMapper<Patient>() {
		public Patient map(final ResultSet resultSet) throws SQLException {
			final Patient patientResult = new Patient(
					resultSet.getString("full_name"),
					resultSet.getString("gender"),
					resultSet.getString("physician_name"),
					resultSet.getDate("dob"),
					resultSet.getString("facility_name"),
					resultSet.getLong("facility_fk"),
					resultSet.getString("facility_id"),
					resultSet.getLong("spectra_mrn"),
					resultSet.getString("client_name"), 0L,
					resultSet.getString("modality"), null,
					resultSet.getString("external_mrn"),
					resultSet.getString("lab"), null, 0l, "", "", 0l, 0l, null,
					null, 0l, null,resultSet.getString("patient_type"));
			return patientResult;
		}
	};

	/**
	 * Query to get the patient details.
	 */
	private static final String GET_PATIENT_RESULT_DETAILS = "SELECT dp.full_name, "
			+ "dp.dob, dp.gender, NVL(dm.modality_description, dp.modality_code )"
			+ " modality, dp.spectra_mrn, dp.external_mrn, dc.name as client_name, "
			+ "df.facility_id, df.name as facility_name, df.phone_number, dp.home_phone_number, "
			+ "dp.primary_email, dph.physician_name, "
			+ "DECODE(df.east_west_flag,'SE','Rockleigh','SW','Milpitas',df.east_west_flag) "
			+ "AS lab, dp.facility_fk,dp.patient_type FROM ih_dw.dim_patient dp JOIN"
			+ " ih_dw.dim_facility df on df.facility_pk = dp.facility_fk"
			+ " JOIN ih_dw.dim_client dc on dc.client_pk = df.client_fk "
			+ "LEFT OUTER JOIN ih_dw.dim_modality dm ON dm.modality_code = dp.modality_code "
			+ "LEFT OUTER JOIN ih_dw.dim_physician dph ON dph.physician_pk = dp.physician_fk "
			+ "WHERE dp.spectra_mrn = ? ";

	/**
	 * This method is used to display patient details.
	 * 
	 * @param facilityId
	 *            - Holds the facility id.
	 * @param spectraMRN
	 *            - Holds the spectra MRN.
	 * @return An object of PatientResult.
	 */
//	public Patient getPatientDetails(final long facilityId,	final long spectraMRN) {
	public Patient getPatientDetails(final long facilityId,	final long spectraMRN, final String patientDOB) {
		
		//timc
		//System.out.println(new java.util.Date()+" - #### PatientDAOImpl.getPatientDetails @@@@   #-> facilityId - "+facilityId);
		//System.out.println(new java.util.Date()+" - #### PatientDAOImpl.getPatientDetails @@@@   #-> spectraMRN - "+spectraMRN);
		//System.out.println(new java.util.Date()+" - #### PatientDAOImpl.getPatientDetails @@@@   #-> patientDOB - "+patientDOB);
		
		Patient patientResult = null;
		List<Patient> patientResultLst = new ArrayList<>();
		StringBuffer query = new StringBuffer();
		ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				preparedStatement.setLong(1, spectraMRN);
				if (facilityId > 0) {
				preparedStatement.setLong(2, facilityId);
			}
			}
		};
		query.append(GET_PATIENT_RESULT_DETAILS);
		if (facilityId > 0) {
			query.append("and dp.facility_fk = ?");
		}
		try {
			patientResultLst = DataAccessHelper.executeQuery(query.toString(),
					paramMapper, PATIENT_RESULT_DETAILS_MAPPER);
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		if (null != patientResultLst && !patientResultLst.isEmpty()) {
			patientResult = patientResultLst.get(0);
		}
		return patientResult;
	}
	
	/**
	 * Result mapper to map the result set to search results object.
	 */
	private static final ResultMapper<Patient> PATIENT_REQ_DETAILS_MAPPER = new ResultMapper<Patient>() {
		public Patient map(final ResultSet resultSet) throws SQLException {
			final Patient patientResult = new Patient(
					resultSet.getString("patient_name"),
					resultSet.getString("gender"),
					resultSet.getString("ordering_physician_name"),
					resultSet.getDate("patient_dob"),
					resultSet.getString("facility_name"),
					resultSet.getLong("facility_pk"), null,
					resultSet.getLong("spectra_mrn"), null, 0l,
					resultSet.getString("modality"),
					resultSet.getString("hlab_number"),
					resultSet.getString("external_mrn"), null,
					resultSet.getString("corporation_name"), 0l, "", "", 0l,
					0l, null, null, 0l, null,resultSet.getString("patient_type"));
			return patientResult;
		}
	};

	/**
	 * Query to get the patient requisition details.
	 */
	private static final String GET_PATIENT_REQ_DETAILS = "select dlo.requisition_id,"
			+ " dlo.collection_date, DECODE(dlo.requisition_status,'F',"
			+ "'Final','P','Partial',dlo.requisition_status) as "
			+ "requisition_status, dlo.patient_name, dlo.patient_dob,"
			+ "dlo.gender, NVL(dm.modality_description, dlo.modality_code ) "
			+ "modality , dlo.initiate_id, sm.spectra_mrn, dlo.chart_num,"
			+ " CASE WHEN dlo.external_mrn = sm.spectra_mrn THEN null ELSE "         
			+ "dlo.external_mrn END external_mrn, dlo.ordering_physician_name, "
			+ "da.account_number, da.hlab_number, da.name account_name,"
			+ " df.facility_id primary_facility_number, df.name facility_name,"
			+ "  dc.name corporation_name, dlo.draw_frequency, "
			+ "count(distinct dlod.lab_order_details_pk) test_count,"
			+ " sum( CASE WHEN dlod.order_detail_status = 'X' THEN 1 ELSE 0 END)"
			+ " cancelled_test_ind, sum( CASE WHEN r.abnormal_flag in "
			+ "('AH','AL','EH','EL','CA','CE') THEN 1 ELSE 0 END) "
			+ "alert_exception_ind, df.facility_pk from ih_dw.dim_lab_order dlo "
			+ "JOIN ih_dw.spectra_mrn_associations sma ON "
			+ "dlo.spectra_mrn_assc_fk = sma.spectra_mrn_assc_pk "
			+ "JOIN ih_dw.spectra_mrn_master sm ON "
			+ "sma.spectra_mrn_fk = sm.spectra_mrn_pk "
			+ "JOIN ih_dw.dim_account da ON dlo.account_fk = da.account_pk "
			+ "JOIN ih_dw.dim_facility df ON da.facility_fk = df.facility_pk "
			+ "JOIN ih_dw.dim_client dc ON df.client_fk = dc.client_pk "
			+ "JOIN ih_dw.dim_modality dm ON dm.modality_code = dlo.modality_code "
			+ "JOIN ih_dw.dim_lab_order_details dlod on"
			+ " dlod.lab_order_fk = dlo.lab_order_pk LEFT OUTER "
			+ "JOIN ih_dw.results r on r.lab_order_fk = dlo.lab_order_pk "
			+ "and r.lab_order_details_fk = dlod.lab_order_details_pk "
			+ "WHERE dlo.requisition_id = UPPER(?) AND dlod.test_category not in ('MISC','PENDING')"
			+ " group by dlo.requisition_id,"
			+ " dlo.collection_date, DECODE(dlo.requisition_status,'F'"
			+ ",'Final','P','Partial',dlo.requisition_status), dlo.patient_name,"
			+ " dlo.patient_dob, dlo.gender, NVL(dm.modality_description, "
			+ "dlo.modality_code )  , dlo.initiate_id, sm.spectra_mrn, "
			+ "dlo.chart_num, dlo.external_mrn, dlo.alternate_patient_id, "
			+ "dlo.ordering_physician_name, da.account_number, da.hlab_number, "
			+ "da.name , df.facility_id, df.name , dc.name,dlo.draw_frequency, df.facility_pk";

	/**
	 * This method is used to display requisition patient details.
	 * 
	 * @param facilityNum
	 *            - Holds the facility number.
	 * @param requisitionNum
	 *            - Holds the requisition number.
	 * @return An object of PatientResult.
	 */
	public Patient getPatientReqInfo(final String facilityNum,
			final String requisitionNum) {
		Patient patientResult = null;
		List<Patient> patientResultLst = new ArrayList<>();
		ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				preparedStatement.setString(1, requisitionNum);
			}
		};
		try {
			patientResultLst = DataAccessHelper.executeQuery(
					GET_PATIENT_REQ_DETAILS, paramMapper,
					PATIENT_REQ_DETAILS_MAPPER);
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		if (null != patientResultLst && !patientResultLst.isEmpty()) {
			patientResult = patientResultLst.get(0);
		}
		return patientResult;
	}
}
