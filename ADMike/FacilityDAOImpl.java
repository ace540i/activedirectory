/* ===================================================================*/
/* © 2015 Fresenius Medical Care Holdings, Inc. All rights reserved.  */
/* ===================================================================*/
package com.spectra.symfonielabs.dao;

import com.spectra.symfonie.common.constants.CommonConstants;
import com.spectra.symfonie.common.util.StringUtil;
import com.spectra.symfonie.dataaccess.connection.ConnectionManager;
import com.spectra.symfonie.dataaccess.framework.CallableParamMapper;
import com.spectra.symfonie.dataaccess.framework.DataAccessHelper;
import com.spectra.symfonie.dataaccess.framework.ParamMapper;
import com.spectra.symfonie.dataaccess.framework.ResultMapper;
import com.spectra.symfonie.framework.exception.BusinessException;
import com.spectra.symfonielabs.domainobject.FacilityDemographics;
import com.spectra.symfonielabs.domainobject.FacilitySearch;
import com.spectra.symfonielabs.domainobject.Patient;
import com.spectra.symfonielabs.domainobject.RequisitionDetails;
import com.spectra.symfonielabs.domainobject.Search;

import java.sql.CallableStatement;
import java.sql.Connection;
//import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.jdbc.OracleTypes;

/**
 * This interface contains methods that is used to have patient related
 * activities.
 * 
 */
public class FacilityDAOImpl implements FacilityDAO {

	/**
	 * Result mapper to map the result set to search object.
	 */
	private static final ResultMapper<Search> FAC_DET_MAPPER = 
			new ResultMapper<Search>() {
		public Search map(final ResultSet resultSet) throws SQLException {
			final Search searchDet = new Search(null,
					resultSet.getString("FACILITY_NAME"),
					resultSet.getLong("FACILITY_PK"),
					resultSet.getString("FACILITY_ID"),
					resultSet.getString("FACILITY_NAME"));
			return searchDet;
		}
	};

	/**
	 * Query to get the search details.
	 */
	
	private static final String GET_FACILITY_DETAILS = new StringBuffer()
	.append("SELECT distinct df.corporate_acronym || ' - ' || DF.DISPLAY_NAME as FACILITY_NAME, ")
	.append("DF.FACILITY_ID AS FACILITY_ID, DF.FACILITY_PK, df.name as fac_name FROM ")
	.append("IH_DW.DIM_FACILITY DF JOIN ih_dw.dim_account da ON da.facility_fk = ")
	.append("df.facility_pk JOIN ih_dw.dim_client dc ON dc.client_pk = df.client_fk ")
	.append( " WHERE UPPER(DISPLAY_NAME) LIKE ? ")
	.append( " and da.account_type IN ")
	.append( "('STUDY','DRAW STATION','IN-HOUSE EAST', 'IN-HOUSE WEST','SPECTRA WEST', ")
	.append( "'SPECTRA EAST') ")
	.append( "AND upper(da.account_status) IN ('ACTIVE','IN PROGRESS','TRANSFERRED') ")
	.append( "order by fac_name asc").toString(); 

	/**
	 * Gets the search results for the facility.
	 * 
	 * @param searchName
	 *            - Holds the search facility name.
	 * @return List<Search> - Holds the list of search details.
	 * 
	 */
	public List<Search> getfacilities(final String searchName) {

		List<Search> searchDetList = new ArrayList<Search>();
		final ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				preparedStatement.setString(1, "%" + searchName.toUpperCase()
						+ "%");
			}
		};
		try {
			System.out.println("GET_FACILITY_DETAILS "+GET_FACILITY_DETAILS);
			searchDetList = DataAccessHelper.executeQuery(GET_FACILITY_DETAILS,
					paramMapper, FAC_DET_MAPPER);
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		return searchDetList;
	}
	/**
	 * Query to get the dashboard details.
	 */
	private static final String GET_DASHBOARD_DETAILS = new StringBuffer()
	.append("select collection_date, upper(decode(requisition_status,'F','Complete','P','Partial Complete',requisition_status)) ")
	.append("requisition_status, patient_type_sub_group, count(*) as requisition_count, ")
	.append("sum(decode(requisition_status, 'P', 1, 0)) as partial_count, ")
	.append("sum(decode(requisition_status, 'F', 1, 0)) as complete_count ")
	.append("from ih_dw.dim_lab_order dlo join ih_dw.lov_patient_type lpt ")
	.append("on lpt.patient_type = dlo.patient_type where dlo.collection_date ")
	.append("between trunc(sysdate - 7) and trunc(sysdate) ").toString(); 
	/**
	 * Result mapper to map the result set to RequisitionDetails object.
	 */
	private static final ResultMapper<RequisitionDetails> GET_DASHBOARD_MAPPER = 
			new ResultMapper<RequisitionDetails>() {
		public RequisitionDetails map(final ResultSet resultSet) throws SQLException {
			final RequisitionDetails reqDetailObj = new RequisitionDetails(
					resultSet.getString("requisition_status"),
					resultSet.getString("patient_type_sub_group"),
					resultSet.getDate("collection_date"),
					resultSet.getLong("requisition_count"));
			return reqDetailObj;
		}
	};
	public List<RequisitionDetails> getDashboardDetails(final String labComboVal,
			final String reqTypeComboVal) {
		final ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				int counter = 0;
				if (!reqTypeComboVal.equalsIgnoreCase("All")) {
					preparedStatement.setString(++counter, reqTypeComboVal);
				}
				if (!"All".equalsIgnoreCase(labComboVal)) {
					preparedStatement.setString(++counter, labComboVal);
				}
			}
		};
		List<RequisitionDetails> reqDetailsLst = new ArrayList<RequisitionDetails>();
		StringBuffer query = new StringBuffer();
		try {
			query.append(GET_DASHBOARD_DETAILS);
			if (!"All".equalsIgnoreCase(reqTypeComboVal)) {
				query.append("and patient_type_sub_group = upper(?) ");
			}
			if (!"All".equalsIgnoreCase(labComboVal)) {
				query.append("and dlo.lab_fk = (select L.lab_pk from ih_dw.DIM_LAB L "
						+ "where L.lab_id = ?)");
			}
			query.append("group by collection_date, requisition_status, patient_type_sub_group "
					+ "order by collection_date asc");
			System.out.println("GET_DASHBOARD_DETAILS "+query.toString());
			reqDetailsLst = DataAccessHelper.executeQuery(query.toString(), paramMapper,
					GET_DASHBOARD_MAPPER);
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		return reqDetailsLst;
	}
	private static final String GET_CORPORATIONS = new StringBuffer()
	.append( "select distinct df.corporate_acronym || ' - ' || dc.name as SelCorpName, ")
	.append( "dc.client_pk as corporation_id, ") 
	.append( "dc.name as corporation_name, dc.CORPORATE_ACRONYM as acronym from ih_dw.dim_facility df, ")
	.append( "ih_dw.dim_client dc, ih_dw.dim_account da where df.client_fk ")
	.append( "= dc.client_pk AND df.facility_pk = da.facility_fk and ")
	.append( "da.account_type IN ")
	.append( "('STUDY','DRAW STATION','IN-HOUSE EAST', 'IN-HOUSE WEST','SPECTRA WEST', ")
	.append( "'SPECTRA EAST') AND upper(da.account_status) IN ")
	.append( "('ACTIVE','IN PROGRESS','TRANSFERRED') and ")
	.append( "(upper(dc.name) like ? or upper(dc.CORPORATE_ACRONYM) like ? )") 
	.append( " order by acronym,corporation_name asc").toString();
	
	/**
	 * Result mapper to map the result set to search object.
	 */
	private static final ResultMapper<Search> CORP_DETAILS_MAPPER = 
			new ResultMapper<Search>() {
		public Search map(final ResultSet resultSet) throws SQLException {
			final Search searchDet = new Search(
					resultSet.getString("corporation_name"),
					resultSet.getLong("corporation_id"), 
					resultSet.getString("acronym"),
					resultSet.getString("selCorpName"),0L, null, null);
			return searchDet;
		}
	};
	/**
     * Gets the search results for the corporation.
     * 
     * @param searchName -
     *            Holds the search facility name.
     * @return List<Search> - Holds the list of search details.
     * 
     */
	public List<Search> getCorporations(final String searchName) {
		List<Search> searchDetList = new ArrayList<Search>();
		final ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				preparedStatement.setString(1, "%" + searchName.toUpperCase()
						+ "%");
				preparedStatement.setString(2, "%" + searchName.toUpperCase()
						+ "%");
			}
		};
		try {
			System.out.println("GET_CORPORATIONS "+GET_CORPORATIONS);
			searchDetList = DataAccessHelper.executeQuery(GET_CORPORATIONS,
					paramMapper, CORP_DETAILS_MAPPER);
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		return searchDetList;
	}
	
	/**
	 * SQL query for retreiving facility search results by one or more of the below fields
	 * 1) Corporate Acronym
	 * 2) Corporate Group Name
	 * 3) Facility
	 */
	private static final String GET_FAC_SEARCH_RESULTS = new StringBuffer()
		.append("select sl_no, total_entity_cnt, facility_id, facility_name, corporation_name, ")
		.append(	   "account_type, account_status, servicing_lab, hlab_number, acc_facility_id ")
		.append("from (")
		.append(  "select rownum sl_no, total_entity_cnt, facility_id, facility_name, ")
		.append(		 "corporation_name, account_type, account_status, servicing_lab, ")
		.append(		 "hlab_number, acc_facility_id ")
		.append(  "from (")
		.append(	"SELECT distinct f.facility_pk as facility_id, f.display_name as facility_name, ")
		.append(		   "c.name as corporation_name, a.account_type, a.account_status, ")
		.append(		   "decode(f.east_west_flag, 'SE', 'Rockleigh', 'SW', 'Milpitas', f.east_west_flag) AS servicing_lab, ")
		.append(		   "count(distinct f.facility_pk) over (partition by null) as total_entity_cnt, ")
		.append(		   "a.hlab_number, a.facility_id as acc_facility_id ")
		.append(	"FROM ih_dw.dim_facility f, ")
		.append(		 "ih_dw.dim_client c, ")
		.append(		 "ih_dw.dim_account a ")
		.append(	"WHERE f.client_fk = c.client_pk ")
		.append(	  "AND f.facility_pk = a.facility_fk ")
		.append(	  "AND a.facility_id = a.hlab_number ")
		.append(	  "AND a.account_type IN ('STUDY','DRAW STATION','IN-HOUSE EAST', 'IN-HOUSE WEST','SPECTRA WEST', 'SPECTRA EAST') ")
		.append(	  "AND upper(a.account_status) IN ('ACTIVE','IN PROGRESS','TRANSFERRED') ").toString();
	
	/**
	 * Result mapper to map the result set to FacilitySearch object.
	 */
	private static final ResultMapper<FacilitySearch> FAC_SEARCH_RESULTS_MAPPER = 
			new ResultMapper<FacilitySearch>() {
		public FacilitySearch map(final ResultSet resultSet)
				throws SQLException {
			final FacilitySearch facilitySearch = new FacilitySearch(
					resultSet.getString("facility_name"),
					resultSet.getLong("facility_id"),
					resultSet.getString("corporation_name"),
					resultSet.getString("account_type"),
					resultSet.getString("account_status"),
					resultSet.getString("servicing_lab"), 
					resultSet.getLong("total_entity_cnt"), 
					resultSet.getLong("sl_no"),
					resultSet.getString("hlab_number"));
			return facilitySearch;
		}
	};
	/**
	 * Gets the facility search results based on the entered corporation name
	 * and facility name.
	 * 
	 * @param corporationId
	 *            - Holds the corporation Id.
	 * @param corporationName
	 *            - Holds the corporation name.
	 * @param facilityName
	 *            - Holds the facility name.
	 * @return List<FacilitySearch> - Holds the list of facility search details.
	 * @throws BusinessException
	 */
	public List<FacilitySearch> getFacSearchResults(final Search searchCriteria) {
		StringBuffer query = new StringBuffer();
		List<FacilitySearch> facSearchResultsLst = new ArrayList<FacilitySearch>();
		final ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				int counter = 0;
				if (searchCriteria.getCorporationId() > 0) {
					preparedStatement.setLong(++counter, searchCriteria.getCorporationId());
				}
				if (!CommonConstants.EMPTY_STRING.equalsIgnoreCase(StringUtil
						.valueOf(searchCriteria.getFacilityName()))) {
					preparedStatement.setString(++counter, "%"
							+ searchCriteria.getFacilityName().toUpperCase()
							+ "%");
				}
			}
		};
		try {
			query.append(GET_FAC_SEARCH_RESULTS);
			if (searchCriteria.getCorporationId() > 0) {
				query.append("and c.client_pk = ? ");
			}
			if (!CommonConstants.EMPTY_STRING.equalsIgnoreCase(StringUtil
					.valueOf(searchCriteria.getFacilityName()))) {
				query.append("and upper(f.display_name) like ?");
			}
			
			String sortDirection = " ASC ";
			if (searchCriteria.getSortDirection() != null
					&& searchCriteria.getSortDirection().equalsIgnoreCase("DESC")) {
				sortDirection = " DESC ";
			}			
			
			query.append(" ORDER BY ");
			
			if ( searchCriteria.getSortField() != null) {
				
				Map<String, String> columnMap = new HashMap<String, String>();
				columnMap.put("facilityName", "f.display_name");
				columnMap.put("corporationName", "c.name");
				columnMap.put("accountType", "a.account_type");
				columnMap.put("accountStatus", "a.account_status");
				columnMap.put("servicingLab", "servicing_lab");				
				
				query.append("UPPER(").append(columnMap.get(searchCriteria.getSortField())).append(") ")
				.append(sortDirection);				
				
				if (!searchCriteria.getSortField().equalsIgnoreCase("facilityName")) {					
					query.append(", UPPER(f.display_name) ASC");				
				}
				

			} else {
				query.append(" UPPER(f.display_name) ASC");				
			}			
			query.append(" ))");
			System.out.println("GET_FAC_SEARCH_RESULTS "+query.toString());
			facSearchResultsLst = DataAccessHelper.executeQuery(query.toString(),
					paramMapper, FAC_SEARCH_RESULTS_MAPPER);
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		return facSearchResultsLst;
	}
	
	private static final String GET_FAC_DEMOGRAPHICS =  new StringBuffer()
				.append("SELECT F.DISPLAY_NAME AS facility_name, c.name AS ")
				.append( "corporation_name, f.facility_pk, f.corporate_acronym ")
				.append( "|| ' - ' || f.DISPLAY_NAME AS display_fac_name, ")
				.append( "decode(f.east_west_flag, 'SE', 'Rockleigh', 'SW', 'Milpitas', ")
				.append( "f.east_west_flag) AS servicing_lab, a.type_of_service, ")
				.append( "f.clinical_rep AS NCE, f.sales_rep, f.phone_number, f.state, ")
				.append( "a.facility_id as acc_facility_id, f.time_zone, f.order_system,f.patient_report, ")	
				.append( "a.CLINICAL_MANAGER,a.MEDICAL_DIRECTOR,a.ADMINISTRATOR,a.PHONE_COMMENTS,")
				.append( "DECODE(f.ami_database, 'eCube Server A', 'A', ")
				.append( "'eCube Server B', 'B', 'eCube Server C', 'C', f.east_west_flag) ")
				.append( "AS ecube_server, ")
				.append( "TO_CHAR(NVL2 (f.open_time_mo_we_fr, f.open_time_tu_th_sa, ")
				.append( "f.open_time_sa), 'HH:MI AM') AS open_time, ")
				.append( "TO_CHAR(NVL2 (f.close_time_mo_we_fr, f.close_time_tu_th_sa, ")
				.append( "f.close_time_sa), 'HH:MI AM') AS close_time,  ")
				.append( "f.draw_week, ")
				.append( "RTRIM(NVL2(f.day_draw1, f.day_draw1 ||',',NULL) || ")
				.append( "NVL2(f.day_draw2, f.day_draw2 ||',',NULL) || ")
				.append( "NVL2(f.day_draw3, f.day_draw3 ||',',NULL) || ")
				.append( "NVL2(f.day_draw4, f.day_draw4 ||',',NULL) || ")
				.append( "NVL2(f.day_draw5, f.day_draw5 ||',',NULL),',') draw_days, ")
				.append( "DECODE(f.kits_indicator, '0', 'N', '1', 'Y', f.kits_indicator) ")
				.append( "AS kit_indicator, (f.address_line1 || ' ' || f.address_line2 || ")
				.append( "', ' || f.city || ', ' || f.state || ' ' || f.zip || ' ' || f.country ) ")
				.append( "as mailing_address, (f.phys_address_line1 || ' ' || f.phys_address_line2 ")
				.append( "|| ', ' || f.phys_city || ', ' || f.phys_state || ' ' || f.phys_zip || ' ' || f.country) as ")
				.append( "physical_address, a.alert_notes as alert_info, f.kit_comments, ")
				.append( "DECODE(f.kits_custom_indicator, '0', 'N', '1', 'Y', f.kits_custom_indicator) ")
				.append( "as kits_custom_indicator, ")
				.append( "DECODE(f.kits_cust_monthly_indicator, '0', 'N', '1', 'Y', ")
				.append( "f.kits_cust_monthly_indicator) as kits_cust_monthly_indicator, ")
				.append( "DECODE(f.kits_cust_mid_indicator, '0', 'N', '1', 'Y', ")
				.append( "f.kits_cust_mid_indicator) as kits_cust_mid_indicator, ")
				.append( "DECODE(f.kits_generic_indicator, '0', 'N', '1', 'Y', ")
				.append( "f.kits_generic_indicator) as kits_generic_indicator, ")
				.append( "DECODE(f.kits_gen_monthly_indicator, '0', 'N', '1', 'Y', ")
				.append( "f.kits_gen_monthly_indicator) as kits_gen_monthly_indicator, ")
				.append( "DECODE(f.kits_gen_monthly_indicator, '0', 'N', '1', 'Y', ")
				.append( "f.kits_gen_monthly_indicator) as kits_gen_mid_indicator, ")
				.append( "f.corporate_acronym, ")
				.append( " (CASE")
				.append( " WHEN f.internal_external_flag ='I'")
				.append( " THEN")
				.append( " (SELECT SUM(patient_count)  FROM ih_dw.dim_account")
				.append( " WHERE facility_id = f.facility_id AND ((account_category <> 'Water' or account_category is null) and  (type_of_service ='FULL' or type_of_service is null) ))")
				.append( " ELSE")
				.append( " (SELECT SUM(patient_count) FROM ih_dw.dim_account")
    			.append( " WHERE facility_id = f.facility_id AND account_category  IN ('HEMO','PD','HOME HEMO'))")
    			.append( " END) AS patient_count,")
    			.append( " (CASE")
    			.append( " WHEN f.internal_external_flag ='I'")
    			.append( " THEN")
    			.append( " (SELECT SUM(hemo_count) FROM ih_dw.dim_account")
				.append( " WHERE facility_id = f.facility_id AND ((account_category <> 'Water' or account_category is null)   and  (type_of_service ='FULL' or type_of_service is null)))")
				.append( " ELSE")
				.append( " (SELECT SUM(hemo_count) FROM ih_dw.dim_account")
    			.append( " WHERE facility_id = f.facility_id AND account_category  IN ('HEMO','PD','HOME HEMO'))")
    			.append( " END) AS hemo_count,")
    			.append( " (CASE")
    			.append( " WHEN f.internal_external_flag ='I'")
    			.append( " THEN")
				.append( " (SELECT SUM(hh_count)FROM ih_dw.dim_account")
				.append( " WHERE facility_id = f.facility_id AND ((account_category <> 'Water' or account_category is null)  and  (type_of_service ='FULL' or type_of_service is null)))")
				.append( " ELSE")
				.append( " (SELECT SUM(hh_count) FROM ih_dw.dim_account")
    			.append( " WHERE facility_id = f.facility_id AND account_category  IN ('HEMO','PD','HOME HEMO'))")
    			.append( " END) AS hh_count,")
    			.append( " (CASE")
    			.append( " WHEN f.internal_external_flag ='I'")
				.append( " THEN ")
				.append( " (SELECT SUM(pd_count) FROM ih_dw.dim_account")
				.append( " WHERE facility_id = f.facility_id AND ((account_category <> 'Water' or account_category is null)  and  (type_of_service ='FULL' or type_of_service is null)))")
				.append( " ELSE")
    		    .append( " (SELECT SUM(pd_count) FROM ih_dw.dim_account")
    			.append( " WHERE facility_id = f.facility_id AND account_category  IN ('HEMO','PD','HOME HEMO'))")
    			.append( " END) AS pd_count, ")
				.append(" f.supply_depot,f.sap_number,f.supply_delivery_sch1,f.supply_delivery_sch2,f.supply_delivery_sch3,to_char(f.first_received_date,'MM/DD/YYYY') as first_received_date ,")
				.append(" to_char(f.discontinued_date,'MM/DD/YYYY') as discontinued_date ")
				.append( "FROM ih_dw.dim_facility f, ih_dw.dim_account a, ")
				.append( "ih_dw.dim_client c WHERE f.client_fk = c.client_pk AND ")
				.append( "f.facility_pk = a.facility_fk AND f.facility_id = ")
				.append( "a.hlab_number AND f.facility_id = ?").toString();
	
	private static final ResultMapper<FacilityDemographics> 
	GET_FAC_DEMOGRAPHICS_MAPPER = new ResultMapper<FacilityDemographics>() {
		public FacilityDemographics map(final ResultSet resultSet)
				throws SQLException {
			String customIndicator = resultSet
					.getString("kits_custom_indicator");
			String genericIndicator = resultSet
					.getString("kits_generic_indicator");
			String customMonthInd = resultSet
					.getString("kits_cust_monthly_indicator");
			String customMidInd = resultSet
					.getString("kits_cust_mid_indicator");
			String genericMonthInd = resultSet
					.getString("kits_gen_monthly_indicator");
			String genericMidInd = resultSet
					.getString("kits_gen_mid_indicator");
			String customIndVal = CommonConstants.EMPTY_STRING;
			String genericIndVal = CommonConstants.EMPTY_STRING;
			String finalIndicatorVal = CommonConstants.EMPTY_STRING;
			//Differentiate the custom indicator types.
			if ("Y".equalsIgnoreCase(customIndicator)) {
				if ("Y".equalsIgnoreCase(customMonthInd)
						&& "Y".equalsIgnoreCase(customMidInd)) {
					customIndVal = "Custom Monthly, Custom Mid";
				} else if ("Y".equalsIgnoreCase(customMonthInd) && 
						"N".equalsIgnoreCase(customMidInd)) {
					customIndVal = "Custom Monthly";
				} else if ("N".equalsIgnoreCase(customMonthInd) && 
						"Y".equalsIgnoreCase(customMidInd)) {
					customIndVal = "Custom Mid";
				}
			}
			//Differentiate the generic indicator types.
			if ("Y".equalsIgnoreCase(genericIndicator)) {
				if ("Y".equalsIgnoreCase(genericMonthInd)
						&& "Y".equalsIgnoreCase(genericMidInd)) {
					genericIndVal = "Generic Monthly, Generic Mid";
				} else if ("Y".equalsIgnoreCase(genericMonthInd) && 
						"N".equalsIgnoreCase(genericMidInd)) {
					genericIndVal = "Generic Monthly";
				} else if ("N".equalsIgnoreCase(genericMonthInd) && 
						"Y".equalsIgnoreCase(genericMidInd)) {
					genericIndVal = "Generic Mid";
				}
			}
			if (CommonConstants.EMPTY_STRING.equalsIgnoreCase(customIndVal)) {
				finalIndicatorVal = genericIndVal;
			} else if (CommonConstants.EMPTY_STRING
					.equalsIgnoreCase(genericIndVal)) {
				finalIndicatorVal = customIndVal;
			}
			final FacilityDemographics facDemographicsObj = new FacilityDemographics(
					resultSet.getString("facility_name"),
					resultSet.getString("corporation_name"),
					resultSet.getString("servicing_lab"),
					resultSet.getString("type_of_service"),
					resultSet.getString("NCE"),
					resultSet.getString("sales_rep"),
					resultSet.getString("phone_number"),
					resultSet.getString("state"),
					resultSet.getString("acc_facility_id"),
					resultSet.getString("time_zone"),
					resultSet.getString("order_system"),
					resultSet.getString("ecube_server"),
					resultSet.getString("patient_report"),
					resultSet.getString("draw_week"),
					resultSet.getString("draw_days"),
					resultSet.getString("kit_indicator"),
					resultSet.getString("physical_address"),
					resultSet.getString("mailing_address"),
					resultSet.getString("alert_info"), finalIndicatorVal,
					resultSet.getString("kit_comments"), 
					resultSet.getString("corporate_acronym"),
					resultSet.getLong("facility_pk"),
					resultSet.getString("display_fac_name"),
					resultSet.getString("clinical_manager"),
					resultSet.getString("medical_director"),
					resultSet.getString("administrator"),
					resultSet.getString("phone_comments"),
					resultSet.getString("patient_count"),
					resultSet.getString("hemo_count"),
					resultSet.getString("pd_count"),
					resultSet.getString("hh_count"),
					resultSet.getString("supply_depot"),
					resultSet.getString("sap_number"),
					resultSet.getString("supply_delivery_sch1"),
					resultSet.getString("supply_delivery_sch2"),
					resultSet.getString("supply_delivery_sch3"),
					resultSet.getString("first_received_date"),
					resultSet.getString("discontinued_date"));

			return facDemographicsObj;
		}
	};
	
	public FacilityDemographics getFacDemographics(final String hLABNum) {
		List<FacilityDemographics> facDemographicsLst = 
				new ArrayList<FacilityDemographics>();
		FacilityDemographics facilityDemographics = new FacilityDemographics();
		final ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				preparedStatement.setString(1, hLABNum);
			}
		};
		try {
 			System.out.println("GET_FAC_DEMOGRAPHICS "+hLABNum+" "+ GET_FAC_DEMOGRAPHICS);
			facDemographicsLst = DataAccessHelper.executeQuery(GET_FAC_DEMOGRAPHICS,
					paramMapper, GET_FAC_DEMOGRAPHICS_MAPPER);
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		if (null != facDemographicsLst && !facDemographicsLst.isEmpty()) {
			facilityDemographics = facDemographicsLst.get(0);
		}
		return facilityDemographics;
	}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////	
// timc   US- 1963
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////	
// WAS:
//	private static final String GET_FAC_DEM_ACCOUNTS = 
//			"SELECT a.account_category, a.account_status, "
//			+ "a.account_number, a.hlab_number, a.phone_number, a.fax_number "
//			+ "FROM ih_dw.dim_account a WHERE a.facility_id = ? " 
//			+ " ORDER BY a.account_category";

	private static final String GET_FAC_DEM_ACCOUNTS =  new StringBuffer()
			.append("SELECT a.account_category, a.account_status,")
			.append("a.account_number, a.hlab_number, a.phone_number,a.fax_number")
			.append(", (CASE a.type_of_service WHEN 'FULL' THEN 0 ELSE 1 END) main_account ")	
			.append(", (CASE a.account_status WHEN 'Inactive #' THEN 1 ELSE 0 END ) status ") 
			.append(",patient_count ")
			.append("FROM ih_dw.dim_account a WHERE a.facility_id = ? " )
			.append("and a.account_type NOT IN ('HOME PATIENT', 'DRAW STATION')  " )
			.append(" ORDER BY main_account, status, a.account_status, a.account_category").toString();

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//timc 
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////	
	
	private static final ResultMapper<FacilityDemographics> 
	GET_FAC_DEM_ACCOUNTS_MAPPER = new ResultMapper<FacilityDemographics>() {
		public FacilityDemographics map(final ResultSet resultSet)
				throws SQLException {
			final FacilityDemographics facDemographicsObj = 
					new FacilityDemographics(
					resultSet.getString("account_category"),
					resultSet.getString("account_status"),
					resultSet.getString("account_number"),
					resultSet.getString("hlab_number"), 
					resultSet.getString("phone_number"), 
					resultSet.getString("fax_number"),
					resultSet.getString("patient_count"));
			return facDemographicsObj;
		}
	};
	
	public List<FacilityDemographics> getFacDemoAccountLst(final String facilityId) {
		List<FacilityDemographics> facDemoAccLst = new ArrayList<FacilityDemographics>();
		final ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				preparedStatement.setString(1, facilityId);
			}
		};
		try {
			System.out.println("GET_FAC_DEM_ACCOUNTS "+GET_FAC_DEM_ACCOUNTS);
			facDemoAccLst = DataAccessHelper.executeQuery(GET_FAC_DEM_ACCOUNTS,
					paramMapper, GET_FAC_DEM_ACCOUNTS_MAPPER);
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		return facDemoAccLst;
	}
	
	private static final String GET_FAC_SCHEDULE = new StringBuffer()
			.append("SELECT TO_CHAR(f.open_time_mo_we_fr, 'HH:MI AM')  || ' - '  || ")
			.append( "TO_CHAR(f.close_time_mo_we_fr, 'HH:MI AM') AS mon_wed_fri, ")
			.append( "TO_CHAR(f.open_time_tu_th_sa, 'HH:MI AM')  || ' - '  || ")
			.append( "TO_CHAR(f.close_time_tu_th_sa, 'HH:MI AM') AS Tue_thu_sat, ")
			.append( "TO_CHAR(f.open_time_sa, 'HH:MI AM')  || ' - '  || ")
			.append( "TO_CHAR(f.close_time_sa, 'HH:MI AM')AS sat ")
			.append( "FROM ih_dw.dim_facility f WHERE f.facility_id= ?").toString();

	private static final ResultMapper<FacilityDemographics> 
	GET_FAC_SCHEDULE_MAPPER = new ResultMapper<FacilityDemographics>() {
		public FacilityDemographics map(final ResultSet resultSet)
				throws SQLException {
			final FacilityDemographics facDemographicsObj = 
					new FacilityDemographics(
					resultSet.getString("mon_wed_fri"),
					resultSet.getString("Tue_thu_sat"),
					resultSet.getString("sat"));
			return facDemographicsObj;
		}
	};
	
	public List<FacilityDemographics> getFacDemoScheduleLst(
			final String facilityId) {
		List<FacilityDemographics> facDemoScheduleLst = 
				new ArrayList<FacilityDemographics>();
		final ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				preparedStatement.setString(1, facilityId);
			}
		};
		try {
			System.out.println("GET_FAC_SCHEDULE "+GET_FAC_SCHEDULE);
			facDemoScheduleLst = DataAccessHelper.executeQuery(GET_FAC_SCHEDULE,
					paramMapper, GET_FAC_SCHEDULE_MAPPER);
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		return facDemoScheduleLst;
	}
	
	private static final String GET_FAC_LEVEL_GRAPH = new StringBuffer()
//	.append("EXEC KORUS_REF.SPECIMEN_TRACKING_PKG.PATIENT_REQUISITION_LIST")
//    .append(" (:rc,'A118860', '04/01/2017','04/11/2017','HH~HE~PD', p_not_received=>'N' , p_in_process=>'N',")
//    .append(" p_final=>'N',p_alerts=>'N',p_cancelled_test=>'N',p_partial_received=>'N',p_order_by=>'2 asc, 5 asc')").toString();
	//.append("declare c ref ")
	.append("{call IH_DW.specimen_tracking_pkg.requisition_status(?,?,?,?)}").toString();
	/**
	 * Result mapper to map the result set to RequisitionDetails object.
	 */
	private static final ResultMapper<RequisitionDetails> GET_FAC_LEVEL_GRAPH__MAPPER = 
			new ResultMapper<RequisitionDetails>() {
		public RequisitionDetails map(final ResultSet resultSet) throws SQLException {
			final RequisitionDetails reqDetailObj = new RequisitionDetails(
				//	resultSet.getString("derived_status"),
					resultSet.getString("requisition_status"),
					resultSet.getString("patient_type_sub_group"),
					resultSet.getDate("collection_date"),
					resultSet.getLong("STATUS_COUNT"));
//			
//			PATIENT_TYPE_SUB_GROUP
//			COLLECTION_DATE
//			REQUISITION_STATUS
//			STATUS_COUNT
//			COLLECTION_DATE_TOTAL
			
			
			return reqDetailObj;
		}
	};
	/**
	 * Gets the facility level graph details based on the patient type sub group
	 * (requisition type).
	 * 
	 * @param reqTypeComboVal
	 *            - Holds the requisition type.
	 * @param facilityIdVal
	 *            - Holds the facility Id Value.
	 * @return List<RequisitionDetails> - Holds the list of graph details.
	 */
	public List<RequisitionDetails> getFacLevelGraphDetails(
			final String reqTypeComboVal, final String facilityIdVal,final String drawDateVal) {
		final CallableParamMapper paramMapper = new CallableParamMapper() {
			public void mapParam(final CallableStatement CallableStatement)
					throws SQLException {
			//	int counter = 0;
			}
		};
		List<RequisitionDetails> reqDetailsLst = new ArrayList<RequisitionDetails>();
	//	StringBuffer query = new StringBuffer();
		try {		
			SimpleDateFormat sdf = new SimpleDateFormat("mm/dd/yyyy");
			System.out.println("=============================================================== drawDateVal "+drawDateVal);
			Date drawDateVal1  = (Date)sdf.parse(drawDateVal); 
			Calendar c = Calendar.getInstance();
			c.setTime(drawDateVal1);
	        c.add(Calendar.DATE, -7);
 	        Date  drawDateVal2 = c.getTime();
	        String  drawDateVal3 = sdf.format(drawDateVal2);
 	        System.out.println("=============================================================== drawDateVal3 "+drawDateVal3);

 
			
			
			
			
			
			System.out.println("facilityIdVal = "+facilityIdVal);
			System.out.println("Req Type = "+reqTypeComboVal);
			System.out.println("Draw Date = "+drawDateVal);
			System.out.println("Draw Date3 = "+drawDateVal3);
		//	System.out.println("GET_FAC_LEVEL_GRAPH__MAPPER "+query.toString());
			Connection conn = ConnectionManager.getInstance().getConnection();
//			CallableStatement callableStatement = conn.prepareCall("{call IH_DW.specimen_tracking_pkg.requisition_status(?,?,?,?)}");
			
			CallableStatement callableStatement = conn.prepareCall("{call IH_DW.specimen_tracking_pkg.requisition_status(?,?,?,?,?)}");
			int counter = 0;
			callableStatement.registerOutParameter(++counter, OracleTypes.CURSOR);
			callableStatement.setString(++counter, reqTypeComboVal);
			callableStatement.setString(++counter, facilityIdVal);
			callableStatement.setString(++counter,drawDateVal3);
			callableStatement.setString(++counter, drawDateVal);
			
			ResultSet resultSet = null;
			
		    callableStatement.execute();
		    
	     System.out.println(" callableStatement "+callableStatement.getParameterMetaData().getParameterCount());
	     System.out.println(" callableStatement "+callableStatement.getString(1));
	
		    resultSet = (ResultSet)callableStatement.getObject(1); 
		//    ResultSetMetaData rsmd = resultSet.getMetaData();
		   // for (int i=1; i<rsmd.getColumnCount()+1; i++){
		//    	System.out.println(rsmd.getColumnName(i));
		    //	reqDetailsLst.add((RequisitionDetails) resultSet);
		//    }
//		    resultSet.next();
           while(resultSet.next()) {                                         
                RequisitionDetails object = GET_FAC_LEVEL_GRAPH__MAPPER.map(resultSet);
                reqDetailsLst.add(object);      
          }		 
		} catch (SQLException | ParseException sqlException) {
			sqlException.printStackTrace();
		}
		return reqDetailsLst;
	}
	
	/**Gets the facility level table  details based on the patient type sub group **/ 
	private static final String GET_FAC_LEVEL_TABLE = 	new StringBuffer()
			.append(" select * from (SELECT collection_date, requisition_id,draw_frequency,test_count1,patient_name,patient_type,spectra_mrn,facility_fk,")	
			.append(" upper(CASE")
			.append(" WHEN complete_count = test_count")
			.append(" THEN 'Complete'")
			.append(" WHEN complete_count > 0")
			.append(" THEN 'Partial Complete'")
			.append(" WHEN scheduled_count = test_count")
			.append(" THEN 'Scheduled'")
			.append(" WHEN in_process_count > 0 and done_count = 0")
			.append(" THEN 'In Process'")
			.append(" END) AS derived_status, cancelled_test_ind,alert_exception_ind,NumOfTubesNotReceived")     
			.append(" FROM")
			.append(" (SELECT dlo.collection_date,")
			.append(" dlo.requisition_id,")
			.append(" dlo.draw_frequency,COUNT(DISTINCT dlod.lab_order_details_pk) test_count1,")
			.append(" patient_name,")
			.append(" patient_type,")
			.append(" spectra_mrn,")
			.append(" da.facility_fk,")
			.append(" COUNT(*) test_count,")
			.append(" SUM(")
			.append(" CASE")
			.append(" WHEN clinical_status = 'S'")
			.append(" THEN 1")
			.append(" ELSE 0")
			.append(" END) scheduled_count,")
			.append(" SUM(")
			.append(" CASE")
			.append(" WHEN clinical_status IN ( 'A', 'T', 'V')")
			.append(" THEN 1")
			.append(" ELSE 0")
			.append(" END) in_process_count,")
			.append(" SUM(")
			.append(" CASE")
			.append(" WHEN clinical_status = 'D'")
			.append(" THEN 1")
			.append(" ELSE 0")
			.append(" END) done_count,")
			.append(" SUM(")
			.append(" CASE")
			.append(" WHEN clinical_status IN ('CM', 'D', 'X')")
			.append(" THEN 1")
			.append(" ELSE 0")
			.append(" END) complete_count,")
			.append(" SUM(CASE")
            .append(" WHEN (dlod.order_detail_status = 'X'")
            .append(" OR dlod.clinical_status        = 'CM')")
            .append(" AND (SELECT COUNT(order_control_reason)")
            .append(" FROM ih_dw.lov_order_control_reason")
            .append(" WHERE (order_control_reason_code   = dlod.order_control_reason_code")
            .append(" AND reportable_flag                ='1')")
            .append(" OR dlod.order_control_reason_code IS NULL) > 0")
            .append(" THEN 1")
            .append(" ELSE 0")
            .append(" END) cancelled_test_ind,")
            .append(" SUM(CASE ")
            .append(" WHEN r.derived_abnormal_flag IN ('AH','AL','EH','EL','CA','CE')")
            .append(" THEN 1")
            .append(" ELSE 0")
            .append(" END) alert_exception_ind,NumOfTubesNotReceived")
			.append(" FROM ih_dw.dim_lab_order dlo")
			.append(" JOIN ih_dw.dim_lab_order_details dlod ON dlo.lab_order_pk = dlod.lab_order_fk")          
			.append(" JOIN ih_dw.spectra_mrn_associations sma ON dlo.spectra_mrn_assc_fk = sma.spectra_mrn_assc_pk")
			.append(" JOIN ih_dw.spectra_mrn_master sm ON sma.spectra_mrn_fk = sm.spectra_mrn_pk" )
			.append(" JOIN ih_dw.dim_account da on da.account_pk=dlo.account_fk")
			.append(" JOIN ih_dw.dim_facility df on df.facility_pk=da.facility_fk")	
			.append(" LEFT OUTER JOIN ih_dw.results r ON r.lab_order_fk = dlo.lab_order_pk")
            .append(" AND r.lab_order_details_fk = dlod.lab_order_details_pk")
			.append(" LEFT OUTER JOIN")
			.append(" (SELECT requisition_id,")
			.append(" (SUM(1) - SUM(")
			.append(" CASE")
			.append(" WHEN specimen_received_date_time IS NULL")
			.append(" THEN 0")
			.append(" ELSE 1")
			.append(" END)) NumOfTubesNotReceived")
			.append(" FROM")
			.append(" (WITH accession_tbl AS")
			.append(" (SELECT dlod2.requisition_id,")
			.append(" dlod2.accession_number,")
			.append(" MIN(dlod2.specimen_received_date_time) specimen_received_date_time")
			.append(" FROM ih_dw.dim_lab_order_details dlod2")
			.append(" WHERE dlod2.test_category NOT IN ('MISC','PENDING', 'PAT')")
			.append(" and dlod2.collection_date=TO_DATE(?,'mm/dd/yyyy')")
			.append(" GROUP BY dlod2.requisition_id,")
			.append(" dlod2.accession_number")
			.append(" ),")
			.append(" container_tbl AS")
			.append(" (SELECT dlod2.requisition_id,")
			.append(" 	dlod2.accession_number,")
			.append("	dlod2.specimen_container_desc,")
			.append("	dlod2.specimen_method_desc")
			.append("	FROM ih_dw.dim_lab_order_details dlod2")
			.append("	WHERE dlod2.test_category NOT IN ('MISC','PENDING','CALC', 'PAT')")
			.append("  and dlod2.collection_date=TO_DATE(?,'mm/dd/yyyy')")
			.append("	GROUP BY dlod2.requisition_id,")
			.append("	dlod2.accession_number,")
			.append("  dlod2.specimen_container_desc,")
			.append("	dlod2.specimen_method_desc")
			.append("	)")
			.append("	SELECT accession_tbl.requisition_id,")
			.append("	accession_tbl.accession_number,")
			.append("	container_tbl.specimen_container_desc,")
			.append("	container_tbl.specimen_method_desc,")
			.append("	accession_tbl.specimen_received_date_time")
			.append("	FROM accession_tbl,")
			.append("	container_tbl")
			.append("	WHERE accession_tbl.requisition_id = container_tbl.requisition_id (+)")
			.append("	AND accession_tbl.accession_number = container_tbl.accession_number (+)")
			.append("	) tubesTbl1")
			.append("	GROUP BY tubesTbl1.requisition_id")
			.append("	) tubesTbl")
			.append("	ON tubesTbl.requisition_id = dlo.requisition_id")		
			.append("  WHERE  dlo.collection_date  = TO_DATE(?,'mm/dd/yyyy')  ")
			.append("  AND dlod.test_category NOT IN ('MISC','PENDING')")
			.append("  AND df.facility_id = ? ")
			.append("  GROUP BY dlo.collection_date,")
			.append("  dlo.requisition_id,")
			.append("  patient_name,")
			.append("  patient_type,")
			.append("  spectra_mrn,")
			.append("  da.facility_fk,")
			.append("  dlo.draw_frequency,")
			.append("	NumOfTubesNotReceived) t1")
			.append("  ) t2  ").toString();
	/**
	 * Result mapper to map the result set to RequisitionDetails object.
	 */

	private static final ResultMapper<RequisitionDetails> GET_FAC_LEVEL_TABLE_MAPPER = new ResultMapper<RequisitionDetails>() {
		public RequisitionDetails map(final ResultSet resultSet)
				throws SQLException {
			boolean abnormalFlag = false;
			boolean cancelledTestIndicator = false;
			if (0 != resultSet.getInt("alert_exception_ind")) {
				abnormalFlag = true;
			}
			if (0 != resultSet.getInt("cancelled_test_ind")) {
				cancelledTestIndicator = true;
			}
			final Patient patientResult = new Patient(
					resultSet.getString("patient_name"),
					null,
					null,
					null,
					null,
					0l, 
					null,
					resultSet.getLong("spectra_mrn"), null, 
					resultSet.getLong("facility_fk"),
					null,
					null,
					null,
					null,
					null, 0l,
					null,
					null, 0l, 0l, null, null, 0l,
					null,resultSet.getString("patient_type"));
			final RequisitionDetails reqDetails = new RequisitionDetails(
					resultSet.getString("requisition_id"), patientResult,
					resultSet.getDate("collection_date"),
					resultSet.getString("derived_status"), null,
					resultSet.getString("draw_frequency"),
					resultSet.getInt("test_count1"), abnormalFlag,
					cancelledTestIndicator,
					resultSet.getString("patient_type"), null,resultSet.getString("patient_type"));				
				    reqDetails.setNumOfTubesNotRec(resultSet.getInt("NumOfTubesNotReceived"));
			return reqDetails;
		}
	};

	/**
	 * Gets the facility level req table details based on the patient type sub group
	 * (requisition type).
	 * 
	 * @param reqTypeComboVal
	 *            - Holds the requisition type.
	 * @param facilityIdVal
	 *            - Holds the facility Id Value.
	 * @return List<RequisitionDetails> - Holds the list of graph details.
	 */
	public List<RequisitionDetails> getFacRequisitionDetails(
			final String reqTypeComboVal, final String facilityIdVal,final String drawDate,final String patType) {
		final ParamMapper paramMapper = new ParamMapper() {
		
			
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				int counter = 0;
				String patTypeSel = CommonConstants.EMPTY_STRING; 
				preparedStatement.setString(++counter,drawDate);
				preparedStatement.setString(++counter,drawDate);
				preparedStatement.setString(++counter,drawDate);	
				preparedStatement.setString(++counter, facilityIdVal);
						
			}
		};
		List<RequisitionDetails> reqDetailsLst = new ArrayList<RequisitionDetails>();
		StringBuffer query = new StringBuffer();
		try {
			
			query.append(GET_FAC_LEVEL_TABLE);

			 if (patType.equalsIgnoreCase("finalCount")){
				 query.append(" where derived_status='COMPLETE' ");				
			}else if (patType.equalsIgnoreCase("inProcessCount")){
				 query.append(" where derived_status='IN PROCESS' ");		
			} else 	if (patType.equalsIgnoreCase("completeCount")){ 
				 query.append(" where derived_status='COMPLETE' ");
	    	}else if (patType.equalsIgnoreCase("partialCount")){
	    		 query.append(" where derived_status='PARTIAL COMPLETE' ");
		    }
			
			if ("Patient".equalsIgnoreCase(reqTypeComboVal)) {
				query.append(" and patient_type NOT IN ('EN','SF')");
	    	}else if ("Environmental".equalsIgnoreCase(reqTypeComboVal)) {
				query.append(" and patient_type IN ('EN')");
			}
	    	else if ("Staff".equalsIgnoreCase(reqTypeComboVal)) {
				query.append(" and patient_type IN ('SF')");
	    	}            
			query.append(" ORDER BY collection_date DESC ");
            System.out.println("GET_FAC_LEVEL_TABLE "+query.toString());
			reqDetailsLst = DataAccessHelper.executeQuery(query.toString(), paramMapper,GET_FAC_LEVEL_TABLE_MAPPER);
			
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		return reqDetailsLst;
	}

}
