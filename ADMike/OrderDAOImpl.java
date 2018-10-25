/* ===================================================================*/
/* � 2015 Fresenius Medical Care Holdings, Inc. All rights reserved.  */
/* ===================================================================*/
package com.spectra.symfonielabs.dao;

import com.spectra.symfonie.common.util.DateUtil;
import com.spectra.symfonie.dataaccess.framework.DataAccessHelper;
import com.spectra.symfonie.dataaccess.framework.ParamMapper;
import com.spectra.symfonie.dataaccess.framework.ResultMapper;
import com.spectra.symfonie.framework.logging.ApplicationRootLogger;
import com.spectra.symfonielabs.domainobject.Accession;
import com.spectra.symfonielabs.domainobject.EnvRequisitionDetails;
import com.spectra.symfonielabs.domainobject.FacilityDemographics;
import com.spectra.symfonielabs.domainobject.OrderSummary;
import com.spectra.symfonielabs.domainobject.Patient;
import com.spectra.symfonielabs.domainobject.RequisitionDetails;
import com.spectra.symfonielabs.domainobject.Results;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This contains the implementation methods that are available in the interface.
 * 
 */
public class OrderDAOImpl implements OrderDAO {

	/**
	 * Logger for this class.
	 */
	private static final Logger LOGGER = ApplicationRootLogger
			.getLogger(OrderDAOImpl.class.getName());
	
	/**
	 * Result mapper to map the result set to search results object.
	 */
	private static final ResultMapper<OrderSummary> ORDER_DET_MAPPER = new ResultMapper<OrderSummary>() {
		public OrderSummary map(final ResultSet resultSet) throws SQLException {
			boolean abnormalFlag = false;
			boolean cancelledTestIndicator = false;
			if (0 != resultSet.getInt("alert_exception_ind")) {
				abnormalFlag = true;
			}
			if (0 != resultSet.getInt("cancelled_test_ind")) {
				cancelledTestIndicator = true;
			}			
			final OrderSummary orderResultLst = new OrderSummary(0L,
					resultSet.getDate("collection_date"),
					resultSet.getString("requisition_id"),
					resultSet.getInt("test_count"),
					resultSet.getString("draw_frequency"),
					resultSet.getString("requisition_status"), 0, 0L,
					resultSet.getInt("NumOfTubesNotReceived"),
					null, null, null, 0, null, 0L, null,
					resultSet.getLong("total_count"),
					resultSet.getLong("sl_no"),
					abnormalFlag, cancelledTestIndicator,resultSet.getString("patient_type"));
			
			final FacilityDemographics fac = new FacilityDemographics(
					resultSet.getString("display_name"),
					resultSet.getString("corporate_acronym"));

			orderResultLst.setFacility(fac);
			
			return orderResultLst;
		}
	};

	/**
	 * Query to get the order summary details.
	 */
	private static final String GET_ORDER_DET_SQL = new StringBuffer()
	.append("SELECT sl_no, total_count, requisition_id, collection_date, requisition_status, draw_frequency, test_count, " )
	.append("cancelled_test_ind, alert_exception_ind, corporate_acronym, display_name, NumOfTubesNotReceived,patient_type " )
	.append(" FROM " )
	.append("	(SELECT rownum sl_no, total_count, requisition_id, collection_date, requisition_status, draw_frequency, test_count, cancelled_test_ind, alert_exception_ind, corporate_acronym, display_name, NumOfTubesNotReceived,patient_type ")
	.append("	FROM ")
	.append("  		( ")
	.append("SELECT COUNT (DISTINCT dlo.requisition_id) over (PARTITION BY NULL) total_count, dlo.requisition_id, ") 
	.append("dlo.collection_date, ")
	.append("DECODE(dlo.requisition_status,'F','Final', 'P','Partial',dlo.requisition_status) AS requisition_status, ")
	.append("dlo.draw_frequency,dlo.patient_type, COUNT(DISTINCT dlod.lab_order_details_pk) test_count, ")
	.append(" SUM( CASE WHEN (dlod.order_detail_status = 'X' OR dlod.clinical_status = 'CM') ")
	.append(" AND dlod.order_control_reason IS NOT NULL")
	.append(" AND ((SELECT COUNT(order_control_reason_code)")  
	.append(" FROM ih_dw.lov_order_control_reason") 
	.append(" WHERE order_control_reason = dlod.order_control_reason ") 
	.append(" AND reportable_flag='1') > 0)") 	
	.append(" THEN 1 ELSE 0 END) cancelled_test_ind,")
	.append("SUM( CASE WHEN r.derived_abnormal_flag IN ('AH','AL','EH','EL','CA','CE') ")
	.append("THEN 1 ELSE 0 END ) alert_exception_ind, ")
	.append("df.corporate_acronym, df.display_name, ")
	.append("NumOfTubesNotReceived ")
	.append("FROM ih_dw.dim_lab_order dlo ")
	.append("JOIN ih_dw.spectra_mrn_associations sma ")
	.append("ON dlo.spectra_mrn_assc_fk = sma.spectra_mrn_assc_pk ")
	.append("JOIN ih_dw.dim_facility df ")
	.append("ON sma.facility_fk = df.facility_pk ")
	.append("JOIN ih_dw.spectra_mrn_master sm ")
	.append("ON sma.spectra_mrn_fk = sm.spectra_mrn_pk ")
	.append("JOIN ih_dw.dim_lab_order_details dlod ")
	.append("ON dlod.lab_order_fk = dlo.lab_order_pk ")
	.append("LEFT OUTER JOIN ih_dw.results r ")
	.append("ON r.lab_order_fk = dlo.lab_order_pk ")
	.append("AND r.lab_order_details_fk = dlod.lab_order_details_pk ")
	.append("LEFT OUTER JOIN  ( select requisition_id, (sum(1) - sum(case when specimen_received_date_time is null then 0 else 1 end)) NumOfTubesNotReceived ")
	.append("from ")
	.append("(WITH accession_tbl AS ")
	.append("(SELECT dlod2.requisition_id, dlod2.accession_number, MIN(dlod2.specimen_received_date_time) specimen_received_date_time ")
	.append("FROM ih_dw.dim_lab_order_details dlod2 ")
	.append("WHERE dlod2.test_category NOT IN ('MISC','PENDING', 'PAT') ")
	.append("GROUP BY dlod2.requisition_id, dlod2.accession_number), ")
	.append("container_tbl AS ")
	.append("(SELECT dlod2.requisition_id, dlod2.accession_number, dlod2.specimen_container_desc, ")
	.append("dlod2.specimen_method_desc ")
	.append("FROM ih_dw.dim_lab_order_details dlod2 ")
	.append("WHERE dlod2.test_category NOT IN ('MISC','PENDING','CALC', 'PAT') ")
	.append("GROUP BY dlod2.requisition_id, dlod2.accession_number, ")
	.append("dlod2.specimen_container_desc, dlod2.specimen_method_desc) ")
	.append("SELECT accession_tbl.requisition_id, accession_tbl.accession_number, container_tbl.specimen_container_desc, ")
	.append("container_tbl.specimen_method_desc, accession_tbl.specimen_received_date_time ")
	.append("FROM accession_tbl, container_tbl ")
	.append("WHERE accession_tbl.requisition_id = container_tbl.requisition_id (+) ")
	.append("AND accession_tbl.accession_number = container_tbl.accession_number (+)) tubesTbl1 "  )   
	.append("     group by tubesTbl1.requisition_id ) tubesTbl ")   
	.append("  	ON tubesTbl.requisition_id = dlo.requisition_id ")
	.append("			WHERE trunc(dlo.collection_date) between trunc(?) and trunc(sysdate) ")
	.append("	AND dlod.test_category NOT IN ('MISC','PENDING') ")
	.append("				AND sm.spectra_mrn = ? ")
	.append("		GROUP BY dlo.requisition_id, dlo.collection_date, NumOfTubesNotReceived, df.corporate_acronym, df.display_name,")
	.append("			DECODE(dlo.requisition_status,'F','Final','P','Partial',dlo.requisition_status) ,")
	.append("	dlo.draw_frequency,dlo.Patient_type  ")
	.append("ORDER BY dlo.collection_date DESC))  ").toString();  
	
	/**
	 * This method is used to get all the order details.
	 * 
	 * @param facilityId
	 *            - Holds the facility id.
	 * @param spectraMRN
	 *            - Holds the spectra MRN.
	 * @param drawDate
	 *            - Holds the draw date.
	 * @return A list of OrderSummary object.
	 */
	public List<OrderSummary> getOrderSummary(final long facilityId,
			final long spectraMRN, final Date drawDate, final OrderSummary orderSumm) {
		List<OrderSummary> orderSummaryLst = new ArrayList<>();
		StringBuffer orderSummQuery = new StringBuffer();
		orderSummQuery.append(GET_ORDER_DET_SQL);
		final String spectramrnVal= Long.toString(spectraMRN);
		ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				preparedStatement.setDate(1,DateUtil.convertUtilDateToSqlDate(drawDate));
				preparedStatement.setString(2, spectramrnVal);
			}
		};
		if (orderSumm.getStartIndex() != 0 && orderSumm.getEndIndex() != 0) {
			orderSummQuery.append(" WHERE SL_NO BETWEEN "
                    + orderSumm.getStartIndex() + " AND  "
                    + orderSumm.getEndIndex());
		}
		try {
			LOGGER.log(Level.FINE, "orderSummQuery: " +  orderSummQuery.toString());
			orderSummaryLst = DataAccessHelper.executeQuery(
					orderSummQuery.toString(), paramMapper, ORDER_DET_MAPPER);
			} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		LOGGER.log(Level.FINE, "orderSummaryLst size: " +  orderSummaryLst.size());
		return orderSummaryLst;
	}

	/**
	 * Result mapper to map the result set to search results object.
	 */
	private static final ResultMapper<Accession> TUBE_DET_MAPPER = new ResultMapper<Accession>() {
		public Accession map(final ResultSet resultSet) throws SQLException {
			final Accession tubeTypeDetLst = new Accession(
					resultSet.getString("accession_number"),
					resultSet.getString("specimen_container_desc"),
					resultSet.getString("specimen_method_desc"),
					resultSet.getTimestamp("specimen_received_date_time"),
					resultSet.getString("condition"),
					resultSet.getString("derived_status"),
					resultSet.getInt("lab_fk"));
			return tubeTypeDetLst;
		}
	};

	/**
	 * Query to get the tube type summary details.
	 */
	private static final String GET_TUBE_DET_SQL = "select "
			+ "accession_number, specimen_container_desc,"
			+ "specimen_method_desc, specimen_received_date_time,"
			+ " (select max(order_test_name ) from ih_dw.dim_lab_order_details "
			+ "where requisition_id = main_tbl.requisition_id "
			+ "and accession_number = main_tbl.accession_number and "
			+ "test_category = 'MISC') condition, CASE WHEN "
			+ "(in_process_count > 0 AND specimen_received_date_time is null ) "
			+ "THEN 'Intransit' WHEN scheduled_count = test_count THEN "
			+ "'Scheduled' WHEN in_process_count > 0 THEN 'In Process' WHEN "
			+ "cancel_count = test_count THEN 'Cancelled' WHEN "
			+ "complete_count = test_count THEN 'Done' WHEN "
			+ "done_count > 0 THEN 'Partial Complete' ELSE "
			+ "null END as derived_status,lab_fk FROM ( WITH accession_tbl AS "
			+ "(SELECT dlod.requisition_id, dlod.accession_number, "
			+ "min(dlod.specimen_received_date_time) "
			+ "specimen_received_date_time, count(*) test_count, " 
			+ "sum(CASE WHEN clinical_status = 'S' THEN 1 ELSE 0 END) "
			+ "scheduled_count, "
			+ "sum(CASE WHEN clinical_status in ( 'A', 'T', 'V') "
			+ "THEN 1 ELSE 0 END) in_process_count, "
			+ "sum(CASE WHEN clinical_status = 'D' THEN 1 ELSE 0 END) done_count, "
			+ "sum(CASE WHEN clinical_status in ('CM', 'D', 'X') THEN 1 ELSE 0 END) "
			+ "complete_count, sum(CASE WHEN clinical_status IN ('CM', 'X') THEN 1 ELSE 0 END) "
			+ "cancel_count,dlod.lab_fk FROM ih_dw.dim_lab_order_details dlod "		
			+ " WHERE "
			+ "dlod.requisition_id = UPPER(?) AND dlod.test_category "
			+ "not in ('MISC','PENDING', 'PAT')"
			+ " GROUP BY dlod.requisition_id, "
			+ "dlod.accession_number,dlod.lab_fk), container_tbl AS "
			+ "(SELECT dlod.requisition_id, dlod.accession_number,dlod.lab_fk, "
			+ "dlod.specimen_container_desc, dlod.specimen_method_desc "
			+ "FROM ih_dw.dim_lab_order_details dlod"
			+ " WHERE "
			+ "dlod.requisition_id = UPPER(?) AND dlod.test_category "
			+ "NOT IN ('MISC','PENDING','CALC', 'PAT') "
			+ " GROUP BY "
			+ "dlod.requisition_id, dlod.accession_number, "
			+ "dlod.specimen_container_desc, "
			+ "dlod.specimen_method_desc,dlod.lab_fk) SELECT accession_tbl.requisition_id, "
			+ "accession_tbl.accession_number, container_tbl.specimen_container_desc,accession_tbl.lab_fk, "
			+ "container_tbl.specimen_method_desc, "
			+ "accession_tbl.specimen_received_date_time, accession_tbl.test_count, "
			+ "accession_tbl.scheduled_count, accession_tbl.in_process_count, "
			+ "accession_tbl.done_count, accession_tbl.complete_count, "
			+ "accession_tbl.cancel_count FROM accession_tbl, "
			+ "container_tbl WHERE accession_tbl.requisition_id = "
			+ "container_tbl.requisition_id (+) AND "
			+ "accession_tbl.accession_number = "
			+ "container_tbl.accession_number (+) ) main_tbl "
			+ "ORDER BY accession_number";
     
	/**
	 * This method is used to get the tube summary details.
	 * 
	 * @param requisitionNo
	 *            - Holds the requisition number.
	 * @return A list of object OrderSummary.
	 */
	public List<Accession> getTubeSummary(final String requisitionNo) {
		List<Accession> tubeSummaryLst = new ArrayList<Accession>();
		ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				preparedStatement.setString(1, requisitionNo);
				preparedStatement.setString(2, requisitionNo);
			}
		};
		try {
			System.out.println("GET_TUBE_DET_SQL: " + GET_TUBE_DET_SQL);
			LOGGER.log(Level.FINE, "GET_TUBE_DET_SQL: " +  GET_TUBE_DET_SQL);
			tubeSummaryLst = DataAccessHelper.executeQuery(GET_TUBE_DET_SQL,
					paramMapper, TUBE_DET_MAPPER);
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		LOGGER.log(Level.FINE, "tubeSummaryLst size: " +  tubeSummaryLst.size());
		return tubeSummaryLst; 
	}
	
	/**
	 * Result mapper to map the result set to search results object.
	 */
	private static final ResultMapper<Results> ORDER_REQ_DET_MAPPER =
			new ResultMapper<Results>() {
		public Results map(final ResultSet resultSet) throws SQLException {
			final Results results = new Results(null,
					resultSet.getString("accession_number"),
					resultSet.getString("patient_type_sub_group"),
					resultSet.getString("result_test_name"),
					resultSet.getString("textual_result_full"),
					resultSet.getString("unit_of_measure"),
					resultSet.getString("reference_range"),
					resultSet.getString("result_comment"),
					resultSet.getString("abnormal_flag"),
					resultSet.getString("clinical_status"), 
					resultSet.getString("order_control_reason"),
					resultSet.getString("order_test_name"), resultSet.getString("sh_msg"), //resultSet.getString("sh_msg"));   // <<--  (timc) 
					resultSet.getInt("parent_test_count"));  
			return results;
		}
	};

	/**
	 * Query to get the order requisition details.
	 */
		private static final String GET_ORDER_REQ_DET_SQL   = new StringBuffer("select ")  
			.append( " dlod.accession_number,lpt.patient_type_sub_group patient_type_sub_group,")
			.append( "  nvl(r.order_test_name,dlod.order_test_name ) AS order_test_name, ")
			.append("nvl(r.result_test_name,dlod.order_test_name ) AS result_test_name,")
			.append(" Count(*) OVER (PARTITION")
			.append(" BY nvl(r.order_test_name,dlod.order_test_name) ) parent_test_count, ")
			.append("(CASE WHEN textual_result_full IS NULL AND order_detail_status IN ")
			.append("('I', 'O', 'P') THEN 'PENDING' when r.textual_result_full is null ")
			.append("and order_detail_status in ('X') then 'CANCELLED' ELSE ")
			.append("textual_result_full END) textual_result_full, r.unit_of_measure, r.reference_range, ")
			.append("r.derived_abnormal_flag as abnormal_flag, r.result_comment, ")
			.append("NVL(lcs.clinical_status_def, dlod.clinical_status) clinical_status, ")
			.append("dlod.order_control_reason,    NVL(sh_msg, '-') sh_msg ")
			.append(" FROM ih_dw.dim_lab_order_details dlod JOIN ih_dw.dim_order_test dot on dot.order_test_pk = dlod.order_test_fk ")  
			.append("LEFT OUTER JOIN ih_dw.results r  ")
			.append("on dlod.lab_order_details_pk = r.lab_order_details_fk ")
			.append("LEFT OUTER JOIN ih_dw.lov_clinical_status lcs on ")
			.append("lcs.clinical_status = dlod.clinical_status ")
			.append("LEFT OUTER JOIN IH_DW.DIM_LAB_ORDER dlo ON dlod.LAB_ORDER_FK = DLO.LAB_ORDER_PK")
			.append(" LEFT OUTER JOIN IH_DW.LOV_PATIENT_TYPE lpt ON dlo.patient_type   = lpt.patient_type")
.append(" LEFT OUTER JOIN  ")
.append("   ( ")
.append("     select accession_number, order_test_code, wm_concat(distinct  ")
.append("       CASE ")
.append("         WHEN (priority = 'RTN' AND event_status_code = 'S' AND entered_by = 'IF') ")
.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Order Received' ")
.append("         WHEN (priority = 'RTN' AND event_status_code = 'S' AND entered_by <> 'IF') ")
.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Order Received - '||entered_by   ")
.append("         WHEN (priority = 'Q' AND event_status_code = 'S' AND entered_by = 'IF') ")
.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Add-On Order Received' ")
.append("         WHEN (priority = 'Q' AND event_status_code = 'S' AND entered_by <> 'IF') ")
.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Add-On Order Received - '||entered_by     ")
	.append("         WHEN (priority = 'RTN' AND event_status_code = 'A' AND entered_by = 'IF') ")
	.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Tube Scanned' ")
	.append("         WHEN (priority = 'RTN' AND event_status_code = 'A' AND entered_by <> 'IF') ")
	.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Tube Scanned - '||entered_by ")  
	.append("         WHEN (priority = 'Q' AND event_status_code = 'A' AND entered_by = 'IF') ")
	.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Add-On Tube Scanned' ")
	.append("         WHEN (priority = 'Q' AND event_status_code = 'A' AND entered_by <> 'IF') ")
	.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Add-On Tube Scanned - '||entered_by ") 
.append("         WHEN (event_status_code = 'D') ")
.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Result Finalized' ")
.append("         WHEN (event_status_code = 'X' AND entered_by <> 'IF') ")
.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Cancelled - '||entered_by ")
.append("         WHEN (event_status_code = 'X' AND entered_by = 'IF') ")
.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Cancelled' ")
.append("         ELSE '*'  ")
.append("       END ) sh_msg ")
.append("     FROM  ")
.append("       (select dlod.accession_number, dlod.order_test_code, dlod.order_test_name, dlod.priority, st.event_status_code,st.entered_by, ")
.append("               REPLACE (dlod.order_test_name, ',', '-') tn,  to_char(st.status_date_time, 'mm/dd/yyyy HH24:MI AM')  dt,  DECODE(dl.lab_id,'SW','PT','SE','ET', 'HW','PT','HE','ET',  dl.lab_id) AS TZ   ")
.append("         FROM ih_dw.dim_lab_order_details dlod  ")
.append("           LEFT OUTER JOIN IH_DW.DIM_LAB_ORDER dlo ON dlod.LAB_ORDER_FK = dlo.LAB_ORDER_PK ")
.append("           JOIN ih_dw.dim_lab dl ON dlo.lab_fk = dl.lab_pk  ")
.append("         JOIN ih_dw.STATUS_HISTORY st  ON dlod.accession_number =  st.accession_number AND dlod.order_test_code = st.order_test_code ")
.append("        WHERE st.event_status_code IN('A','D','X','S') and  dlod.REQUISITION_ID = UPPER(?) ")
.append("       )  ")
.append("       group by accession_number, order_test_code ")
.append("   ) st ")
.append("   ON dlod.accession_number =  st.accession_number AND dlod.order_test_code = st.order_test_code ")	
			.append(" WHERE dlod.requisition_id = UPPER(?) ")
			.append(" AND NVL(result_test_code, 'X') != 'COLLB' ")
			.append("AND dot.micro_culture_format_flag='N' ")
			.append("AND   NVL(dlod.test_category,'X') not in ('PENDING') ")
			.append("AND   NVL(dot.order_test_type,'X') not in ( '+','-','C') ")
			.append(" ORDER BY dlod.accession_number, ")
			.append( "dlod.order_test_name, result_test_name").toString();



//.append("         WHEN (event_status_code = 'A') ")
//.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Tube Scanned'  ")		
		
		
		/**
		 * This method is used to get the order details based on requisition number.
		 * 
		 * @param requisitionNo
		 *            - Holds the requisition number.
		 * @return A list of Results.
		 */
		public List<Results> getOrderTestDetails(final String requisitionNo,
				final String reportGroupVal) {
			
			List<Results> lstResults = new ArrayList<Results>();
			ParamMapper paramMapper = new ParamMapper() {
				public void mapParam(final PreparedStatement preparedStatement)
						throws SQLException {
					preparedStatement.setString(1, requisitionNo);						// timc 8/11/2016
					preparedStatement.setString(2, requisitionNo);						// timc 8/11/2016
				}
			};
			try {
				LOGGER.log(Level.FINE, "GET_ORDER_REQ_DET_SQL: " +  GET_ORDER_REQ_DET_SQL);
				System.out.println("GET_ORDER_REQ_DET_SQL "+GET_ORDER_REQ_DET_SQL);
				lstResults = DataAccessHelper.executeQuery(GET_ORDER_REQ_DET_SQL,
						paramMapper, ORDER_REQ_DET_MAPPER);
			} catch (SQLException sqlException) {
				sqlException.printStackTrace();
			}
			LOGGER.log(Level.FINE, "lstResults size: " +  lstResults.size());
			return lstResults;
		}

		/**
		 * Result mapper to map the result set to search results object.
		 */
		private static final ResultMapper<Results> MICRO_ORDER_REQ_DET_MAPPER =
				new ResultMapper<Results>() {
			public Results map(final ResultSet resultSet) throws SQLException {

				final Results results = new Results(null,
						resultSet.getString("accession_number"),
						resultSet.getString("patient_type_sub_group"),
						resultSet.getString("result_test_name"),
						resultSet.getString("textual_result_full"),
						resultSet.getString("unit_of_measure"),
						resultSet.getString("reference_range"),
						resultSet.getString("result_comment"),
						resultSet.getString("abnormal_flag"),
						resultSet.getString("clinical_status"), 
						resultSet.getString("order_control_reason"),
						resultSet.getString("order_test_name"), resultSet.getString("sh_msg"),  // timc
						resultSet.getInt("parent_test_count"),
						resultSet.getString("report_notes"),
						resultSet.getString("specimen_method_desc"),
						resultSet.getString("specimen_source_desc"),
						resultSet.getString("seq_num"),
						resultSet.getString("micro_isolate"),
						resultSet.getString("micro_organism_name"),
						resultSet.getString("micro_sensitivity_name"),
						resultSet.getString("micro_sensitivity_flag"));  
				
				return results;
			}
		};

		/**
		 * Query to get the order requisition details.
		 */
		private static final String GET_MICRO_ORDER_REQ_DET_SQL = new StringBuffer("select ")
		.append("dlod.accession_number,lpt.patient_type_sub_group patient_type_sub_group, ")
		.append("dlod.accession_number, nvl(r.order_test_name,dlod.order_test_name ) AS order_test_name, ")
		.append("dlod.order_control_reason, dlod.report_notes, dlod.specimen_method_desc, dlod.specimen_source_desc, ")
		.append("nvl(r.result_test_name,dlod.order_test_name ) AS result_test_name, r.result_comment,  ")
		.append("case when (r.result_test_name is not null and Upper(r.result_test_name)='COLLECTION TIME') then 1 else DRT.sequence_number end seq_num, ") 
		.append("Count(*) OVER (PARTITION BY nvl(r.order_test_name,dlod.order_test_name) ) parent_test_count, ")
		.append("r.textual_result_full, r.unit_of_measure, r.reference_range, r.derived_abnormal_flag as abnormal_flag, ")
		.append("r.micro_isolate, r.micro_organism_name, r.micro_sensitivity_name, r.micro_sensitivity_flag, ")
		.append("NVL(lcs.clinical_status_def, dlod.clinical_status) clinical_status, NVL(sh_msg, '-') sh_msg  ")
		.append("FROM ih_dw.dim_lab_order_details dlod ")
		.append("	JOIN ih_dw.dim_order_test dot ")
		.append("		on dot.order_test_pk = dlod.order_test_fk ")
		.append("	LEFT OUTER JOIN ih_dw.results r  ")
		.append("		on dlod.lab_order_details_pk = r.lab_order_details_fk ")
		.append("	LEFT OUTER JOIN ih_dw.lov_clinical_status lcs ")
		.append("		on lcs.clinical_status = dlod.clinical_status ")
	  	.append("	LEFT OUTER JOIN IH_DW.DIM_RESULT_TEST DRT ")
	    .append("		ON DRT.RESULT_TEST_PK=R.RESULT_TEST_FK ")	
	    .append("	LEFT OUTER JOIN IH_DW.DIM_LAB_ORDER dlo ON dlod.LAB_ORDER_FK = DLO.LAB_ORDER_PK")
	    .append("	LEFT OUTER JOIN IH_DW.LOV_PATIENT_TYPE lpt ON dlo.patient_type   = lpt.patient_type ")
.append(" LEFT OUTER JOIN  ")
.append("  ( ")
.append("    select accession_number, order_test_code, wm_concat(distinct  ")
.append("      CASE ")
.append("        WHEN (priority = 'RTN' AND event_status_code = 'S' AND entered_by = 'IF') ")
.append("         THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Order Received' ")
.append("        WHEN (priority = 'RTN' AND event_status_code = 'S' AND entered_by <> 'IF') ")
.append("         THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Order Received - '||entered_by   ")
.append("        WHEN (priority = 'Q' AND event_status_code = 'S' AND entered_by = 'IF') ")
.append("         THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Add-On Order Received' ")
.append("        WHEN (priority = 'Q' AND event_status_code = 'S' AND entered_by <> 'IF') ")
.append("         THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Add-On Order Received - '||entered_by     ")
	.append("         WHEN (priority = 'RTN' AND event_status_code = 'A' AND entered_by = 'IF') ")
	.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Tube Scanned' ")
	.append("         WHEN (priority = 'RTN' AND event_status_code = 'A' AND entered_by <> 'IF') ")
	.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Tube Scanned - '||entered_by ")  
	.append("         WHEN (priority = 'Q' AND event_status_code = 'A' AND entered_by = 'IF') ")
	.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Add-On Tube Scanned' ")
	.append("         WHEN (priority = 'Q' AND event_status_code = 'A' AND entered_by <> 'IF') ")
	.append("          THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Add-On Tube Scanned - '||entered_by ")
.append("        WHEN (event_status_code = 'D') ")
.append("         THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Result Finalized' ")
.append("        WHEN (event_status_code = 'X' AND entered_by <> 'IF') ")
.append("         THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Cancelled - '||entered_by ")
.append("        WHEN (event_status_code = 'X' AND entered_by = 'IF') ")
.append("         THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Cancelled' ")
.append("        ELSE '*'  ")
.append("      END ) sh_msg ")
.append("    FROM  ")
.append("      (select dlod.accession_number, dlod.order_test_code, dlod.order_test_name, dlod.priority, st.event_status_code,st.entered_by, ")
.append("              REPLACE (dlod.order_test_name, ',', '-') tn,  to_char(st.status_date_time, 'mm/dd/yyyy HH24:MI AM')  dt,  DECODE(dl.lab_id,'SW','PT','SE','ET', 'HW','PT','HE','ET',  dl.lab_id) AS TZ   ")
.append("        FROM ih_dw.dim_lab_order_details dlod  ")
.append("          LEFT OUTER JOIN IH_DW.DIM_LAB_ORDER dlo ON dlod.LAB_ORDER_FK = dlo.LAB_ORDER_PK ")
.append("          JOIN ih_dw.dim_lab dl ON dlo.lab_fk = dl.lab_pk  ")
.append("        JOIN ih_dw.STATUS_HISTORY st  ON dlod.accession_number =  st.accession_number AND dlod.order_test_code = st.order_test_code ")
.append("       WHERE st.event_status_code IN('A','D','X','S') and  dlod.REQUISITION_ID = UPPER(?)  ")
.append("      )  ")
.append("      group by accession_number, order_test_code ")
.append("  ) st ")
.append("  ON dlod.accession_number =  st.accession_number AND dlod.order_test_code = st.order_test_code ")
		.append("	WHERE dlod.requisition_id = UPPER(?) ")
//		.append("	AND dot.micro_flag='Y' ")
		.append("	AND dot.micro_culture_format_flag='Y' ")
		.append("	AND NVL(dlod.test_category,'X') not in ('MISC','PENDING') ")
		.append("	AND NVL(dot.order_test_type,'X') not in ( '+','-','C') ")
		.append("ORDER BY dlod.accession_number, dlod.order_test_name, seq_num, r.micro_sensitivity_flag, r.result_sequence").toString();
		
//		.append("        WHEN (event_status_code = 'A') ")
//		.append("         THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Tube Scanned'  ")
		
		/**
		 * This method is used to get the micro order details based on requisition number.
		 * 
		 * @param requisitionNo
		 *            - Holds the requisition number.
		 * @return A list of Results.
		 */
		public List<Results> getMicroOrderTestDetails(final String requisitionNo) {
			List<Results> lstResults = new ArrayList<Results>();
			ParamMapper paramMapper = new ParamMapper() {
				public void mapParam(final PreparedStatement preparedStatement)
						throws SQLException {
					preparedStatement.setString(1, requisitionNo);
					preparedStatement.setString(2, requisitionNo);        //  timc 8/12/2016  MICRO  sh_msg
				}
			};
			try {
				System.out.println("GET_MICRO_ORDER_REQ_DET_SQL "+GET_MICRO_ORDER_REQ_DET_SQL);
				LOGGER.log(Level.FINE, "GET_MICRO_ORDER_REQ_DET_SQL: " +  GET_MICRO_ORDER_REQ_DET_SQL);
				lstResults = DataAccessHelper.executeQuery(GET_MICRO_ORDER_REQ_DET_SQL,
						paramMapper, MICRO_ORDER_REQ_DET_MAPPER);
			} catch (SQLException sqlException) {
				sqlException.printStackTrace();
			}
			LOGGER.log(Level.FINE, "lstResults size: " +  lstResults.size());
			return lstResults;
		}
		
		/**
		 * Result mapper to map the result set to search results object.
		 */
		private static final ResultMapper<RequisitionDetails> PATIENT_REQ_DETAILS_MAPPER = new ResultMapper<RequisitionDetails>() {
			public RequisitionDetails map(final ResultSet resultSet)
					throws SQLException {
				boolean abnormalFlag = false;
				boolean cancelledTestIndicator = false;
				final Patient patientResult = new Patient(
						resultSet.getString("patient_name"),
						resultSet.getString("gender"),
						resultSet.getString("ordering_physician_name"),
						resultSet.getDate("patient_dob"),
						resultSet.getString("facility_name"),
						resultSet.getLong("facility_pk"), 
						resultSet.getString("primary_facility_number"),
						resultSet.getLong("spectra_mrn"), null, 0l,
						resultSet.getString("modality"),
						resultSet.getString("initiate_id"),
						resultSet.getString("external_mrn"),
						resultSet.getString("lab_name"),
						resultSet.getString("corporation_name"), 0l,
						resultSet.getString("account_number"),
						resultSet.getString("hlab_number"), 0l, 0l, null, null, 0l,
						null,resultSet.getString("patient_type"));
				if (0 != resultSet.getInt("alert_exception_ind")) {
					abnormalFlag = true;
				}
				if (0 != resultSet.getInt("cancelled_test_ind")) {
					cancelledTestIndicator = true;
				}
				final RequisitionDetails reqDetails = new RequisitionDetails(
						resultSet.getString("requisition_id"), patientResult,
						resultSet.getDate("collection_date"),
						resultSet.getString("requisition_status"), "",
						resultSet.getString("draw_frequency"),
						resultSet.getInt("test_count"), abnormalFlag,
						cancelledTestIndicator,
						resultSet.getString("patient_type_sub_group"), null,resultSet.getString("patient_type"));
				return reqDetails;
			}
		};

	/**
	 * Query to get the patient requisition details.
	 */
	private static final String GET_PATIENT_REQ_DETAILS = new StringBuffer("select")
			.append(" dlo.requisition_id,")
			.append(" dlo.collection_date, DECODE(dlo.requisition_status,'F',")
			.append("'Final','P','Partial',dlo.requisition_status) as ")
			.append("requisition_status, dlo.patient_name, dlo.patient_dob,dlo.patient_type,")
			.append("dlo.gender, NVL(dm.modality_description, dlo.modality_code ) ")
			.append("modality , dlo.initiate_id, sm.spectra_mrn, dlo.chart_num,")
			.append(" dlo.external_mrn, dlo.ordering_physician_name, ")
			.append("da.account_number, da.hlab_number, da.name account_name,")
			.append(" df.facility_id primary_facility_number, df.display_name facility_name,")
			.append("  dc.name corporation_name, dlo.draw_frequency, lpt.patient_type_sub_group, ")
			.append(" DECODE(dl.lab_id,'SW','Milpitas','SE','Rockleigh', 'HW','Milpitas','HE','Rockleigh', ")
			.append(" dl.lab_id) AS lab_name, ")
			.append("count(distinct dlod.lab_order_details_pk) test_count,")
			.append(" SUM( CASE WHEN (dlod.order_detail_status = 'X' OR dlod.clinical_status = 'CM') ")
			.append(" AND dlod.order_control_reason IS NOT NULL")
			.append(" AND ((SELECT COUNT(order_control_reason_code)")  
			.append(" FROM ih_dw.lov_order_control_reason") 
			.append(" WHERE order_control_reason = dlod.order_control_reason") 
			.append(" AND reportable_flag='1') > 0)") 	
			.append(" THEN 1 ELSE 0 END) cancelled_test_ind,")
			.append(" sum( CASE WHEN r.derived_abnormal_flag in ")
			.append("('AH','AL','EH','EL','CA','CE') THEN 1 ELSE 0 END) ")
			.append("alert_exception_ind, df.facility_pk from ih_dw.dim_lab_order dlo ")
			.append("JOIN ih_dw.dim_lab dl ON dlo.lab_fk = dl.lab_pk ")	
			.append("JOIN ih_dw.spectra_mrn_associations sma ON ")
			.append("dlo.spectra_mrn_assc_fk = sma.spectra_mrn_assc_pk ")
			.append("JOIN ih_dw.spectra_mrn_master sm ON ")
			.append("sma.spectra_mrn_fk = sm.spectra_mrn_pk ")
			.append("JOIN ih_dw.dim_account da ON dlo.account_fk = da.account_pk ")
			.append("JOIN ih_dw.dim_facility df ON da.facility_fk = df.facility_pk ")
			.append("JOIN ih_dw.dim_client dc ON df.client_fk = dc.client_pk ")
			.append("JOIN ih_dw.dim_modality dm ON dm.modality_code = dlo.modality_code ")
			.append("JOIN ih_dw.dim_lab_order_details dlod on")
			.append(" dlod.lab_order_fk = dlo.lab_order_pk JOIN ")
			.append("ih_dw.lov_patient_type lpt ON dlo.patient_type = ")
			.append("lpt.patient_type LEFT OUTER JOIN ih_dw.results r on ")
			.append("r.lab_order_fk = dlo.lab_order_pk ")
			.append("and r.lab_order_details_fk = dlod.lab_order_details_pk ")
			.append("WHERE dlo.requisition_id = UPPER(?) AND dlod.test_category not in ")
			.append("('MISC','PENDING') group by dlo.requisition_id,")
			.append(" dlo.collection_date, DECODE(dlo.requisition_status,'F'")
			.append(",'Final','P','Partial',dlo.requisition_status), dlo.patient_name,")
			.append(" dlo.patient_dob,dlo.patient_type, dlo.gender, NVL(dm.modality_description, ")
			.append("dlo.modality_code )  , dlo.initiate_id, sm.spectra_mrn, ")
			.append("dlo.chart_num, dlo.external_mrn, dlo.alternate_patient_id, ")
			.append("dlo.ordering_physician_name, da.account_number, da.hlab_number, ")
			.append("da.name , df.facility_id, df.display_name , dc.name,dlo.draw_frequency,")
			.append(" DECODE(dl.lab_id,'SW','Milpitas','SE','Rockleigh', 'HW','Milpitas','HE',")
			.append("'Rockleigh',  dl.lab_id) , ")
			.append(" df.facility_pk, lpt.patient_type_sub_group").toString();

	/**
	 * This method is used to display requisition patient details.
	 * 
	 * @param facilityNum
	 *            - Holds the facility number.
	 * @param requisitionNum
	 *            - Holds the requisition number.
	 * @return An object of RequisitionDetails.
	 */
	public RequisitionDetails getPatientReqInfo(final String facilityNum,
			final String requisitionNum) {
		RequisitionDetails patientResult = new RequisitionDetails();
		List<RequisitionDetails> patientResultLst = new ArrayList<>();
		ParamMapper paramMapper = new ParamMapper() {
			public void mapParam(final PreparedStatement preparedStatement)
					throws SQLException {
				preparedStatement.setString(1, requisitionNum);
			}
		};
		try {
			LOGGER.log(Level.FINE, "GET_PATIENT_REQ_DETAILS: " +  GET_PATIENT_REQ_DETAILS);
			System.out.println("ORDER: GET_PATIENT_REQ_DETAILS "+GET_PATIENT_REQ_DETAILS);
			patientResultLst = DataAccessHelper.executeQuery(
					GET_PATIENT_REQ_DETAILS, paramMapper,
					PATIENT_REQ_DETAILS_MAPPER);
		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		}
		if (null != patientResultLst && !patientResultLst.isEmpty()) {
			patientResult = patientResultLst.get(0);
		}
		LOGGER.log(Level.FINE, "patientResultLst size: " +  patientResultLst.size());
		return patientResult;
	}
		
	private static final String GET_ENV_REQ_DETAILS = new StringBuffer("select")
		.append(" dlo.requisition_id,")			
		.append(" dlo.collection_date_time, DECODE(dlo.requisition_status,'F',")
		.append(" 'Final','P','Partial',dlo.requisition_status) as ")
		.append(" requisition_status, dlo.patient_name, dlo.patient_dob,dlo.patient_type,")
		.append(" dlo.gender, NVL(dm.modality_description, dlo.modality_code) ")
		.append(" modality , dlo.initiate_id, sm.spectra_mrn, dlo.chart_num,")
		.append(" dlo.external_mrn, dlo.ordering_physician_name,")
		.append(" da.account_number, da.hlab_number, da.name account_name,")
		.append(" df.facility_id primary_facility_number, df.display_name facility_name,")
		.append(" dc.name corporation_name, dlo.draw_frequency, lpt.patient_type_sub_group, ")
		.append(" DECODE(dl.lab_id,'SW','Milpitas','SE','Rockleigh', 'HW','Milpitas','HE',")
		.append(" 'Rockleigh', dl.lab_id) AS lab_name, ")
		.append(" count(distinct dlod.lab_order_details_pk) test_count,")
		.append(" SUM( CASE WHEN (dlod.order_detail_status = 'X' OR dlod.clinical_status = 'CM') ")
		.append(" AND dlod.order_control_reason IS NOT NULL")
		.append(" AND ((SELECT COUNT(order_control_reason_code)")  
		.append(" FROM ih_dw.lov_order_control_reason") 
		.append(" WHERE order_control_reason = dlod.order_control_reason") 
		.append(" AND reportable_flag='1') > 0)") 	
		.append(" THEN 1 ELSE 0 END) cancelled_test_ind,")
		.append("  sum( CASE WHEN r.derived_abnormal_flag in ")
		.append(" ('AH','AL','EH','EL','CA','CE') THEN 1 ELSE 0 END) ")
		.append(" alert_exception_ind, df.facility_pk, de.machine_name, ")
		.append( "de.serial_number ")
		.append( ", MAX(CASE WHEN r.RESULT_TEST_CODE IN ('COLLB') ")
		.append( "		      THEN r.TEXTUAL_RESULT ")
		.append( "		      ELSE '' ")
		.append( "		    END) COLLB ")			
		.append( " from ih_dw.dim_lab_order dlo ")
		.append( " JOIN IH_DW.dim_lab dl ON dlo.lab_fk = dl.lab_pk ")
		.append( "JOIN ih_dw.spectra_mrn_associations sma ON ")
		.append( "dlo.spectra_mrn_assc_fk = sma.spectra_mrn_assc_pk ")
		.append( "JOIN ih_dw.spectra_mrn_master sm ON ")
		.append( "sma.spectra_mrn_fk = sm.spectra_mrn_pk ")
		.append( "JOIN ih_dw.dim_account da ON dlo.account_fk = da.account_pk ")
		.append( "JOIN ih_dw.dim_facility df ON da.facility_fk = df.facility_pk ")
		.append( "JOIN ih_dw.dim_client dc ON df.client_fk = dc.client_pk ")
		.append("JOIN ih_dw.dim_modality dm ON dm.modality_code = dlo.modality_code ")
		.append("JOIN ih_dw.dim_lab_order_details dlod on")
		.append( " dlod.lab_order_fk = dlo.lab_order_pk JOIN ")
		.append( "ih_dw.lov_patient_type lpt ON dlo.patient_type = ")
		.append( "lpt.patient_type JOIN ih_dw.dim_equipment de ON ")
		.append("dlo.spectra_mrn_assc_fk = sma.spectra_mrn_assc_pk AND ")
		.append( "sma.spectra_mrn_fk = de.spectra_mrn_fk LEFT OUTER JOIN ")
		.append( "ih_dw.results r on r.lab_order_fk = dlo.lab_order_pk ")
		.append( "and r.lab_order_details_fk = dlod.lab_order_details_pk ")
		.append( "WHERE dlo.requisition_id = UPPER(?) AND dlod.test_category not in ")
		.append( "('MISC','PENDING') group by dlo.requisition_id,")
		.append( " dlo.collection_date_time, DECODE(dlo.requisition_status,'F'")
		.append( ",'Final','P','Partial',dlo.requisition_status), dlo.patient_name,")
		.append(" dlo.patient_dob,dlo.patient_type, dlo.gender, NVL(dm.modality_description, ")
		.append( "dlo.modality_code )  , dlo.initiate_id, sm.spectra_mrn, ")
		.append( "dlo.chart_num, dlo.external_mrn, dlo.alternate_patient_id, ")
		.append( "dlo.ordering_physician_name, da.account_number, da.hlab_number, ")
		.append( "da.name , df.facility_id, df.display_name, dc.name,dlo.draw_frequency,")
		.append( " DECODE(dl.lab_id,'SW','Milpitas','SE','Rockleigh', 'HW','Milpitas','HE',")
		.append( "'Rockleigh',  dl.lab_id) , ")
		.append(" df.facility_pk, lpt.patient_type_sub_group, ")
		.append( "de.machine_name, de.serial_number").toString();
	
		/**
		 * Result mapper to map the result set to search results object.
		 */
		private static final ResultMapper<RequisitionDetails> ENV_REQ_DETAILS_MAPPER = 
				new ResultMapper<RequisitionDetails>() {
			public RequisitionDetails map(final ResultSet resultSet)
					throws SQLException {
				boolean abnormalFlag = false;
				boolean cancelledTestIndicator = false;
				final Patient patientResult = new Patient(
						resultSet.getString("patient_name"),
						resultSet.getString("gender"),
						resultSet.getString("ordering_physician_name"),
						resultSet.getDate("patient_dob"),
						resultSet.getString("facility_name"),
						resultSet.getLong("facility_pk"),
						resultSet.getString("primary_facility_number"),
						resultSet.getLong("spectra_mrn"), null, 0l,
						resultSet.getString("modality"),
						resultSet.getString("initiate_id"),
						resultSet.getString("external_mrn"),
						resultSet.getString("lab_name"),
						resultSet.getString("corporation_name"), 0l,
						resultSet.getString("account_number"),
						resultSet.getString("hlab_number"), 0l, 0l, null, null, 0l,
						null,resultSet.getString("patient_type"));
				if (0 != resultSet.getInt("alert_exception_ind")) {
					abnormalFlag = true;
				}
				if (0 != resultSet.getInt("cancelled_test_ind")) {
					cancelledTestIndicator = true;
				}
				final EnvRequisitionDetails envRequisitionDetails = new EnvRequisitionDetails(
						resultSet.getString("machine_name"),
						resultSet.getString("serial_number"), null, null, resultSet.getString("COLLB"),  //collectedBy   // timc  //
						resultSet.getString("collection_date_time"), null, null);
		
				final RequisitionDetails reqDetails = new RequisitionDetails(
						resultSet.getString("requisition_id"), patientResult,
						resultSet.getDate("collection_date_time"),
						resultSet.getString("requisition_status"), "",
						resultSet.getString("draw_frequency"),
						resultSet.getInt("test_count"), abnormalFlag,
						cancelledTestIndicator,
						resultSet.getString("patient_type_sub_group"),
						envRequisitionDetails,resultSet.getString("patient_type"));
				
				return reqDetails;
			}
		};
		
		public RequisitionDetails getEnvReqInfo(final String facilityNum,
				final String requisitionNum) {
			RequisitionDetails patientResult = new RequisitionDetails();
			List<RequisitionDetails> patientResultLst = new ArrayList<>();
			ParamMapper paramMapper = new ParamMapper() {
				public void mapParam(final PreparedStatement preparedStatement)
						throws SQLException {
					preparedStatement.setString(1, requisitionNum);
				}
			};
			try {
				LOGGER.log(Level.FINE, "GET_ENV_REQ_DETAILS: " +  GET_ENV_REQ_DETAILS);
				System.out.println("ENV_REQ_DETAILS_MAPPER "+ENV_REQ_DETAILS_MAPPER);
				patientResultLst = DataAccessHelper.executeQuery(
						GET_ENV_REQ_DETAILS, paramMapper, ENV_REQ_DETAILS_MAPPER);
			} catch (SQLException sqlException) {
				sqlException.printStackTrace();
			}
			LOGGER.log(Level.FINE, "patientResultLst size: " +  patientResultLst.size());
			if (null != patientResultLst && !patientResultLst.isEmpty()) {
				patientResult = patientResultLst.get(0);
			}
			return patientResult;
		}
		
		private static final String GET_REPORT_TEST_DET = new StringBuffer().
				append("SELECT dlod.accession_number, NVL(r.order_test_name,dlod.order_test_name )  AS order_test_name, ").
				append(		 " NVL(r.result_test_name,dlod.order_test_name ) AS result_test_name, ").
				append(		 " COUNT(*) OVER (PARTITION BY NVL(r.order_test_name,dlod.order_test_name) ) parent_test_count, ").
				append(		 " (CASE WHEN textual_result_full IS NULL AND dlod.order_detail_status IN ('I', 'O', 'P') THEN 'PENDING' ").
				append(		 	   " when r.textual_result_full is null and dlod.order_detail_status in ('X') then 'CANCELLED' ").
				append(			   " ELSE textual_result_full END) textual_result_full, "). 
				append(		 " r.unit_of_measure, r.reference_range, r.derived_abnormal_flag AS abnormal_flag, r.result_comment, ").
				append(		 " NVL(lcs.clinical_status_def, dlod.clinical_status) clinical_status, " ).
				append(		 " dlod.order_control_reason, NVL(r.report_grouping, dot.inquiry_grouping) report_grouping, NVL(sh_msg,'-' ) sh_msg ").				
				append("FROM ih_dw.dim_lab_order_details dlod ").
				append(		" JOIN ih_dw.dim_order_test dot ON dot.order_test_pk = dlod.order_test_fk ").
				append(		" LEFT OUTER JOIN (SELECT r2.*, drt.report_grouping, sequence_number ").
				append(						 " FROM ih_dw.results r2 , ih_dw.dim_result_test drt ").
				append(						 " WHERE r2.result_test_fk = drt.result_test_pk) r ").
				append(						 " ON dlod.lab_order_details_pk = r.lab_order_details_fk "). 
				append(		" LEFT OUTER JOIN ih_dw.lov_clinical_status lcs ON lcs.clinical_status = dlod.clinical_status ").
				append(		" LEFT OUTER JOIN ih_dw.lov_report_grouping lrg ON r.report_grouping = lrg.report_grouping AND r.source_lab_system = lrg.source_lab_system ").
				append(		" LEFT OUTER JOIN " ).
				append(		" ( " ).
				append(			" select accession_number, order_test_code, wm_concat(distinct " ).
				append(					" CASE " ).
				append(					" WHEN (priority = 'RTN' AND event_status_code = 'S' AND entered_by = 'IF') ").
				append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Order Received' ").
				append(					" WHEN (priority = 'RTN' AND event_status_code = 'S' AND entered_by <> 'IF') ").
				append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Order Received - '||entered_by ").  
				append(					" WHEN (priority = 'Q' AND event_status_code = 'S' AND entered_by = 'IF') ").
				append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Add-On Order Received' ").
				append(					" WHEN (priority = 'Q' AND event_status_code = 'S' AND entered_by <> 'IF') ").
				append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Add-On Order Received - '||entered_by "). 
				append(					" WHEN (priority = 'RTN' AND event_status_code = 'A' AND entered_by = 'IF') ").
				append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Tube Scanned' ").
				append(					" WHEN (priority = 'RTN' AND event_status_code = 'A' AND entered_by <> 'IF') ").
				append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Tube Scanned - '||entered_by ").  
				append(					" WHEN (priority = 'Q' AND event_status_code = 'A' AND entered_by = 'IF') ").
				append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Add-On Tube Scanned' ").
				append(					" WHEN (priority = 'Q' AND event_status_code = 'A' AND entered_by <> 'IF') ").
				append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Add-On Tube Scanned - '||entered_by "). 				
				append(					" WHEN (event_status_code = 'D') ").
				append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Result Finalized' ").   
				append(					" WHEN (event_status_code = 'X' AND entered_by <> 'IF') ").
				append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Cancelled - '||entered_by ").      
				append(					" WHEN (event_status_code = 'X' AND entered_by = 'IF') ").
				append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Cancelled' ").      
				append(					" ELSE '*' ").
				append(					" END ) sh_msg ").
				append(			" FROM ").
				append(				" (select dlod.accession_number, dlod.order_test_code, dlod.order_test_name, dlod.priority, st.event_status_code,st.entered_by, ").
				append(						" REPLACE (dlod.order_test_name, ',', '-') tn,  to_char(st.status_date_time, 'mm/dd/yyyy HH24:MI AM')  dt,  DECODE(dl.lab_id,'SW','PT','SE','ET', 'HW','PT','HE','ET',  dl.lab_id) AS TZ ").  
				append(				" FROM ih_dw.dim_lab_order_details dlod ").
				append(					" LEFT OUTER JOIN IH_DW.DIM_LAB_ORDER dlo ON dlod.LAB_ORDER_FK = dlo.LAB_ORDER_PK ").
				append(					" JOIN ih_dw.dim_lab dl ON dlo.lab_fk = dl.lab_pk ").
				append(					" JOIN ih_dw.STATUS_HISTORY st  ON dlod.accession_number =  st.accession_number AND dlod.order_test_code = st.order_test_code ").
				append(				" WHERE st.event_status_code IN('A','D','X','S') and  dlod.REQUISITION_ID = UPPER(?) " ).
				append(				" ) " ). 
				append(			" group by accession_number, order_test_code ").
				append(		" ) st ").
				append(		" ON dlod.accession_number =  st.accession_number AND dlod.order_test_code = st.order_test_code ").
				append(" WHERE dlod.requisition_id = UPPER(?)   AND NVL(result_test_code, 'X') != 'COLLB' AND dot.micro_culture_format_flag='N' ").
				append(		" AND NVL(dlod.test_category,'X') NOT IN ('PENDING') AND NVL(dot.order_test_type,'X')NOT IN ( '+','-','C') " ).
				append(" ORDER BY case when NVL(r.report_grouping, dot.inquiry_grouping)='Problem Log' then 0.1 "). //Make sure Problem Log always appears top. The minimum sort order defined is 10, so 0.1 should place ahead of any other tests.
				append(				 " when lrg.sort_order is null then (select lrg1.sort_order ").
				append(												   " from ih_dw.lov_report_grouping lrg1 ").
				append(												   " where lrg1.report_grouping=NVL(r.report_grouping, dot.inquiry_grouping) ").
				append(												   " 	and lrg1.source_lab_system=dot.source_lab_system) ").
				append(				 " Else lrg.sort_order End,  report_grouping, ").
				append(			" case when NVL(r.report_grouping, dot.inquiry_grouping)='Other Tests' Then NVL(r.order_test_name,dlod.order_test_name ) ").
				append(				 " Else r.sequence_number || 'A' End, NVL(r.result_test_name,dlod.order_test_name )").toString();

		
//		+ "	        WHEN (event_status_code = 'A') "
//		+ "	         THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Tube Scanned' "
		
		
		
			/**
			 * Result mapper to map the result set to search results object.
			 */
			private static final ResultMapper<Results> REPORT_TEST_DET_MAPPER = new ResultMapper<Results>() {
				public Results map(final ResultSet resultSet) throws SQLException {
					final Results results = new Results(null,
							resultSet.getString("accession_number"),
							null,
							resultSet.getString("result_test_name"),		// <==================  result_test_name : COLLB= Collected By: //
							resultSet.getString("textual_result_full"),		// <==================  textual_result : COLLB= Johnson //
							resultSet.getString("unit_of_measure"),
							resultSet.getString("reference_range"),
							resultSet.getString("result_comment"),
							resultSet.getString("abnormal_flag"),
							resultSet.getString("clinical_status"), 
							resultSet.getString("order_control_reason"),
							resultSet.getString("order_test_name"),   resultSet.getString("sh_msg"),      
							resultSet.getInt("parent_test_count")
					);
					results.setReportGrpName(resultSet.getString("report_grouping"));
					results.setShmsg(resultSet.getString("sh_msg"));
					return results;
				}
			};
			
			public List<Results> getReportTestDetails(final String requisitionNo, final String reportGroupVal) {
				List<Results> lstResults = new ArrayList<Results>();
				ParamMapper paramMapper = new ParamMapper() {
					public void mapParam(final PreparedStatement preparedStatement)
							throws SQLException {
						preparedStatement.setString(1, requisitionNo);
						preparedStatement.setString(2, requisitionNo);
					}
				};
				try {
					LOGGER.log(Level.FINE, "GET_REPORT_TEST_DET: " +  GET_REPORT_TEST_DET);
					System.out.println("GET_REPORT_TEST_DET "+GET_REPORT_TEST_DET);
					lstResults = DataAccessHelper.executeQuery(GET_REPORT_TEST_DET,
							paramMapper, REPORT_TEST_DET_MAPPER);
				} catch (SQLException sqlException) {
					sqlException.printStackTrace();
				}
				
				LOGGER.log(Level.FINE, "lstResults size: " +  lstResults.size());
				return lstResults;
			}
			private static final String GET_STATUS_TEST_DET = new StringBuffer().
					append("SELECT dlod.accession_number, NVL(r.order_test_name,dlod.order_test_name )  AS order_test_name, ").
					append(		 " NVL(r.result_test_name,dlod.order_test_name ) AS result_test_name, ").
					append(		 " COUNT(*) OVER (PARTITION BY NVL(r.order_test_name,dlod.order_test_name) ) parent_test_count, ").
					append(		 " (CASE WHEN textual_result_full IS NULL AND dlod.order_detail_status IN ('I', 'O', 'P') THEN 'PENDING' ").
					append(		 	   " when r.textual_result_full is null and dlod.order_detail_status in ('X') then 'CANCELLED' ").
					append(			   " ELSE textual_result_full END) textual_result_full, "). 
					append(		 " r.unit_of_measure, r.reference_range, r.derived_abnormal_flag AS abnormal_flag, r.result_comment, ").
					append(		 " NVL(lcs.clinical_status_def, dlod.clinical_status) clinical_status, " ).
					append(		 " dlod.order_control_reason, NVL(r.report_grouping, dot.inquiry_grouping) report_grouping, NVL(sh_msg,'-' ) sh_msg ").				
					append("FROM ih_dw.dim_lab_order_details dlod ").
					append(		" JOIN ih_dw.dim_order_test dot ON dot.order_test_pk = dlod.order_test_fk ").
					append(		" LEFT OUTER JOIN (SELECT r2.*, drt.report_grouping, sequence_number ").
					append(						 " FROM ih_dw.results r2 , ih_dw.dim_result_test drt ").
					append(						 " WHERE r2.result_test_fk = drt.result_test_pk) r ").
					append(						 " ON dlod.lab_order_details_pk = r.lab_order_details_fk "). 
					append(		" LEFT OUTER JOIN ih_dw.lov_clinical_status lcs ON lcs.clinical_status = dlod.clinical_status ").
					append(		" LEFT OUTER JOIN ih_dw.lov_report_grouping lrg ON r.report_grouping = lrg.report_grouping AND r.source_lab_system = lrg.source_lab_system ").
					append(		" LEFT OUTER JOIN " ).
					append(		" ( " ).
					append(			" select accession_number, order_test_code, wm_concat(distinct " ).
					append(					" CASE " ).
					append(					" WHEN (priority = 'RTN' AND event_status_code = 'S' AND entered_by = 'IF') ").
					append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Order Received' ").
					append(					" WHEN (priority = 'RTN' AND event_status_code = 'S' AND entered_by <> 'IF') ").
					append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Order Received - '||entered_by ").  
					append(					" WHEN (priority = 'Q' AND event_status_code = 'S' AND entered_by = 'IF') ").
					append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Add-On Order Received' ").
					append(					" WHEN (priority = 'Q' AND event_status_code = 'S' AND entered_by <> 'IF') ").
					append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Add-On Order Received - '||entered_by "). 
					append(					" WHEN (priority = 'RTN' AND event_status_code = 'A' AND entered_by = 'IF') ").
					append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Tube Scanned' ").
					append(					" WHEN (priority = 'RTN' AND event_status_code = 'A' AND entered_by <> 'IF') ").
					append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Tube Scanned - '||entered_by ").  
					append(					" WHEN (priority = 'Q' AND event_status_code = 'A' AND entered_by = 'IF') ").
					append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Interface Add-On Tube Scanned' ").
					append(					" WHEN (priority = 'Q' AND event_status_code = 'A' AND entered_by <> 'IF') ").
					append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Manual Add-On Tube Scanned - '||entered_by "). 				
					append(					" WHEN (event_status_code = 'D') ").
					append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Result Finalized' ").   
					append(					" WHEN (event_status_code = 'X' AND entered_by <> 'IF') ").
					append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Cancelled - '||entered_by ").      
					append(					" WHEN (event_status_code = 'X' AND entered_by = 'IF') ").
					append(					" THEN tn||'|'|| dt||' '|| TZ||'|'|| 'Cancelled' ").      
					append(					" ELSE '*' ").
					append(					" END ) sh_msg ").
					append(			" FROM ").
					append(				" (select dlod.accession_number, dlod.order_test_code, dlod.order_test_name, dlod.priority, st.event_status_code,st.entered_by, ").
					append(						" REPLACE (dlod.order_test_name, ',', '-') tn,  to_char(st.status_date_time, 'mm/dd/yyyy HH24:MI AM')  dt,  DECODE(dl.lab_id,'SW','PT','SE','ET', 'HW','PT','HE','ET',  dl.lab_id) AS TZ ").  
					append(				" FROM ih_dw.dim_lab_order_details dlod ").
					append(					" LEFT OUTER JOIN IH_DW.DIM_LAB_ORDER dlo ON dlod.LAB_ORDER_FK = dlo.LAB_ORDER_PK ").
					append(					" JOIN ih_dw.dim_lab dl ON dlo.lab_fk = dl.lab_pk ").
					append(					" JOIN ih_dw.STATUS_HISTORY st  ON dlod.accession_number =  st.accession_number AND dlod.order_test_code = st.order_test_code ").
					append(				" WHERE st.event_status_code IN('A','D','X','S') and  dlod.REQUISITION_ID = UPPER(?) " ).
					append(				" ) " ). 
					append(			" group by accession_number, order_test_code ").
					append(		" ) st ").
					append(		" ON dlod.accession_number =  st.accession_number AND dlod.order_test_code = st.order_test_code ").
					append(" WHERE dlod.requisition_id = UPPER(?)   AND NVL(result_test_code, 'X') != 'COLLB' AND dot.micro_culture_format_flag='N' ").
					append(		" AND NVL(dlod.test_category,'X') NOT IN ('PENDING') AND NVL(dot.order_test_type,'X')NOT IN ( '+','-','C') " ).
					append( "ORDER BY").
   					append(" case when clinical_status = 'Cancelled' then 1 when clinical_status = 'Scheduled' then 2 when clinical_status = 'Received' then 3 when clinical_status = 'Valued' then 4").
   					append(" when clinical_status = 'Done' then 5 else 6 end,").
   					append("case when NVL(r.report_grouping, dot.inquiry_grouping)='Problem Log' then 0.1 "). //Make sure Problem Log always appears top. The minimum sort order defined is 10, so 0.1 should place ahead of any other tests.
   					append(				 " when lrg.sort_order is null then (select lrg1.sort_order ").
   					append(												   " from ih_dw.lov_report_grouping lrg1 ").
   					append(												   " where lrg1.report_grouping=NVL(r.report_grouping, dot.inquiry_grouping) ").
   					append(												   " 	and lrg1.source_lab_system=dot.source_lab_system) ").
   					append(				 " Else lrg.sort_order End,  report_grouping, ").
   					append(			" case when NVL(r.report_grouping, dot.inquiry_grouping)='Other Tests' Then NVL(r.order_test_name,dlod.order_test_name ) ").
   					append(				 " Else r.sequence_number || 'A' End, NVL(r.result_test_name,dlod.order_test_name )").toString();
			
				/**
				 * Result mapper to map the result set to search results object.
				 */
				private static final ResultMapper<Results> GET_STATUS_TEST_DET_MAPPER = new ResultMapper<Results>() {
					public Results map(final ResultSet resultSet) throws SQLException {
						final Results results = new Results(null,
								resultSet.getString("accession_number"),
								null,
								resultSet.getString("result_test_name"),		// <==================  result_test_name : COLLB= Collected By: //
								resultSet.getString("textual_result_full"),		// <==================  textual_result : COLLB= Johnson //
								resultSet.getString("unit_of_measure"),
								resultSet.getString("reference_range"),
								resultSet.getString("result_comment"),
								resultSet.getString("abnormal_flag"),
								resultSet.getString("clinical_status"), 
								resultSet.getString("order_control_reason"),
								resultSet.getString("order_test_name"),   resultSet.getString("sh_msg"),      
								resultSet.getInt("parent_test_count")
						);
						results.setReportGrpName(resultSet.getString("report_grouping"));
						results.setShmsg(resultSet.getString("sh_msg"));
						return results;
					}
				};
			
			public List<Results> getStatusTestDetails(final String requisitionNo, final String reportGroupVal) {
				List<Results> lstResults = new ArrayList<Results>();
				ParamMapper paramMapper = new ParamMapper() {
					public void mapParam(final PreparedStatement preparedStatement)
							throws SQLException {
						preparedStatement.setString(1, requisitionNo);
						preparedStatement.setString(2, requisitionNo);
					}
				};
				try {
					LOGGER.log(Level.FINE, "GET_STATUS_TEST_DET: " +  GET_STATUS_TEST_DET);
  				System.out.println("GET_STATUS_TEST_DET "+GET_STATUS_TEST_DET);
					lstResults = DataAccessHelper.executeQuery(GET_STATUS_TEST_DET,
							paramMapper, GET_STATUS_TEST_DET_MAPPER);
				} catch (SQLException sqlException) {
					sqlException.printStackTrace();
				}
				
				LOGGER.log(Level.FINE, "lstResults size: " +  lstResults.size());
				return lstResults;
			}
			
		}
