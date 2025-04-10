DECLARE mdn_input STRING;
SET mdn_input = '7033866838';


 WITH cust_map AS (
  SELECT DISTINCT cust_id, cust_line_seq_id, acct_num, nm_first, nm_last, cal.mtn, cal.vzw_imsi, cal.insert_dt, LEFT(cal.EQP_DEVICE_ID,15) AS IMEI,
  FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_v.cust_acct_line` AS cal
  WHERE mtn_status_ind = 'A'
  QUALIFY row_number() OVER (PARTITION BY mtn ORDER BY insert_dt DESC) = 1
 )


 SELECT *
 FROM cust_map
 WHERE mtn = mdn_input
