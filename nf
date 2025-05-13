
CREATE OR REPLACE PROCEDURE `${nqes_fwa_module_gudv_tgt_dataset_name}.nqes_fwa_sp`(from_date DATE, to_date DATE)
OPTIONS(strict_mode=False)
BEGIN
  --Insert entry into audit table
    MERGE ${target_project_id}.${audit_tgt_dataset_name}.${audit_target_tblname} tgt
    USING(
        SELECT "nqes_fwa_daily_norm_sp" prc_name, Cast(from_date AS datetime) AS start_time, Cast(SPLIT(from_date," ")[OFFSET(0)] AS DATE) src_prc_dt
        ) src
    ON (src.prc_name=tgt.PROCESS_NAME AND src.START_TIME=tgt.START_TIME)
    WHEN NOT MATCHED THEN
    INSERT(
        PROCESS_MODULE,
        SCHEDULER,
        PROCESS_NAME,
        SOURCE_NAME,
        TARGET_NAME,
        START_TIME,
        END_TIME,
        PROCESS_DT,
        NUM_RECORDS_AFFECTED,
        STATUS,
        RETURN_MESSAGE,
        schedule_time,
        Job_name
        )
    VALUES
  (
    "network_research_assistant",
    "airflow",
    prc_name,
	  "${nqes_fwa_module_fjpv_src_id}.${nqes_fwa_module_fjpv_scores_src_dataset_name}.${nqes_fwa_module_fjpv_scores_scores_src_tblname},${nqes_fwa_module_fjpv_src_id}.${nqes_fwa_module_fjpv_scores_src_dataset_name}.${nqes_fwa_module_fjpv_scores_long_src_tblname},${nqes_fwa_module_fjpv_src_id}.${nqes_fwa_module_fjpv_curated_src_dataset_name}.${nqes_fwa_module_fjpv_curated_src_tblname},${nqes_fwa_module_gudv_src_id}.${nqes_fwa_module_gudv_fwa_src_dataset_name}.${nqes_fwa_module_gudv_fwa_src_tblname},${nqes_fwa_module_gudv_src_id}.${nqes_fwa_module_gudv_analytics_src_dataset_name}.${nqes_fwa_module_gudv_analytics_src_tblname}"
    "${target_project_id}.${nqes_fwa_module_gudv_tgt_dataset_name}.${nqes_fwa_module_gudv_tgt_tblname}",
     start_time,
     NULL,
     src_prc_dt,
     NULL,
     "Started",
     NULL,
     safe_cast(from_date as timestamp),
     "gudv_nw_research_assistant_nqes_site_daily_norm"
      ); 

MERGE into `${target_project_id}.${nqes_fwa_module_gudv_tgt_dataset_name}.${nqes_fwa_module_gudv_tgt_tblname}` tgt
using (
WITH scores AS (
  SELECT *
  FROM (
    SELECT rpt_dt, acct_num,imsi, cust_id, cust_line_seq_id, score_name,
    NTILE(10) OVER(PARTITION BY score_name ORDER BY avg(score_value) DESC) as decile_num,
    AVG(score_value) as score_value
    FROM `${nqes_fwa_module_fjpv_src_id}.${nqes_fwa_module_fjpv_scores_src_dataset_name}.${nqes_fwa_module_fjpv_scores_scores_src_tblname}`
    WHERE rpt_dt BETWEEN from_date AND to_date
    -- AND score_name = 'fwa_score'
    GROUP BY 1,2,3,4,5,6
  ) a
    PIVOT(AVG(a.score_value) as score, AVG(a.decile_num) as decile FOR a.score_name in ('fwa_score', 'ih_device_subscore', 'ih_expectation_v_reality_subscore', 'ih_group_score', 'ih_wifi_subscore', 'ntwk_accessibility_subscore', 'ntwk_capacity_subscore', 'ntwk_ebh_subscore', 'ntwk_group_score', 'ntwk_reliability_5g_subscore', 'ntwk_reliability_subscore', 'ntwk_signal_strength_quality_5g_subscore', 'ntwk_signal_strength_quality_subscore', 'ntwk_speed_subscore'))
),  driver as (
  --6217872
 SELECT rpt_dt, acct_num, cust_id, cust_line_seq_id,
  MAX(subscore_1) subscore_1, MAX(subscore_2) subscore_2, MAX(subscore_3) subscore_3, MAX(feature_1) feature_1, MAX(feature_2) feature_2, MAX(feature_3) feature_3
 
  FROM (
  SELECT rpt_dt, acct_num,cust_id, cust_line_seq_id, feature, AVG(value) as shap_value,
    CASE
    WHEN feature = 'shap_aseu_utilization' THEN 'Capacity'
    WHEN feature = 'shap_avg_bsrp_device' THEN 'Signal'
    WHEN feature = 'shap_avg_memory_percent_free' THEN 'Device'
    WHEN feature = 'shap_avg_modem_temp' THEN 'Device'
    WHEN feature = 'shap_avg_path_loss_device' THEN 'Signal'
    WHEN feature = 'shap_avg_pucch_tx_power_device' THEN 'Signal'
    WHEN feature = 'shap_avg_rsrp_4g_device' THEN 'Signal'
    WHEN feature = 'shap_avg_rsrp_5g_device' THEN 'Signal_5G'
    WHEN feature = 'shap_avg_rsrq_4g_device' THEN 'Signal'
    WHEN feature = 'shap_avg_rsrq_5g_device' THEN 'Signal_5G'
    WHEN feature = 'shap_bh_dl_tp' THEN 'Speed'
    WHEN feature = 'shap_bh_ul_tp' THEN 'Speed'
    WHEN feature = 'shap_device_reboot_count' THEN 'Device'
    WHEN feature = 'shap_device_total_downtime_day_in_secs' THEN 'Device'
    WHEN feature = 'shap_handover_attempt_count_lte_device' THEN 'Reliability'
    WHEN feature = 'shap_ipv6_switches_device' THEN 'Device'
    WHEN feature = 'shap_max_util_pct' THEN 'EBH'
    WHEN feature = 'shap_max_wifi_device_cnt' THEN 'WiFi'
    WHEN feature = 'shap_mean_cqi_device' THEN 'Signal'
    WHEN feature = 'shap_mean_sinr_5g_device' THEN 'Signal_5G'
    WHEN feature = 'shap_mean_sinr_device' THEN 'Signal'
    WHEN feature = 'shap_non_uw_percentage' THEN 'Accessibility'
    WHEN feature = 'shap_nr_scg_change_count_device' THEN 'Reliability_5G'
    WHEN feature = 'shap_nr_scg_change_failure_count_device' THEN 'Reliability_5G'
    WHEN feature = 'shap_open_nrb_tickets_cnt_30days' THEN 'Expectation_vs_Reality'
    WHEN feature = 'shap_pbit0_fd_avg' THEN 'EBH'
    WHEN feature = 'shap_pbit0_fdv_avg' THEN 'EBH'
    WHEN feature = 'shap_pbit0_fl_avg' THEN 'EBH'
    WHEN feature = 'shap_prbu_avg' THEN 'Capacity'
    WHEN feature = 'shap_rach_attempt_count_lte_device' THEN 'Accessibility'
    WHEN feature = 'shap_rrc_connect_request_count_device' THEN 'Accessibility'
    END AS subscore,
    row_number() OVER (PARTITION BY acct_num, cust_id, cust_line_seq_id ORDER BY AVG(value) DESC) as driver,
    FROM `${nqes_fwa_module_fjpv_src_id}.${nqes_fwa_module_fjpv_scores_src_dataset_name}.${nqes_fwa_module_fjpv_scores_long_src_tblname}`
    WHERE rpt_dt BETWEEN from_date AND to_date
    AND feature LIKE 'shap_%'
    and feature NOT IN ('shap_max_wifi_device_cnt',
    -- 'shap_device_total_downtime_day_in_secs',
    'shap_avg_memory_percent_free',
    'shap_avg_modem_temp',
    'shap_open_nrb_tickets_cnt_30days',
    'shap_ipv6_switches_device'
    -- 'shap_device_reboot_count'
    )
    GROUP BY 1,2,3,4,5
    QUALIFY driver <= 3
  )
  PIVOT(max(subscore) subscore, max(feature) feature for driver IN (1,2,3))
  GROUP BY 1,2,3,4
), primary_enb as (
  SELECT DISTINCT rpt_dt, cust_id, cust_line_seq_id, connected_enb, connected_4g_sector, ran_vendor,
  FROM (
    SELECT trans_dt as rpt_dt, cust_id, cust_line_seq_id, connected_enb, connected_4g_sector, ran_vendor,
    COUNT(*) as enb_connections
    FROM `${nqes_fwa_module_gudv_src_id}.${nqes_fwa_module_gudv_fwa_src_dataset_name}.${nqes_fwa_module_gudv_fwa_src_tblname}`
    WHERE connected_enb IS NOT NULL
    AND trans_dt BETWEEN from_date AND to_date
    GROUP BY 1,2,3,4,5,6
  )
  QUALIFY ROW_NUMBER() OVER(PARTITION BY cust_id, cust_line_seq_id ORDER BY enb_connections DESC, connected_enb  DESC) = 1
), primary_gnb as (
  SELECT DISTINCT rpt_dt, cust_id, cust_line_seq_id, connected_gnb, connected_5g_sector, ran_vendor,
  FROM (
    SELECT trans_dt as rpt_dt, cust_id, cust_line_seq_id, connected_gnb, connected_5g_sector, ran_vendor,
    COUNT(*) as gnb_connections
    FROM `${nqes_fwa_module_gudv_src_id}.${nqes_fwa_module_gudv_fwa_src_dataset_name}.${nqes_fwa_module_gudv_fwa_src_tblname}`
    WHERE connected_gnb IS NOT NULL
    AND trans_dt BETWEEN from_date AND to_date
    GROUP BY 1,2,3,4,5,6
  )
  QUALIFY ROW_NUMBER() OVER(PARTITION BY cust_id, cust_line_seq_id ORDER BY gnb_connections DESC, connected_gnb DESC) = 1
), kpis AS (
    SELECT trans_dt AS rpt_dt, cust_id, cust_line_seq_id,
    MAX(cause_code) AS cause_code,
    MAX(address_classification) AS address_classification,
    MAX(CASE WHEN location_violation_flag = 'Y' THEN 1 else 0 END) AS loc_violation,
    AVG(SAFE_CAST(latitude as numeric)) sqdb_latitude,
    AVG(SAFE_CAST(longitude as numeric)) sqdb_longitude,
    MAX(device_type) as device_type,
    MAX(price_plan) AS price_plan,
    MAX(predicted_enb) AS predicted_enb,
    MAX(predicted_4g_sector) AS predicted_4g_sector,
    MAX(predicted_gnb) AS predicted_gnb,
    MAX(predicted_5g_sector) AS predicted_5g_sector,
    AVG(bh_dl_tp) AS bh_dl_tp,
    AVG(bh_ul_tp) AS bh_ul_tp,
    AVG(weighted_dl_tp) AS weighted_dl_tp,
    AVG(weighted_ul_tp) AS weighted_ul_tp,
    approx_quantiles( CAST(bh_dl_tp as NUMERIC) , 100)[offset(50)] as UG_p50_dl_speed,
    approx_quantiles( CAST(bh_ul_tp as NUMERIC) , 100)[offset(50)] as UG_p50_ul_speed,
    approx_quantiles( CAST(ookla_downloadresult as NUMERIC) , 100)[offset(50)] as OOKLA_p50_dl_speed,
    approx_quantiles( CAST(ookla_uploadresult as NUMERIC) , 100)[offset(50)] as OOKLA_p50_ul_speed,
   
  FROM  `${nqes_fwa_module_gudv_src_id}.${nqes_fwa_module_gudv_fwa_src_dataset_name}.${nqes_fwa_module_gudv_fwa_src_tblname}` as base
  WHERE trans_dt BETWEEN from_date AND to_date
  -- AND customer_segment = 'CONSUMER'
  -- AND product = 'C_BAND'
  GROUP BY 1,2,3
 
), base AS (
  SELECT a.* EXCEPT(lte_market_id), mar.*
  FROM (
    SELECT kpis.*,
    primary_enb.connected_enb as primary_enb, primary_enb.connected_4g_sector as primary_enb_sector,
    primary_gnb.connected_gnb as primary_gnb, primary_gnb.connected_5g_sector as primary_gnb_sector,
    CAST( (case
        when CAST(primary_enb.connected_enb AS numeric) between 0 and 299999 then FLOOR(CAST(primary_enb.connected_enb AS numeric)/ 1000)
        when CAST(primary_enb.connected_enb AS numeric)  between 300000 and 599999 then FLOOR(CAST(primary_enb.connected_enb AS numeric) /1000) - 300
        when CAST(primary_enb.connected_enb AS numeric)  between 600000 and 899999 then FLOOR(CAST(primary_enb.connected_enb AS numeric) /1000) - 600
        when CAST(primary_enb.connected_enb AS numeric)  > 899999 then 900
        else 999 end)
        AS INT64 ) as lte_market_id
    FROM kpis
    LEFT JOIN primary_enb
    ON kpis.cust_id = primary_enb.cust_id
    AND kpis.cust_line_seq_id = primary_enb.cust_line_seq_id
    AND kpis.rpt_dt = primary_enb.rpt_dt
    LEFT JOIN primary_gnb
    ON kpis.cust_id = primary_gnb.cust_id
    AND kpis.cust_line_seq_id = primary_gnb.cust_line_seq_id
    AND kpis.rpt_dt = primary_gnb.rpt_dt
  ) AS a
  left join `${nqes_fwa_module_gudv_src_id}.${nqes_fwa_module_gudv_analytics_src_dataset_name}.${nqes_fwa_module_gudv_analytics_src_tblname}` mar
  ON cast(mar.lte_market_id as numeric) = a.lte_market_id
  QUALIFY row_number() OVER (PARTITION BY cust_id, cust_line_seq_id ORDER BY a.lte_market_id DESC, a.primary_enb DESC, primary_gnb DESC)  = 1
),  dma AS (
  SELECT rpt_dt, cust_id, cust_line_seq_id,mdn,
  AVG(aseu_capacity) AS aseu_capacity,
AVG(aseu_usage) AS aseu_usage,
AVG(aseu_utilization) AS aseu_utilization,
AVG(avg_bsrp_device) AS avg_bsrp_device,
AVG(avg_memory_percent_free) AS avg_memory_percent_free,
AVG(avg_modem_temp) AS avg_modem_temp,
AVG(avg_path_loss_device) AS avg_path_loss_device,
AVG(avg_pucch_tx_power_device) AS avg_pucch_tx_power_device,
AVG(avg_rsrp_4g_device) AS avg_rsrp_4g_device,
AVG(avg_rsrp_5g_device) AS avg_rsrp_5g_device,
AVG(avg_rsrq_4g_device) AS avg_rsrq_4g_device,
AVG(avg_rsrq_5g_device) AS avg_rsrq_5g_device,
AVG(avg_rx_pdcp_bytes_device) AS avg_rx_pdcp_bytes_device,
AVG(bh_avg_rsrq_5g_device) AS bh_avg_rsrq_5g_device,
-- AVG(bh_dl_tp) AS bh_dl_tp,
AVG(ug_download_speed) as bh_dl_tp_1,
AVG(bh_ul_tp) AS bh_ul_tp_1,
AVG(connected_distance_4g) AS connected_distance_4g,
AVG(connected_distance_5g) AS connected_distance_5g,
AVG(crash_count) AS crash_count,
AVG(device_reboot_count) AS device_reboot_count,
AVG(device_uptime_since_reboot_in_secs) AS device_uptime_since_reboot_in_secs,
AVG(dl_vol_mb) AS dl_vol_mb,
AVG(max_util_pct) AS max_util_pct,
AVG(max_wifi_device_cnt) AS max_wifi_device_cnt,
AVG(mean_cqi_device) AS mean_cqi_device,
AVG(mean_sinr_5g_device) AS mean_sinr_5g_device,
AVG(mean_sinr_device) AS mean_sinr_device,
AVG(nbh_avg_rsrp_4g_device) AS nbh_avg_rsrp_4g_device,
AVG(nbh_avg_rsrp_5g_device) AS nbh_avg_rsrp_5g_device,
AVG(nbh_avg_rsrq_4g_device) AS nbh_avg_rsrq_4g_device,
AVG(nbh_avg_rsrq_5g_device) AS nbh_avg_rsrq_5g_device,
AVG(nbh_mean_sinr_5g_device) AS nbh_mean_sinr_5g_device,
AVG(non_bh_dl_tp) AS non_bh_dl_tp,
AVG(non_bh_ul_tp) AS non_bh_ul_tp,
AVG(non_uw_percentage) AS non_uw_percentage,
AVG(nr_cell_ta_distance) AS nr_cell_ta_distance,
AVG(nr_scg_change_count_device) AS nr_scg_change_count_device,
AVG(nr_scg_change_failure_count_device) AS nr_scg_change_failure_count_device,
SUM(nrb_ticket_cnt_day) AS nrb_ticket_cnt_day,
AVG(open_nrb_tickets_cnt_30days) AS open_nrb_tickets_cnt_30days,
AVG(pbit0_fd_avg) AS pbit0_fd_avg,
AVG(pbit0_fdv_avg) AS pbit0_fdv_avg,
AVG(pbit0_fl_avg) AS pbit0_fl_avg,
AVG(pbit5_fd_avg) AS pbit5_fd_avg,
AVG(pbit5_fdv_avg) AS pbit5_fdv_avg,
AVG(pbit5_fl_avg) AS pbit5_fl_avg,
AVG(prbu_avg) AS prbu_avg,
AVG(predicted_connected_gnb_sect_carr_flag) AS predicted_connected_gnb_sect_carr_flag,
AVG(premium_fwa_pp) AS premium_fwa_pp,
AVG(rach_attempt_count_lte_device) AS rach_attempt_count_lte_device,
AVG(rrc_connect_request_count_device) AS rrc_connect_request_count_device,
AVG(sub1_trigger_prev_month_4g_cnt) AS sub1_trigger_prev_month_4g_cnt,
AVG(sub3_trigger_prev_month_4g_cnt) AS sub3_trigger_prev_month_4g_cnt,
AVG(total_packets_received_device) AS total_packets_received_device,
AVG(total_packets_sent_device) AS total_packets_sent_device,
AVG(trigger_4g_flag) AS trigger_4g_flag,
AVG(trigger_5g_flag) AS trigger_5g_flag,
AVG(ul_vol_mb) AS ul_vol_mb,
AVG(weighted_dl_tp) AS weighted_dl_tp,
AVG(weighted_ul_tp) AS weighted_ul_tp,
AVG(wifi_score) AS wifi_score,
   FROM  `${nqes_fwa_module_fjpv_src_id}.${nqes_fwa_module_fjpv_curated_src_dataset_name}.${nqes_fwa_module_fjpv_curated_src_tblname}` as dma
  WHERE rpt_dt BETWEEN from_date AND to_date
  GROUP BY 1,2,3
),
final_data as(
  SELECT base.*,
  scores.* EXCEPT(rpt_dt, cust_id, cust_line_seq_id),
  driver.* EXCEPT(rpt_dt, acct_num, cust_id, cust_line_seq_id),
  dma.* EXCEPT(rpt_dt, cust_id, cust_line_seq_id,weighted_dl_tp,weighted_ul_tp)
  -- discos.activity_dt, discos.voluntary_disconnects, disco_reasons.disco_reason_type_new as disco_group_reason,
  FROM base
  LEFT JOIN scores
  ON scores.cust_id = base.cust_id AND scores.cust_line_seq_id = base.cust_line_seq_id AND scores.rpt_dt = base.rpt_dt
  LEFT JOIN driver
  ON scores.cust_id = driver.cust_id AND scores.cust_line_seq_id = driver.cust_line_seq_id AND scores.rpt_dt = driver.rpt_dt
  LEFT JOIN dma
  ON base.cust_id = dma.cust_id AND base.cust_line_seq_id = dma.cust_line_seq_id AND base.rpt_dt = dma.rpt_dt
  -- LEFT JOIN discos
  -- ON base.cust_id = discos.cust_id AND base.cust_line_seq_id = discos.cust_line_seq_id AND base.rpt_dt = DATE_SUB(discos.rpt_dt, INTERVAL 1 MONTH)
  -- LEFT JOIN disco_reasons
  -- ON discos.cust_id = disco_reasons.cust_id AND discos.cust_line_seq_id = disco_reasons.cust_line_seq_id AND discos.rpt_dt = disco_reasons.rpt_dt
  where scores.imsi IN (select distinct imsi from vz-it-pr-gudv-dtwndo-0.aid_nwgenie_core_tbls.fwa_imsi)
)
select 
rpt_dt as trans_dt,
cust_id,
cust_line_seq_id,
cause_code,
address_classification,
loc_violation,
sqdb_latitude,
sqdb_longitude,
device_type,
price_plan,
predicted_enb,
safe_cast(predicted_4g_sector as INT64) as predicted_4g_sector ,
predicted_gnb,
safe_cast(predicted_5g_sector as INT64) as predicted_5g_sector ,
bh_dl_tp,
bh_ul_tp,
weighted_dl_tp,
weighted_ul_tp,
UG_p50_dl_speed,
UG_p50_ul_speed,
safe_cast(OOKLA_p50_dl_speed as INT64) as OOKLA_p50_dl_speed,
safe_cast(OOKLA_p50_ul_speed as INT64) as OOKLA_p50_ul_speed,
primary_enb,
safe_cast(primary_enb_sector as INT64) as primary_enb_sector,
primary_gnb,
safe_cast(primary_gnb_sector as INT64) as primary_gnb_sector,
territory,
market,
submarket,
submarket_long,
lte_market_id,
lte_market_name,
acct_num,
imsi,
mdn,
score_fwa_score,
safe_cast(decile_fwa_score as INT64) as decile_fwa_score ,
score_ih_device_subscore,
safe_cast(decile_ih_device_subscore as INT64) as decile_ih_device_subscore, 
score_ih_expectation_v_reality_subscore,
safe_cast(decile_ih_expectation_v_reality_subscore as INT64) as decile_ih_expectation_v_reality_subscore ,
score_ih_group_score,
safe_cast(decile_ih_group_score as INT64 )as decile_ih_group_score,
score_ih_wifi_subscore,
safe_cast(decile_ih_wifi_subscore as INT64) as decile_ih_wifi_subscore,
score_ntwk_accessibility_subscore,
safe_cast(decile_ntwk_accessibility_subscore as INT64) as decile_ntwk_accessibility_subscore,
score_ntwk_capacity_subscore,
safe_cast(decile_ntwk_capacity_subscore as INT64) as decile_ntwk_capacity_subscore,
score_ntwk_ebh_subscore,
safe_cast(decile_ntwk_ebh_subscore as INT64) as decile_ntwk_ebh_subscore,
score_ntwk_group_score ,
safe_cast(decile_ntwk_group_score as INT64) as decile_ntwk_group_score ,
score_ntwk_reliability_5g_subscore,
safe_cast(decile_ntwk_reliability_5g_subscoreas as INT64) as decile_ntwk_reliability_5g_subscoreas,
score_ntwk_reliability_subscore,
safe_cast(decile_ntwk_reliability_subscore as INT64) as decile_ntwk_reliability_subscore,
score_ntwk_signal_strength_quality_5g_subscore,
safe_cast(decile_ntwk_signal_strength_quality_5g_subscore as INT64) AS decile_ntwk_signal_strength_quality_5g_subscore ,
score_ntwk_signal_strength_quality_subscore,
SAFE_CAST(decile_ntwk_signal_strength_quality_subscore AS INT64) as decile_ntwk_signal_strength_quality_subscore,
score_ntwk_speed_subscore,
safe_cast(decile_ntwk_speed_subscore as INT64) as decile_ntwk_speed_subscore,
subscore_1,
subscore_2,
subscore_3,
feature_1,
feature_2,
feature_3,
aseu_capacity,
aseu_usage,
aseu_utilization,
avg_bsrp_device,
avg_memory_percent_free,
avg_modem_temp,
avg_path_loss_device,
avg_pucch_tx_power_device,
avg_rsrp_4g_device,
avg_rsrp_5g_device,
avg_rsrq_4g_device,
avg_rsrq_5g_device,
avg_rx_pdcp_bytes_device,
bh_avg_rsrq_5g_device,
bh_dl_tp_1,
bh_ul_tp_1,
connected_distance_4g,
connected_distance_5g,
safe_cast(crash_count as INT64) as crash_count,
safe_cast(device_reboot_count as INT64)as device_reboot_count,
safe_cast(device_uptime_since_reboot_in_secs as INT64) as device_uptime_since_reboot_in_secs,
safe_cast(dl_vol_mb as INT64)as dl_vol_mb ,
max_util_pct,
safe_cast(max_wifi_device_cntas as INT64) as max_wifi_device_cntas,
mean_cqi_device,
mean_sinr_5g_device,
mean_sinr_device,
nbh_avg_rsrp_4g_device,
nbh_avg_rsrp_5g_device,
nbh_avg_rsrq_4g_device,
nbh_avg_rsrq_5g_device,
nbh_mean_sinr_5g_device,
non_bh_dl_tp,
non_bh_ul_tp,
non_uw_percentage,
nr_cell_ta_distance,
nr_scg_change_count_device,
nr_scg_change_failure_count_device,
safe_cast(nrb_ticket_cnt_day as INT64) as nrb_ticket_cnt_day,
safe_cast(open_nrb_tickets_cnt_30days as INT64) as open_nrb_tickets_cnt_30days,
pbit0_fd_avg,
pbit0_fdv_avg,
pbit0_fl_avg,
pbit5_fd_avg,
pbit5_fdv_avg,
pbit5_fl_avg,
prbu_avg,
safe_cast(predicted_connected_gnb_sect_carr_flag as INT64) as predicted_connected_gnb_sect_carr_flag,
safe_cast(premium_fwa_pp as INT64) as premium_fwa_pp,
safe_cast(rach_attempt_count_lte_device as INT64 ) as rach_attempt_count_lte_device,
safe_cast(rrc_connect_request_count_device as INT64) as rrc_connect_request_count_device,
safe_cast(sub1_trigger_prev_month_4g_cnt as INT64) as sub1_trigger_prev_month_4g_cnt,
safe_cast(sub3_trigger_prev_month_4g_cnt as INT64) as sub3_trigger_prev_month_4g_cnt,
safe_cast(total_packets_received_device as INT64) as total_packets_received_device,
safe_cast(total_packets_sent_device as INT64) as total_packets_sent_device,
trigger_4g_flag,
safe_cast(trigger_5g_flag as INT64) as trigger_5g_flag,
safe_cast(ul_vol_mb as INT64) as ul_vol_mb,
safe_cast(wifi_score as INT64) as wifi_score,
SAFE_CAST(from_date AS DATE) as process_dt,
current_timestamp() as created_timestamp
from final_data )src
on 
tgt.trans_dt=src.trans_dt and
tgt.cust_id =src.cust_id
and tgt.cust_line_seq_id=src.cust_line_seq_id
and tgt.acct_num=src.acct_num

WHEN MATCHED 
THEN UPDATE SET
tgt.cause_code=src.cause_code,
tgt.address_classification=src.address_classification,
tgt.loc_violation=src.loc_violation,
tgt.sqdb_latitude=src.sqdb_latitude,
tgt.sqdb_longitude=src.sqdb_longitude,
tgt.device_type=src.device_type,
tgt.price_plan=src.price_plan,
tgt.predicted_enb=src.predicted_enb,
tgt.predicted_4g_sector=src.predicted_4g_sector,
tgt.predicted_gnb=src.predicted_gnb,
tgt.predicted_5g_sector=src.predicted_5g_sector,
tgt.bh_dl_tp=src.bh_dl_tp,
tgt.bh_ul_tp=src.bh_ul_tp,
tgt.weighted_dl_tp=src.weighted_dl_tp,
tgt.weighted_ul_tp=src.weighted_ul_tp,
tgt.UG_p50_dl_speed=src.UG_p50_dl_speed,
tgt.UG_p50_ul_speed=src.UG_p50_ul_speed,
tgt.OOKLA_p50_dl_speed=src.OOKLA_p50_dl_speed,
tgt.OOKLA_p50_ul_speed=src.OOKLA_p50_ul_speed,
tgt.primary_enb=src.primary_enb,
tgt.primary_enb_sector=src.primary_enb_sector,
tgt.primary_gnb=src.primary_gnb,
tgt.primary_gnb_sector=src.primary_gnb_sector,
tgt.territory=src.territory,
tgt.market=src.market,
tgt.submarket=src.submarket,
tgt.submarket_long=src.submarket_long,
tgt.lte_market_id=src.lte_market_id,
tgt.lte_market_name=src.lte_market_name,
tgt.imsi=src.imsi,
tgt.mdn=src.mdn,
tgt.score_fwa_score=src.score_fwa_score,
tgt.decile_fwa_score=src.decile_fwa_score,
tgt.score_ih_device_subscore=src.score_ih_device_subscore,
tgt.decile_ih_device_subscore=src.decile_ih_device_subscore,
tgt.score_ih_expectation_v_reality_subscore=src.score_ih_expectation_v_reality_subscore,
tgt.decile_ih_expectation_v_reality_subscore=src.decile_ih_expectation_v_reality_subscore,
tgt.score_ih_group_score=src.score_ih_group_score,
tgt.decile_ih_group_score=src.decile_ih_group_score,
tgt.score_ih_wifi_subscore=src.score_ih_wifi_subscore,
tgt.decile_ih_wifi_subscore=src.decile_ih_wifi_subscore,
tgt.score_ntwk_accessibility_subscore=src.score_ntwk_accessibility_subscore,
tgt.decile_ntwk_accessibility_subscore=src.decile_ntwk_accessibility_subscore,
tgt.score_ntwk_capacity_subscore=src.score_ntwk_capacity_subscore,
tgt.decile_ntwk_capacity_subscore=src.decile_ntwk_capacity_subscore,
tgt.score_ntwk_ebh_subscore=src.score_ntwk_ebh_subscore,
tgt.decile_ntwk_ebh_subscore=src.decile_ntwk_ebh_subscore,
tgt.score_ntwk_group_score=src.score_ntwk_group_score,
tgt.decile_ntwk_group_score=src.decile_ntwk_group_score,
tgt.score_ntwk_reliability_5g_subscore=src.score_ntwk_reliability_5g_subscore,
tgt.decile_ntwk_reliability_5g_subscore=src.decile_ntwk_reliability_5g_subscore,
tgt.score_ntwk_reliability_subscore=src.score_ntwk_reliability_subscore,
tgt.decile_ntwk_reliability_subscore=src.decile_ntwk_reliability_subscore,
tgt.score_ntwk_signal_strength_quality_5g_subscore=src.score_ntwk_signal_strength_quality_5g_subscore,
tgt.decile_ntwk_signal_strength_quality_5g_subscore=src.decile_ntwk_signal_strength_quality_5g_subscore,
tgt.score_ntwk_signal_strength_quality_subscore=src.score_ntwk_signal_strength_quality_subscore,
tgt.decile_ntwk_signal_strength_quality_subscore=src.decile_ntwk_signal_strength_quality_subscore,
tgt.score_ntwk_speed_subscore=src.score_ntwk_speed_subscore,
tgt.decile_ntwk_speed_subscore=src.decile_ntwk_speed_subscore,
tgt.subscore_1=src.subscore_1,
tgt.subscore_2=src.subscore_2,
tgt.subscore_3=src.subscore_3,
tgt.feature_1=src.feature_1,
tgt.feature_2=src.feature_2,
tgt.feature_3=src.feature_3,
tgt.aseu_capacity=src.aseu_capacity,
tgt.aseu_usage=src.aseu_usage,
tgt.aseu_utilization=src.aseu_utilization,
tgt.avg_bsrp_device=src.avg_bsrp_device,
tgt.avg_memory_percent_free=src.avg_memory_percent_free,
tgt.avg_modem_temp=src.avg_modem_temp,
tgt.avg_path_loss_device=src.avg_path_loss_device,
tgt.avg_pucch_tx_power_device=src.avg_pucch_tx_power_device,
tgt.avg_rsrp_4g_device=src.avg_rsrp_4g_device,
tgt.avg_rsrp_5g_device=src.avg_rsrp_5g_device,
tgt.avg_rsrq_4g_device=src.avg_rsrq_4g_device,
tgt.avg_rsrq_5g_device=src.avg_rsrq_5g_device,
tgt.avg_rx_pdcp_bytes_device=src.avg_rx_pdcp_bytes_device,
tgt.bh_avg_rsrq_5g_device=src.bh_avg_rsrq_5g_device,
tgt.bh_dl_tp_1=src.bh_dl_tp_1,
tgt.bh_ul_tp_1=src.bh_ul_tp_1,
tgt.connected_distance_4g=src.connected_distance_4g,
tgt.connected_distance_5g=src.connected_distance_5g,
tgt.crash_count=src.crash_count,
tgt.device_reboot_count=src.device_reboot_count,
tgt.device_uptime_since_reboot_in_secs=src.device_uptime_since_reboot_in_secs,
tgt.dl_vol_mb=src.dl_vol_mb,
tgt.max_util_pct=src.max_util_pct,
tgt.max_wifi_device_cnt=src.max_wifi_device_cnt,
tgt.mean_cqi_device=src.mean_cqi_device,
tgt.mean_sinr_5g_device=src.mean_sinr_5g_device,
tgt.mean_sinr_device=src.mean_sinr_device,
tgt.nbh_avg_rsrp_4g_device=src.nbh_avg_rsrp_4g_device,
tgt.nbh_avg_rsrp_5g_device=src.nbh_avg_rsrp_5g_device,
tgt.nbh_avg_rsrq_4g_device=src.nbh_avg_rsrq_4g_device,
tgt.nbh_avg_rsrq_5g_device=src.nbh_avg_rsrq_5g_device,
tgt.nbh_mean_sinr_5g_device=src.nbh_mean_sinr_5g_device,
tgt.non_bh_dl_tp=src.non_bh_dl_tp,
tgt.non_bh_ul_tp=src.non_bh_ul_tp,
tgt.non_uw_percentage=src.non_uw_percentage,
tgt.nr_cell_ta_distance=src.nr_cell_ta_distance,
tgt.nr_scg_change_count_device=src.nr_scg_change_count_device,
tgt.nr_scg_change_failure_count_device=src.nr_scg_change_failure_count_device,
tgt.nrb_ticket_cnt_day=src.nrb_ticket_cnt_day,
tgt.open_nrb_tickets_cnt_30days=src.open_nrb_tickets_cnt_30days,
tgt.pbit0_fd_avg=src.pbit0_fd_avg,
tgt.pbit0_fdv_avg=src.pbit0_fdv_avg,
tgt.pbit0_fl_avg=src.pbit0_fl_avg,
tgt.pbit5_fd_avg=src.pbit5_fd_avg,
tgt.pbit5_fdv_avg=src.pbit5_fdv_avg,
tgt.pbit5_fl_avg=src.pbit5_fl_avg,
tgt.prbu_avg=src.prbu_avg,
tgt.predicted_connected_gnb_sect_carr_flag=src.predicted_connected_gnb_sect_carr_flag,
tgt.premium_fwa_pp=src.premium_fwa_pp,
tgt.rach_attempt_count_lte_device=src.rach_attempt_count_lte_device,
tgt.rrc_connect_request_count_device=src.rrc_connect_request_count_device,
tgt.sub1_trigger_prev_month_4g_cnt=src.sub1_trigger_prev_month_4g_cnt,
tgt.sub3_trigger_prev_month_4g_cnt=src.sub3_trigger_prev_month_4g_cnt,
tgt.total_packets_received_device=src.total_packets_received_device,
tgt.total_packets_sent_device=src.total_packets_sent_device,
tgt.trigger_4g_flag=src.trigger_4g_flag,
tgt.trigger_5g_flag=src.trigger_5g_flag,
tgt.ul_vol_mb=src.ul_vol_mb,
tgt.wifi_score=src.wifi_score,
tgt.process_dt=src.process_dt,
tgt.created_timestamp=src.created_timestamp
WHEN NOT MATCHED THEN 
INSERT (
trans_dt,
cust_id,
cust_line_seq_id,
cause_code,
address_classification,
loc_violation,
sqdb_latitude,
sqdb_longitude,
device_type,
price_plan,
predicted_enb,
predicted_4g_sector,
predicted_gnb,
predicted_5g_sector,
bh_dl_tp,
bh_ul_tp,
weighted_dl_tp,
weighted_ul_tp,
UG_p50_dl_speed,
UG_p50_ul_speed,
OOKLA_p50_dl_speed,
OOKLA_p50_ul_speed,
primary_enb,
primary_enb_sector,
primary_gnb,
primary_gnb_sector,
territory,
market,
submarket,
submarket_long,
lte_market_id,
lte_market_name,
acct_num,
imsi,
mdn,
score_fwa_score,
decile_fwa_score,
score_ih_device_subscore,
decile_ih_device_subscore,
score_ih_expectation_v_reality_subscore,
decile_ih_expectation_v_reality_subscore,
score_ih_group_score,
decile_ih_group_score,
score_ih_wifi_subscore,
decile_ih_wifi_subscore,
score_ntwk_accessibility_subscore,
decile_ntwk_accessibility_subscore,
score_ntwk_capacity_subscore,
decile_ntwk_capacity_subscore,
score_ntwk_ebh_subscore,
decile_ntwk_ebh_subscore,
score_ntwk_group_score,
decile_ntwk_group_score,
score_ntwk_reliability_5g_subscore,
decile_ntwk_reliability_5g_subscore,
score_ntwk_reliability_subscore,
decile_ntwk_reliability_subscore,
score_ntwk_signal_strength_quality_5g_subscore,
decile_ntwk_signal_strength_quality_5g_subscore,
score_ntwk_signal_strength_quality_subscore,
decile_ntwk_signal_strength_quality_subscore,
score_ntwk_speed_subscore,
decile_ntwk_speed_subscore,
subscore_1,
subscore_2,
subscore_3,
feature_1,
feature_2,
feature_3,
aseu_capacity,
aseu_usage,
aseu_utilization,
avg_bsrp_device,
avg_memory_percent_free,
avg_modem_temp,
avg_path_loss_device,
avg_pucch_tx_power_device,
avg_rsrp_4g_device,
avg_rsrp_5g_device,
avg_rsrq_4g_device,
avg_rsrq_5g_device,
avg_rx_pdcp_bytes_device,
bh_avg_rsrq_5g_device,
bh_dl_tp_1,
bh_ul_tp_1,
connected_distance_4g,
connected_distance_5g,
crash_count,
device_reboot_count,
device_uptime_since_reboot_in_secs,
dl_vol_mb,
max_util_pct,
max_wifi_device_cnt,
mean_cqi_device,
mean_sinr_5g_device,
mean_sinr_device,
nbh_avg_rsrp_4g_device,
nbh_avg_rsrp_5g_device,
nbh_avg_rsrq_4g_device,
nbh_avg_rsrq_5g_device,
nbh_mean_sinr_5g_device,
non_bh_dl_tp,
non_bh_ul_tp,
non_uw_percentage,
nr_cell_ta_distance,
nr_scg_change_count_device,
nr_scg_change_failure_count_device,
nrb_ticket_cnt_day,
open_nrb_tickets_cnt_30days,
pbit0_fd_avg,
pbit0_fdv_avg,
pbit0_fl_avg,
pbit5_fd_avg,
pbit5_fdv_avg,
pbit5_fl_avg,
prbu_avg,
predicted_connected_gnb_sect_carr_flag,
premium_fwa_pp,
rach_attempt_count_lte_device,
rrc_connect_request_count_device,
sub1_trigger_prev_month_4g_cnt,
sub3_trigger_prev_month_4g_cnt,
total_packets_received_device,
total_packets_sent_device,
trigger_4g_flag,
trigger_5g_flag,
ul_vol_mb,
wifi_score,
process_dt,
created_timestamp) values
(
src.trans_dt,
src.cust_id,
src.cust_line_seq_id,
src.cause_code,
src.address_classification,
src.loc_violation,
src.sqdb_latitude,
src.sqdb_longitude,
src.device_type,
src.price_plan,
src.predicted_enb,
src.predicted_4g_sector,
src.predicted_gnb,
src.predicted_5g_sector,
src.bh_dl_tp,
src.bh_ul_tp,
src.weighted_dl_tp,
src.weighted_ul_tp,
src.UG_p50_dl_speed,
src.UG_p50_ul_speed,
src.OOKLA_p50_dl_speed,
src.OOKLA_p50_ul_speed,
src.primary_enb,
src.primary_enb_sector,
src.primary_gnb,
src.primary_gnb_sector,
src.territory,
src.market,
src.submarket,
src.submarket_long,
src.lte_market_id,
src.lte_market_name,
src.acct_num,
src.imsi,
src.mdn,
src.score_fwa_score,
src.decile_fwa_score,
src.score_ih_device_subscore,
src.decile_ih_device_subscore,
src.score_ih_expectation_v_reality_subscore,
src.decile_ih_expectation_v_reality_subscore,
src.score_ih_group_score,
src.decile_ih_group_score,
src.score_ih_wifi_subscore,
src.decile_ih_wifi_subscore,
src.score_ntwk_accessibility_subscore,
src.decile_ntwk_accessibility_subscore,
src.score_ntwk_capacity_subscore,
src.decile_ntwk_capacity_subscore,
src.score_ntwk_ebh_subscore,
src.decile_ntwk_ebh_subscore,
src.score_ntwk_group_score,
src.decile_ntwk_group_score,
src.score_ntwk_reliability_5g_subscore,
src.decile_ntwk_reliability_5g_subscore,
src.score_ntwk_reliability_subscore,
src.decile_ntwk_reliability_subscore,
src.score_ntwk_signal_strength_quality_5g_subscore,
src.decile_ntwk_signal_strength_quality_5g_subscore,
src.score_ntwk_signal_strength_quality_subscore,
src.decile_ntwk_signal_strength_quality_subscore,
src.score_ntwk_speed_subscore,
src.decile_ntwk_speed_subscore,
src.subscore_1,
src.subscore_2,
src.subscore_3,
src.feature_1,
src.feature_2,
src.feature_3,
src.aseu_capacity,
src.aseu_usage,
src.aseu_utilization,
src.avg_bsrp_device,
src.avg_memory_percent_free,
src.avg_modem_temp,
src.avg_path_loss_device,
src.avg_pucch_tx_power_device,
src.avg_rsrp_4g_device,
src.avg_rsrp_5g_device,
src.avg_rsrq_4g_device,
src.avg_rsrq_5g_device,
src.avg_rx_pdcp_bytes_device,
src.bh_avg_rsrq_5g_device,
src.bh_dl_tp_1,
src.bh_ul_tp_1,
src.connected_distance_4g,
src.connected_distance_5g,
src.crash_count,
src.device_reboot_count,
src.device_uptime_since_reboot_in_secs,
src.dl_vol_mb,
src.max_util_pct,
src.max_wifi_device_cnt,
src.mean_cqi_device,
src.mean_sinr_5g_device,
src.mean_sinr_device,
src.nbh_avg_rsrp_4g_device,
src.nbh_avg_rsrp_5g_device,
src.nbh_avg_rsrq_4g_device,
src.nbh_avg_rsrq_5g_device,
src.nbh_mean_sinr_5g_device,
src.non_bh_dl_tp,
src.non_bh_ul_tp,
src.non_uw_percentage,
src.nr_cell_ta_distance,
src.nr_scg_change_count_device,
src.nr_scg_change_failure_count_device,
src.nrb_ticket_cnt_day,
src.open_nrb_tickets_cnt_30days,
src.pbit0_fd_avg,
src.pbit0_fdv_avg,
src.pbit0_fl_avg,
src.pbit5_fd_avg,
src.pbit5_fdv_avg,
src.pbit5_fl_avg,
src.prbu_avg,
src.predicted_connected_gnb_sect_carr_flag,
src.premium_fwa_pp,
src.rach_attempt_count_lte_device,
src.rrc_connect_request_count_device,
src.sub1_trigger_prev_month_4g_cnt,
src.sub3_trigger_prev_month_4g_cnt,
src.total_packets_received_device,
src.total_packets_sent_device,
src.trigger_4g_flag,
src.trigger_5g_flag,
src.ul_vol_mb,
src.wifi_score,
src.process_dt,
cast(current_timestamp as timestamp));


-- Insert record into Audit table
UPDATE ${target_project_id}.${audit_tgt_dataset_name}.${audit_target_tblname}
SET
END_TIME=current_datetime(),
NUM_RECORDS_AFFECTED=@@Row_Count,
STATUS="Completed",
RETURN_MESSAGE="Success"
WHERE
PROCESS_NAME="nqes_fwa_sp" AND start_time=Cast(from_date AS datetime);

-- Insert record into Audit table
SELECT "Process Completed Successfully";

EXCEPTION WHEN ERROR THEN
--update audit table with error status
UPDATE ${target_project_id}.${audit_tgt_dataset_name}.${audit_target_tblname}
SET
END_TIME=current_datetime(),
STATUS="Error",
RETURN_MESSAGE=Concat(@@error.message,"------***----",@@ERROR.statement_text)
WHERE
PROCESS_NAME="nqes_fwa_sp" AND start_time=Cast(from_date AS datetime);
RAISE USING message=@@error.message;

END
;
