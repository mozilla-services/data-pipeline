WITH sample AS (
   SELECT client_id,
          date_parse(submission_date_s3, '%Y%m%d') as normalized_submission_date,
          popup_notification_stats['web-notifications'].action_1 AS push_acceptance,
          web_notification_shown
   FROM main_summary
   WHERE submission_date_s3 > date_format(CURRENT_DATE - interval '6' MONTH, '%Y%m%d')
     AND submission_date_s3 < date_format(CURRENT_DATE, '%Y%m%d')
     AND sample_id='42'
),
push_accepters AS (
   SELECT DISTINCT client_id,
                   normalized_submission_date AS eligibility_start,
                   CASE WHEN ((normalized_submission_date + interval '30' DAY) > CURRENT_DATE) THEN CURRENT_DATE ELSE (normalized_submission_date + interval '30' DAY) END AS eligibility_end
   FROM sample
   WHERE push_acceptance > 0
),
combined_push_users AS (
   SELECT DISTINCT p.client_id,
                   eligibility_start,
                   eligibility_end
   FROM sample s
   INNER JOIN push_accepters p ON s.client_id=p.client_id
   WHERE normalized_submission_date BETWEEN eligibility_start AND eligibility_end
   AND web_notification_shown > 0
),
all_dau AS (
   SELECT DISTINCT client_id,
                   normalized_submission_date
   FROM sample
),
push_dau AS (
  SELECT count(DISTINCT p.client_id) AS dau,
         merge(hll_create(p.client_id, 15)) AS hll,
         normalized_submission_date
  FROM all_dau d
  INNER JOIN combined_push_users p ON d.client_id = p.client_id
  WHERE normalized_submission_date BETWEEN eligibility_start AND eligibility_end
  GROUP BY 3
),
push_mau AS (
  SELECT cardinality(merge(hll) over (ORDER BY normalized_submission_date ROWS BETWEEN 27 PRECEDING AND 0 FOLLOWING)) as mau,
         normalized_submission_date
   FROM push_dau
),
smoothed_dau AS (
   SELECT normalized_submission_date,
          avg(dau) OVER (ORDER BY normalized_submission_date ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING) AS smoothed_dau
   FROM push_dau
)
SELECT mau * 100 AS mau,
       dau * 100 AS dau,
       smoothed_dau * 100 AS smoothed_dau,
       m.normalized_submission_date,
       smoothed_dau/mau AS ER
FROM push_mau m
JOIN push_dau d ON m.normalized_submission_date=d.normalized_submission_date
JOIN smoothed_dau s ON m.normalized_submission_date=s.normalized_submission_date
WHERE m.normalized_submission_date > (CURRENT_DATE - interval '6' MONTH + interval '30' DAY)
ORDER BY m.normalized_submission_date
