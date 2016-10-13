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
                   normalized_submission_date + interval '30' DAY AS eligibility_end
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
                   normalized_submission_date AS activity_date
   FROM sample
),
push_dau AS (
  -- Presto can't do RANGE window functions nor can it do count distinct in a window, so the array_agg here and in push_mau is a workaround hack.
  -- I feel dirty.
  SELECT count(DISTINCT p.client_id) AS dau,
         array_agg(p.client_id) AS client_ids,
         activity_date
  FROM all_dau d
  INNER JOIN combined_push_users p ON d.client_id = p.client_id
  WHERE activity_date BETWEEN eligibility_start AND eligibility_end
  GROUP BY 3
),
push_mau AS (
  -- I'm sorry.
  SELECT cardinality(array_distinct(flatten(array_agg(client_ids) over (ORDER BY activity_date ROWS BETWEEN 27 PRECEDING AND 0 FOLLOWING)))) AS mau,
         activity_date
   FROM push_dau
),
smoothed_dau AS (
   SELECT activity_date,
          avg(dau) OVER (ORDER BY activity_date ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING) AS smoothed_dau
   FROM push_dau
)
SELECT mau * 100 AS mau,
       dau * 100 AS dau,
       smoothed_dau * 100 AS smoothed_dau,
       m.activity_date,
       smoothed_dau/mau AS ER
FROM push_mau m
JOIN push_dau d ON m.activity_date=d.activity_date
JOIN smoothed_dau s ON m.activity_date=s.activity_date
WHERE m.activity_date > (CURRENT_DATE - interval '6' MONTH + interval '30' DAY)
ORDER BY m.activity_date
