WITH dados as (SELECT
  *
FROM
(
SELECT 'asdf' as user_pseudo_id, TIMESTAMP('2019-09-09 00:00:00') as event_date, 'tela1' as screen_name, 'category1' as eventCategory, 'action1' as eventAction, 'label1' as eventLabel
UNION ALL
SELECT 'asdf' as user_pseudo_id, TIMESTAMP('2019-09-09 00:00:01') as event_date, 'tela1' as screen_name, 'category1' as eventCategory, 'action1' as eventAction, 'label1' as eventLabel
UNION ALL
SELECT 'asdf' as user_pseudo_id, TIMESTAMP('2019-09-09 00:00:01.0001') as event_date, 'tela1' as screen_name, 'category1' as eventCategory, 'action1' as eventAction, 'label2' as eventLabel
UNION ALL
SELECT 'foo' as user_pseudo_id, TIMESTAMP('2019-09-09 00:29:00') as event_date, 'tela1' as screen_name, 'category1' as eventCategory, 'action1' as eventAction, 'label1' as eventLabel
UNION ALL
SELECT 'foo' as user_pseudo_id, TIMESTAMP('2019-09-09 00:35:00') as event_date, 'tela1' as screen_name, 'category1' as eventCategory, 'action1' as eventAction, 'label1' as eventLabel
UNION ALL
SELECT 'foo' as user_pseudo_id, TIMESTAMP('2019-09-09 01:40:00') as event_date, 'tela1' as screen_name, 'category1' as eventCategory, 'action1' as eventAction, 'label1' as eventLabel
UNION ALL
SELECT 'foo' as user_pseudo_id, TIMESTAMP('2019-09-09 01:45:00') as event_date, 'tela1' as screen_name, 'category1' as eventCategory, 'action1' as eventAction, 'label1' as eventLabel
UNION ALL
SELECT 'fdaas' as user_pseudo_id, TIMESTAMP('2019-09-09 00:00:00') as event_date, 'tela1' as screen_name, 'category1' as eventCategory, 'action1' as eventAction, 'label1' as eventLabel
)
),
tb_intervalo as (
  SELECT
    *,
    (TIMESTAMP_DIFF(event_date, LAG(event_date) OVER (PARTITION BY user_pseudo_id ORDER BY event_date), MINUTE)) as intervalo
  FROM
    dados
),
tb_sessions as (
  SELECT
    *
    EXCEPT (intervalo),
    CASE
      WHEN intervalo IS NULL OR intervalo > 30 THEN
        CONCAT(user_pseudo_id, FORMAT_TIMESTAMP('%Y%m%d%H%M%S', event_date), CAST(ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_date) as STRING))
    END as session_id,
    event_date as session_start,
    LEAD(event_date) OVER(PARTITION BY user_pseudo_id ORDER BY event_date) AS  next_session
  FROM
    tb_intervalo
  WHERE
    intervalo IS NULL OR intervalo > 30
),
resultado as (
  SELECT
    *
    EXCEPT (session_id, session_start)
    
  FROM
    tb_sessions as tb
)

SELECT
  dados.user_pseudo_id,
  dados.event_date,
  dados.screen_name,
  dados.eventCategory,
  dados.eventAction,
  dados.eventLabel,
  tb_sessions.session_id,
  tb_sessions.session_start,
  tb_sessions.next_session
FROM
  dados
LEFT JOIN
  tb_sessions
ON dados.user_pseudo_id = tb_sessions.user_pseudo_id AND dados.event_date >= tb_sessions.session_start AND (dados.event_date < tb_sessions.next_session OR tb_sessions.next_session IS NULL)