set search_path to pex_test,public;

\echo #############################################################################################;
\echo Check, if there are meta records without history record ...;
select * from youtube_meta m
left outer join youtube_history h
  on m.gid = h.gid
where h.gid is null limit 10;

\echo #############################################################################################;
\echo Check, if there are history records without meta record ...;
select *
from youtube_history h
left outer join youtube_meta m
  on m.gid = h.gid
where m.gid is null limit 10;

\echo #############################################################################################;
\echo Check, if number of views can be reduced in time ...;
select * from (
  select
    gid,
    last_value(views) over (partition by gid order by updated_at nulls auto ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_views,
    max(views) over (partition by gid) as max_views
  from youtube_history_denorm
) x
where x.max_views != last_views
limit 10;

\echo Yes, it can be reduced;
select * from youtube.youtube_history where gid = 'YT:--4IaSV821A' order by updated_at;

----------------------------------------------------------------------------------------------------------------------------------------
\echo #############################################################################################;
\echo Check, if there can be more records for certain GID in the same day ...;
select gid, trunc(updated_at), count(*) count_gid
from youtube_history_denorm
group by gid, trunc(updated_at)
having count(*) > 1
order by count_gid desc
limit 10;

\echo Yes, there is up to 36 records per day;
select * from youtube_history_denorm where gid = 'YT:1hN3hdcUd0c' and trunc(updated_at) = date '2018-06-06'
order by updated_at;

----------------------------------------------------------------------------------------------------------------------------------------
\echo #############################################################################################;
\echo Check, how many days of data we have ...;
select distinct trunc(created_at) from youtube_history_denorm order by 1;
-- 2018-06-01
select distinct trunc(updated_at) from youtube_history_denorm order by 1;
-- 2018-06-01 - 2018-06-07

-- all created in 2018-06-01 day, updated in the following 6 days

----------------------------------------------------------------------------------------------------------------------------------------
\echo #############################################################################################;
\echo Check, how often a video can be updated and if any fact is changed everytime

select count(*), gid from youtube_history_denorm group by gid order by 1 desc limit 10;

select * from pex_test.youtube_history_denorm where gid = 'YT:XcEXoUr11kM' order by updated_at;

----------------------------------------------------------------------------------------------------------------------------------------
\echo #############################################################################################;
\echo Check, how many records do not contain change of any fact

select count(*)
from youtube_history_denorm;

select count(*)
from (
    select gid, category_id, user_id, created_at, duration, day_updated_at, updated_at,
      views - nvl(lag(views) over (w1), 0) as views_diff,
      likes - nvl(lag(likes) over (w1), 0) as likes_diff,
      dislikes - nvl(lag(dislikes) over (w1), 0) as dislikes_diff,
      comments - nvl(lag(comments) over (w1), 0) as comments_diff
    from youtube_history_denorm
      window w1 as (partition by category_id, gid order by day_updated_at nulls auto, updated_at nulls auto)
) x
where x.views_diff > 0 or likes_diff > 0 or dislikes_diff > 0 or comments_diff > 0
;

-- we can reduce 2,123,751 out of 4,037,178 (almost 1/2)
