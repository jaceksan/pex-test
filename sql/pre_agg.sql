insert /*+ direct,label(youtube_history_denorm_latest) */ into youtube_history_denorm_latest
select gid, last_views, last_likes, last_dislikes, last_comments, last_updated_at,
  user_id, category_id, created_at, duration, day_updated_at
from (
  select
    gid, category_id, user_id, created_at, duration, day_updated_at,
    last_value(views) over (w1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_views,
    last_value(likes) over (w1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_likes,
    last_value(dislikes) over (w1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_dislikes,
    last_value(comments) over (w1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_comments,
    last_value(updated_at) over (w1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_updated_at,
    row_number() over (w1) as rownum
  from youtube_history_denorm
    window w1 as (partition by category_id, gid order by day_updated_at nulls auto, updated_at nulls auto)
) h where rownum = 1
;

insert /*+ direct,label(youtube_history_denorm_daily) */ into youtube_history_denorm_daily
select gid, views_day, likes_day, dislikes_day, comments_day,
  user_id, category_id, created_at, duration, day_updated_at
from (
  select
    gid, category_id, user_id, created_at, duration, day_updated_at,
    sum(views_diff) over (w2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as views_day,
    sum(likes_diff) over (w2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as likes_day,
    sum(dislikes_diff) over (w2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as dislikes_day,
    sum(comments_diff) over (w2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as comments_day,
    row_number() over (w2) as rownum
  from (
    select gid, category_id, user_id, created_at, duration, day_updated_at, updated_at,
      views - nvl(lag(views) over (w1), 0) as views_diff,
      likes - nvl(lag(likes) over (w1), 0) as likes_diff,
      dislikes - nvl(lag(dislikes) over (w1), 0) as dislikes_diff,
      comments - nvl(lag(comments) over (w1), 0) as comments_diff
    from youtube_history_denorm
      window w1 as (partition by category_id, gid order by day_updated_at nulls auto, updated_at nulls auto)
  ) hh
    window w2 as (partition by category_id, gid, day_updated_at order by updated_at nulls auto)
) h
where h.rownum = 1
;

