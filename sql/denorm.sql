insert /*+ direct,label(youtube_history_denorm_p1) */ into tmp_youtube_history_denorm
select h.*,
  m.user_id, m.category_id, m.created_at, m.duration,
  trunc(h.updated_at) as day_updated_at
from youtube_meta m
join youtube_history h
  on m.gid = h.gid
;

insert /*+ direct,label(youtube_history_denorm_p2) */ into youtube_history_denorm
select
  gid, views, likes, dislikes, comments, updated_at,
  user_id, category_id, created_at, duration, day_updated_at
from (
  select
    gid, views, likes, dislikes, comments, updated_at,
    user_id, category_id, created_at, duration, day_updated_at,
    views - nvl(lag(views) over (w1), 0) as views_diff,
    likes - nvl(lag(likes) over (w1), 0) as likes_diff,
    dislikes - nvl(lag(dislikes) over (w1), 0) as dislikes_diff,
    comments - nvl(lag(comments) over (w1), 0) as comments_diff
  from tmp_youtube_history_denorm t
    window w1 as (partition by category_id, gid order by day_updated_at nulls auto, updated_at nulls auto)
) x
where views_diff > 0 or likes_diff > 0 or dislikes_diff > 0 or comments_diff > 0
;
