
select /*+ label(report_task_1) */
  h.category_id,
  sum(h.views) as total_views,
  sum(h.likes) as total_likes,
  sum(h.dislikes) as total_dislikes,
  sum(h.comments) as total_comments
from youtube_history_denorm_latest h
group by h.category_id
order by total_views desc
;

select /*+ label(report_task_2) */
  category_id, day_updated_at,
  avg(views) as avg_views,
  max(views) as max_views,
  avg(likes) as avg_likes,
  max(likes) as max_likes,
  avg(dislikes) as avg_dislikes,
  max(dislikes) as max_dislikes,
  avg(comments) as avg_comments,
  max(comments) as max_comments,
  avg(duration) as avg_duration,
  max(duration) as max_duration
from youtube_history_denorm_daily
group by category_id, day_updated_at
order by category_id, day_updated_at
;

select /*+ label(report_task_3) */
  count(*) count_videos,
  total_views_group,
  category_id
from (
    select
      greatest(10^ceil(log10(nvl(views, 0))), 1000) as total_views_group,
      category_id
    from youtube_history_denorm_latest
) x
group by total_views_group, category_id
order by total_views_group, category_id
;

select /*+ label(report_task_31) */
  count(*) count_videos,
  total_likes_group,
  category_id
from (
    select
      greatest(10^ceil(log10(nvl(likes, 0))), 1000) as total_likes_group,
      category_id
    from youtube_history_denorm_latest
) x
group by total_likes_group, category_id
order by total_likes_group, category_id
;
