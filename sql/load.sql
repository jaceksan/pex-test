
copy /*+ label(copy_youtube_meta) */ youtube_meta from local 'data/analyst_challenge_data_meta.csv'
delimiter ',' null as '' rejectmax 100
rejected data 'youtube_meta_rej.txt' exceptions 'youtube_meta_exc.txt'
direct
;

copy /*+ label(copy_youtube_history) */ youtube_history (
  gid,
  views_filler FILLER VARCHAR(100),
  views AS CASE WHEN views_filler = '' THEN NULL::INT ELSE views_filler::INT end,
  likes_filler FILLER VARCHAR(100),
  likes AS CASE WHEN likes_filler = '' THEN NULL::INT ELSE likes_filler::INT end,
  dislikes_filler FILLER VARCHAR(100),
  dislikes AS CASE WHEN dislikes_filler = '' THEN NULL::INT ELSE dislikes_filler::INT end,
  comments_filler FILLER VARCHAR(100),
  comments AS CASE WHEN comments_filler = '' THEN NULL::INT ELSE comments_filler::INT end,
  updated_at
)
from local 'data/analyst_challenge_data_history.csv'
delimiter ',' null as '' rejectmax 100 enclosed '"'
rejected data 'youtube_history_rej.txt' exceptions 'youtube_history_exc.txt'
direct
;
