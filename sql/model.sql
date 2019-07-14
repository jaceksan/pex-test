create /*+ label(create_table_youtube_meta) */ table youtube_meta (
	gid varchar(14) not null, -- unique identifier of the YouTube video
	user_id varchar(24), -- unique identifier of the user who uploaded the video
	category_id int, -- numeric id denoting the YouTube category the video was uploaded to. 10 = music, 20 = gaming
	created_at timestamptz, -- timestamp when video was uploaded to YouTube
	duration int, -- length of the video in seconds
	constraint pk_youtube_meta primary key (gid)
)
;

create /*+ label(create_table_youtube_history) */ table youtube_history (
	gid varchar(14) not null, -- unique identifier of the YouTube video
	views int, -- total views the video had accrued at the time of this data update
	likes int, -- total likes the video had accrued at the time of this data update
	dislikes int, -- total dislikes the video had accrued at the time of this data update
	comments int, -- total comments the video had accrued at the time of this data update
	updated_at timestamptz not null, -- timestamp of the data update for this video
	constraint pk_youtube_meta primary key (gid, updated_at)
)
segmented by hash(gid) all nodes
;

create /*+ label(create_table_youtube_history_denorm) */ table youtube_history_denorm (
    gid varchar(14) not null, -- unique identifier of the YouTube video
    views int, -- total views the video had accrued at the time of this data update
    likes int, -- total likes the video had accrued at the time of this data update
    dislikes int, -- total dislikes the video had accrued at the time of this data update
    comments int, -- total comments the video had accrued at the time of this data update
    updated_at timestamptz not null, -- timestamp of the data update for this video
    user_id varchar(24), -- unique identifier of the user who uploaded the video
    category_id int not null encoding RLE, -- numeric id denoting the YouTube category the video was uploaded to. 10 = music, 20 = gaming
    created_at timestamptz, -- timestamp when video was uploaded to YouTube
    duration int, -- length of the video in seconds
    day_updated_at date not null,
    constraint pk_youtube_history_denorm primary key (category_id, gid, day_updated_at, updated_at)
)
order by category_id, gid, day_updated_at, updated_at
segmented by hash(gid) all nodes
;

create /*+ label(create_table_youtube_history_denorm) */ table tmp_youtube_history_denorm like youtube_history_denorm including projections;

create /*+ label(create_table_youtube_history_denorm_latest) */ table youtube_history_denorm_latest (
    gid varchar(14) not null, -- unique identifier of the YouTube video
    views int, -- total views the video had accrued at the time of this data update
    likes int, -- total likes the video had accrued at the time of this data update
    dislikes int, -- total dislikes the video had accrued at the time of this data update
    comments int, -- total comments the video had accrued at the time of this data update
    updated_at timestamptz not null, -- timestamp of the data update for this video
    user_id varchar(24), -- unique identifier of the user who uploaded the video
    category_id int encoding RLE, -- numeric id denoting the YouTube category the video was uploaded to. 10 = music, 20 = gaming
    created_at timestamptz, -- timestamp when video was uploaded to YouTube
    duration int, -- length of the video in seconds
    day_updated_at date not null,
    constraint pk_youtube_history_denorm_latest primary key (category_id, gid, updated_at)
)
order by category_id, gid, updated_at
segmented by hash(gid) all nodes
;

create /*+ label(create_table_youtube_history_denorm_daily) */ table youtube_history_denorm_daily (
    gid varchar(14) not null, -- unique identifier of the YouTube video
    views int, -- total views the video had accrued at the time of this data update
    likes int, -- total likes the video had accrued at the time of this data update
    dislikes int, -- total dislikes the video had accrued at the time of this data update
    comments int, -- total comments the video had accrued at the time of this data update
    user_id varchar(24), -- unique identifier of the user who uploaded the video
    category_id int not null encoding RLE, -- numeric id denoting the YouTube category the video was uploaded to. 10 = music, 20 = gaming
    created_at timestamptz, -- timestamp when video was uploaded to YouTube
    duration int, -- length of the video in seconds
    day_updated_at date not null,
    constraint pk_youtube_history_denorm_daily primary key (category_id, gid, day_updated_at)
)
order by category_id, gid, day_updated_at
segmented by hash(gid) all nodes
;
