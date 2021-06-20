CREATE TABLE IF NOT EXISTS musics
(
    id          bigserial PRIMARY KEY,
    title       text                        NOT NULL,
    duration    integer                     NOT NULL,
    genres      text[]                      NOT NULL,
    popularity  numeric                     NOT NULL,
    created_at  timestamp(0) with time zone NOT NULL DEFAULT NOW(),
    version     integer                     NOT NULL DEFAULT 1
);