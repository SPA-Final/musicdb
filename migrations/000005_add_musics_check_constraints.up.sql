ALTER TABLE musics ADD CONSTRAINT musics_duration_check CHECK (duration >= 0);
ALTER TABLE musics ADD CONSTRAINT genres_length_check CHECK (array_length(genres, 1) BETWEEN 1 AND 8);