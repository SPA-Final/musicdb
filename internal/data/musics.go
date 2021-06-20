package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/SPA-Final/musicdb/internal/validator"
	"github.com/lib/pq"
	"time"
)

type Music struct {
	Id         int64          `gorm:"primaryKey"`
	Title      string         `json:"title"`
	Duration   int16          `json:"duration"`
	Popularity float32        `json:"popularity"`
	Genres     pq.StringArray `json:"genres"`
	CreatedAt  time.Time      `json:"created_at"`
	Version    int32          `json:"version"`
}

func (m *Music) SanitizeGenres(genres []sql.NullString) {
	for _, g := range genres {
		if !g.Valid {
			continue
		}
		m.Genres = append(m.Genres, g.String)
	}
}

func ValidateMovie(v *validator.Validator, movie *Music) {
	v.Check(movie.Title != "", "title", "must be provided")
	v.Check(len(movie.Title) <= 500, "title", "must not be more than 500 bytes long")
	v.Check(movie.Duration != 0, "duration", "must be provided")
	v.Check(movie.Duration > 0, "duration", "must be a positive integer")
	v.Check(movie.Popularity != 0, "popularity", "must be provided")
	v.Check(movie.Popularity > 0, "popularity", "must be a positive number")
	v.Check(movie.Genres != nil, "genres", "must be provided")
	v.Check(len(movie.Genres) >= 1, "genres", "must contain at least 1 genre")
	v.Check(len(movie.Genres) <= 5, "genres", "must not contain more than 5 genres")
	v.Check(validator.Unique(movie.Genres), "genres", "must not contain duplicate values")
}

type MusicsModel struct {
	DB *sql.DB
}

func (m MusicsModel) Insert(mv *Music) error {
	q := `INSERT INTO musics (title, duration, genres, popularity)
		  VALUES ($1, $2, $3, $4)
		  RETURNING id, created_at, version`

	args := []interface{}{mv.Title, mv.Duration, pq.Array(mv.Genres), mv.Popularity}
	return m.DB.QueryRow(q, args...).Scan(&mv.Id, &mv.CreatedAt, &mv.Version)
}

func (m MusicsModel) Get(id int64) (*Music, error) {
	if id < 1 {
		return nil, ErrRecordNotFound
	}

	q := `SELECT *
		  FROM musics
		  WHERE id = $1`

	var ms Music
	var genres []sql.NullString
	err := m.DB.QueryRow(q, id).Scan(
		&ms.Id,
		&ms.Title,
		&ms.Duration,
		pq.Array(&genres),
		&ms.Popularity,
		&ms.CreatedAt,
		&ms.Version,
	)
	ms.SanitizeGenres(genres)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, ErrRecordNotFound
		default:
			return nil, err
		}
	}

	return &ms, nil
}

func (m MusicsModel) GetAll(title string, genres []string, filters Filters) ([]*Music, Metadata, error) {
	q := fmt.Sprintf(`SELECT count(*) OVER(), *
		  FROM musics
		  WHERE (to_tsvector('simple', title) @@ plainto_tsquery('simple', $1) OR $1 = '')
		  AND (genres @> $2 OR $2 = '{}')
		  ORDER BY %s %s, id ASC
	      LIMIT $3 OFFSET $4`, filters.sortColumn(), filters.sortDirection())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	args := []interface{}{title, pq.Array(genres), filters.limit(), filters.offset()}
	rows, err := m.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, Metadata{}, err
	}
	defer rows.Close()

	totalRecords := 0
	musics := []*Music{}
	for rows.Next() {
		var music Music
		var gnrs []sql.NullString
		err := rows.Scan(
			&totalRecords,
			&music.Id,
			&music.Title,
			&music.Duration,
			pq.Array(&gnrs),
			&music.Popularity,
			&music.CreatedAt,
			&music.Version,
		)
		if err != nil {
			return nil, Metadata{}, err
		}

		music.SanitizeGenres(gnrs)
		musics = append(musics, &music)
	}

	if err = rows.Err(); err != nil {
		return nil, Metadata{}, err
	}

	metadata := calculateMetadata(totalRecords, filters.Page, filters.PageSize)

	return musics, metadata, nil
}

func (m MusicsModel) Update(ms *Music) error {
	q := `UPDATE musics
		  SET title = $2, duration = $3, popularity = $4, genres = $5, version = version + 1
		  WHERE id = $1 AND version = $6
		  RETURNING version`

	args := []interface{}{
		ms.Id, ms.Title, ms.Duration, ms.Popularity, pq.Array(ms.Genres), ms.Version,
	}

	err := m.DB.QueryRow(q, args...).Scan(&ms.Version)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrEditConflict
		default:
			return err
		}
	}
	return nil
}

func (m MusicsModel) Delete(id int64) error {
	if id < 1 {
		return ErrRecordNotFound
	}

	q := `DELETE FROM musics
		  WHERE id = $1`
	result, err := m.DB.Exec(q, id)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrRecordNotFound
	}

	return nil
}
