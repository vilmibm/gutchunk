package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

const (
	dsn    = "/mnt/volume_tor1_01/gutenberg/chunker.db?cache=shared&mode=rwc"
	target = "/mnt/volume_tor1_01/gutenberg/aleph.gutenberg.org"
)

func connectDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func createSchema(db *sql.DB) error {
	s := `
		CREATE TABLE IF NOT EXISTS files (
			id       INTEGER PRIMARY KEY,
			name     TEXT,
			author   TEXT,
			filename TEXT,
			content  TEXT
		);

		CREATE TABLE IF NOT EXISTS chunks (
			id       INTEGER PRIMARY KEY,
			chunk    TEXT,
			sourceid INTEGER,

			FOREIGN KEY (sourceid) REFERENCES files(sourceid)
		)`

	_, err := db.Exec(s)

	return err
}

func _main() error {
	db, err := connectDB()
	if err != nil {
		return fmt.Errorf("could not connect to %s: %w", dsn, err)
	}

	if err = createSchema(db); err != nil {
		return fmt.Errorf("failed to create db schema: %w", err)
	}

	//return readFiles(db)

	return makeChunks(db)

	//return nil
}

type bookfile struct {
	ID       int
	Name     string
	Author   string
	Content  string
	Filename string
}

func extractChunks(db *sql.DB, id int) error {
	var err error
	var tx *sql.Tx
	var stmt *sql.Stmt

	tx, err = db.Begin()
	end := func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}
	defer end()
	if err != nil {
		return err
	}

	stmt, err = tx.Prepare("SELECT name, author, filename, content FROM files WHERE id = ?")
	if err != nil {
		return err
	}

	row := stmt.QueryRow(id)
	var b bookfile
	b.ID = id
	err = row.Scan(&b.Name, &b.Author, &b.Filename, &b.Content)
	if err != nil {
		return err
	}
	stmt.Close()

	s := bufio.NewScanner(strings.NewReader(b.Content))
	inHeader := true
	inFooter := false
	chunk := ""
	for s.Scan() {
		text := strings.TrimSpace(s.Text())
		if inFooter {
			break
		}
		if strings.HasPrefix(text, "*** START") {
			inHeader = false
			continue
		}
		if inHeader {
			continue
		}
		if strings.HasPrefix(text, "*** END") {
			inFooter = true
		}
		if text == "" {
			// end of "paragraph"
			if len(chunk) < 300 {
				chunk = ""
				continue
			}
		} else {
			chunk += text + "\n"
			continue
		}
		stmt, err = tx.Prepare("INSERT INTO chunks (sourceid, chunk) VALUES (?, ?)")
		if err != nil {
			return fmt.Errorf("could not prepare: %w", err)
		}

		_, err = stmt.Exec(b.ID, chunk)
		if err != nil {
			return fmt.Errorf("could not insert: %w", err)
		}
		stmt.Close()
		chunk = ""
	}
	return nil
}

func makeChunks(db *sql.DB) error {
	ids := []int{}

	rows, err := db.Query("SELECT id FROM files")
	if err != nil {
		return err
	}

	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			return err
		}
		ids = append(ids, id)
	}

	rows.Close()

	max := len(ids)

	for x, id := range ids {
		fmt.Printf("%d of %d\r", x, max)
		err = extractChunks(db, id)
		if err != nil {
			return err
		}
	}

	return nil
}

func readFiles(db *sql.DB) error {
	return filepath.WalkDir(target, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() ||
			!strings.HasSuffix(d.Name(), "zip") ||
			strings.HasSuffix(d.Name(), "-8.zip") ||
			strings.HasSuffix(d.Name(), "-0.zip") {
			return err
		}

		r, err := zip.OpenReader(path)
		if err != nil {
			return err
		}
		defer r.Close()

		for x, f := range r.File {
			if !strings.HasSuffix(f.Name, ".txt") {
				fmt.Println("skipping ", f.Name)
				continue
			}
			fmt.Println("doin ", f.Name)
			if x > 0 {
				break
			}
			c, err := f.Open()
			if err != nil {
				return err
			}
			bs := bytes.NewBuffer([]byte{})
			if _, err = io.Copy(bs, c); err != nil {
				return err
			}
			name, author := extractNameAuthor(*bs)
			if name == "" {
				name = f.Name
			}

			stmt, err := db.Prepare("INSERT INTO files (name, author, content, filename) VALUES (?, ?, ?, ?)")
			if err != nil {
				return err
			}
			defer stmt.Close()
			_, err = stmt.Exec(name, author, bs.String(), f.Name)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func extractNameAuthor(content bytes.Buffer) (string, string) {
	s := bufio.NewScanner(&content)
	c := 0

	var title string
	var author string

	for s.Scan() {
		if author != "" && title != "" {
			break
		}

		text := strings.TrimSpace(s.Text())

		if strings.HasPrefix(text, "***") {
			break
		}

		if strings.HasPrefix(text, "Title") {
			sp := strings.SplitN(text, ":", 2)
			if len(sp) == 2 {
				title = strings.TrimSpace(sp[1])
			}
		}

		if strings.HasPrefix(text, "Author") {
			sp := strings.SplitN(text, ":", 2)
			if len(sp) == 2 {
				author = strings.TrimSpace(sp[1])
			}
		}

		c++
	}

	return title, author
}

func main() {
	if err := _main(); err != nil {
		fmt.Fprintf(os.Stderr, "error: "+err.Error())
	}
}
