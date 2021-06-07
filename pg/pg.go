package pg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/lambda-direct/gocast-stock-calc/data"
	_ "github.com/lib/pq"
	"io/fs"
	"io/ioutil"
	"log"
	"math"
	"os"
	"time"
)

type Client struct {
	db *sqlx.DB
}

func New(ctx context.Context) (*Client, error) {
	db, err := sqlx.ConnectContext(ctx, "postgres", "postgres://postgres:gocast@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		return nil, err
	}

	return &Client{db}, nil
}

func (c *Client) Close() error {
	return c.db.Close()
}

func (c *Client) SyncData(ctx context.Context, file fs.File) (data.Set, error) {
	hasher := sha256.New()

	now := time.Now()

	s, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("unable to read file: %w", err)
	}

	log.Printf("file read in %dms", time.Since(now).Milliseconds())

	now = time.Now()

	hasher.Write(s)
	newhash := hex.EncodeToString(hasher.Sum(nil))

	log.Printf("hashsum calculated in %dms", time.Since(now).Milliseconds())

	oldhash, err := os.ReadFile("data.json.sha256")
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if newhash == string(oldhash) {
		log.Println("Retrieving data from pg")

		now = time.Now()

		var ds data.Set
		if err := c.db.SelectContext(ctx, &ds, "select timestamp, rate from data order by timestamp desc"); err != nil {
			return nil, err
		}

		log.Printf("data read from pg in %dms", time.Since(now).Milliseconds())

		return ds, nil
	}

	log.Println("Dataset changed")

	now = time.Now()

	var ds data.Set
	if err := json.Unmarshal(s, &ds); err != nil {
		return nil, fmt.Errorf("unable to unmarshall dataset from redis: %w", err)
	}

	log.Printf("file unmarshalled in %dms", time.Since(now).Milliseconds())

	now = time.Now()

	batch := 10000

	for i := 0; i < len(ds); i += batch {
		j := math.Min(float64(i+batch), float64(len(ds)))

		if _, err := c.db.NamedExecContext(ctx, "insert into data (timestamp, rate) values(:timestamp, :rate)", ds[i:int(j)]); err != nil {
			return nil, err
		}
	}

	log.Printf("data saved to db in %dms", time.Since(now).Milliseconds())

	if err := ioutil.WriteFile("data.json.sha256", []byte(newhash), 0644); err != nil {
		return nil, err
	}

	return ds, nil
}
