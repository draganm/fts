package fts_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/fts"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) (*bolted.Bolted, func()) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	b, err := bolted.Open(filepath.Join(td, "db"), 0700)
	require.NoError(t, err)

	return b, func() {
		b.Close()
		os.RemoveAll(td)
	}
}

func TestIndexWriter(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	err := db.Write(func(tx bolted.WriteTx) error {
		tx.CreateMap(dbpath.ToPath("index"))
		iw := fts.NewIndexWriter(tx, dbpath.ToPath("index"))
		return iw.Index("a", "Foo")
	})

	err = db.Read(func(tx bolted.ReadTx) error {
		require.True(t, tx.Exists(dbpath.ToPath("index", "documents", "a")))
		require.True(t, tx.Exists(dbpath.ToPath("index", "inverted", "foo")))
		require.True(t, tx.Exists(dbpath.ToPath("index", "inverted", "foo", "a")))
		return nil
	})

	require.NoError(t, err)
}
