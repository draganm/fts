package fts_test

import (
	"testing"

	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/draganm/fts"
	"github.com/stretchr/testify/require"
)

func TestIndexReader(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	indexPath := dbpath.ToPath("index")

	err := db.Write(func(tx bolted.WriteTx) error {
		tx.CreateMap(indexPath)
		iw := fts.NewIndexWriter(tx, dbpath.ToPath("index"))
		err := iw.Index("a", "this is a test")
		if err != nil {
			return err
		}
		err = iw.Index("b", "this is not a test")
		if err != nil {
			return err
		}

		return nil

	})

	err = db.Read(func(tx bolted.ReadTx) error {
		ir := fts.NewIndexReader(tx, indexPath)
		res, err := ir.Search("this", 10)
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b"}, res)

		res, err = ir.Search("is", 10)
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b"}, res)

		res, err = ir.Search("a", 10)
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b"}, res)

		res, err = ir.Search("not a", 10)
		require.NoError(t, err)
		require.Equal(t, []string{"b"}, res)

		return nil
	})

	require.NoError(t, err)
}
