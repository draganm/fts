package fts

import (
	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
	"github.com/pkg/errors"
)

type IndexReader struct {
	tx        bolted.ReadTx
	indexPath dbpath.Path
}

func NewIndexReader(tx bolted.ReadTx, indexPath dbpath.Path) *IndexReader {
	return &IndexReader{
		tx:        tx,
		indexPath: indexPath,
	}
}

func (i *IndexReader) Search(query string, limit int) ([]string, error) {
	tokens, err := stringToTokens(query)
	if err != nil {
		return nil, errors.Wrap(err, "while tokenizing query")
	}

	if len(tokens) == 0 {
		return nil, nil
	}

	firstToken := tokens[0]

	rest := tokens[1:]

	tx := i.tx

	firstTokenReveseIdexPath := i.indexPath.Append("inverted", firstToken)

	if !tx.Exists(firstTokenReveseIdexPath) {
		return nil, nil
	}

	result := []string{}

outer:
	for it := tx.Iterator(firstTokenReveseIdexPath); !it.Done; it.Next() {
		for _, ot := range rest {
			documentForwardIndexPath := i.indexPath.Append("documents", it.Key, ot)
			if !tx.Exists(documentForwardIndexPath) {
				continue outer
			}
		}
		result = append(result, it.Key)
		if len(result) >= limit {
			break
		}
	}

	return result, nil
}
