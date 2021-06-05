package fts

import (
	"strings"

	"github.com/blevesearch/bleve/v2/analysis/lang/en"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/whitespace"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

type IndexWriter struct {
	tx        bolted.WriteTx
	indexPath dbpath.Path
}

func NewIndexWriter(tx bolted.WriteTx, indexPath dbpath.Path) *IndexWriter {
	return &IndexWriter{
		tx:        tx,
		indexPath: indexPath,
	}
}

func (i *IndexWriter) Index(id, text string) error {

	tx := i.tx

	documentsPath := i.indexPath.Append("documents")

	invertedPath := i.indexPath.Append("inverted")

	if !tx.Exists(documentsPath) {
		tx.CreateMap(documentsPath)
	}

	if !tx.Exists(invertedPath) {
		tx.CreateMap(invertedPath)
	}

	documentPath := documentsPath.Append(id)

	if tx.Exists(documentPath) {
		for it := tx.Iterator(documentPath); !it.Done; it.Next() {
			documentTokenPath := invertedPath.Append(it.Key, id)
			tx.Delete(documentTokenPath)
		}
	} else {
		tx.CreateMap(documentPath)
	}

	cache := registry.NewCache()
	wst, err := cache.Tokenizers.TokenizerNamed(whitespace.Name, cache)
	if err != nil {
		return err
	}

	tf, err := cache.TokenFilters.TokenFilterNamed(en.SnowballStemmerName, cache)
	if err != nil {
		return err
	}

	tokenStream := wst.Tokenize([]byte(text))

	tokenStream = tf.Filter(tokenStream)

	tokens := map[string]struct{}{}

	for _, t := range tokenStream {
		tokens[strings.ToLower(string(t.Term))] = struct{}{}
	}

	for t := range tokens {
		tokenPath := invertedPath.Append(t)
		if !tx.Exists(tokenPath) {
			tx.CreateMap(tokenPath)
		}
		tokenDocumentPath := tokenPath.Append(id)
		tx.Put(tokenDocumentPath, []byte{})

		documentTokenPath := documentPath.Append(t)
		tx.Put(documentTokenPath, []byte{})
	}

	return nil

}
