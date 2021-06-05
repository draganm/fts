package fts

import (
	"strings"

	"github.com/blevesearch/bleve/v2/analysis/lang/en"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/whitespace"
	"github.com/blevesearch/bleve/v2/registry"
)

func stringToTokens(s string) ([]string, error) {
	cache := registry.NewCache()
	wst, err := cache.Tokenizers.TokenizerNamed(whitespace.Name, cache)
	if err != nil {
		return nil, err
	}

	tf, err := cache.TokenFilters.TokenFilterNamed(en.SnowballStemmerName, cache)
	if err != nil {
		return nil, err
	}

	tokenStream := wst.Tokenize([]byte(s))

	tokenStream = tf.Filter(tokenStream)

	tokens := map[string]struct{}{}

	for _, t := range tokenStream {
		tokens[strings.ToLower(string(t.Term))] = struct{}{}
	}

	output := []string{}

	for _, t := range tokenStream {
		lower := strings.ToLower(string(t.Term))
		_, found := tokens[lower]
		if found {
			output = append(output, lower)
		}
		delete(tokens, lower)
	}

	return output, nil

}
