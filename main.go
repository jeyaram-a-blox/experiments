package main

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	// "github.com/blevesearch/bleve/v2"
	gocommons "github.com/jeyaram-a/go-commons"
)

type Property struct {
	Id        int    `json:"id"`
	Name      string `json:"name"`
	Builder   string `json:"builder"`
	RmName    string `json:"rmName"`
	RmPhNo    string `json:"rmPhNo"`
	Aminities []int  `json:"aminities"`
	MinPrice  int    `json:"minPrice"`
	MaxPrice  int    `json:"maxPrice"`
}

var builders = []string{"shoba", "lodha", "prestige", "dlf"}

func NewProperty(i int) *Property {
	min := rand.Intn(1000)
	max := min + rand.Intn(1000)
	return &Property{
		Id:        i,
		Name:      fmt.Sprintf("name_%d", i),
		Builder:   builders[rand.Intn(len(builders))],
		RmName:    fmt.Sprintf("randomRm_%d", i),
		RmPhNo:    "23243434",
		Aminities: []int{1, 3, 4, 5, 6},
		MinPrice:  min,
		MaxPrice:  max,
	}
}

func r() {
	count := 0
	start := time.Now()
	for i := 0; i < 100000; i++ {
		count += 1
	}
	fmt.Printf("count took %+v", time.Now().Sub(start))
}

func searchLinear(props []*Property, min, max int, builder string) {
	start := time.Now()
	count := 0
	for j, _ := range props {
		if props[j].MinPrice >= min && props[j].MaxPrice <= max && props[j].Builder == builder {
			count += 1
		}
	}
	fmt.Printf("linear took %+v\n", time.Now().Sub(start))
	fmt.Println("linear count ", count)
}

func getStart(len, chunk int) []int {
	noc := len / chunk
	if len%chunk != 0 {
		noc += 1
	}
	startArr := make([]int, noc)
	for i := 0; i < noc; i++ {
		startArr[i] = i * chunk
	}

	return startArr
}

func searchLinearOptimzed(props []*Property, min, max int, builder string) {
	start := time.Now()
	chunk := len(props) / 10
	job := gocommons.NewTransformer[int, int](10, getStart(len(props), chunk), func(i int) (int, error) {
		count := 0
		for j := i; j < (i + chunk); j++ {
			if props[j].MinPrice >= min && props[j].MaxPrice <= max && props[j].Builder == builder {
				count += 1
			}
		}
		return count, nil
	}).Transform(nil)
	results := job.Get()
	total := 0
	for _, result := range results {
		total += result.ReturnVal
	}
	fmt.Printf("linear optimized took %+v\n", time.Now().Sub(start))
	fmt.Printf("linear optimized count %d\n", total)
}

func searchBinary(props []*Property, min, max int, builder string) {
	start := time.Now()
	ind, _ := sort.Find(len(props), func(i int) int {
		if min < props[i].MinPrice {
			return -1
		} else if min == props[i].MinPrice {
			return 0
		} else {
			return 1
		}
	})

	j := ind
	count := 0
	for ; j < len(props); j++ {
		if props[j].MinPrice >= min && props[j].MaxPrice <= max && props[j].Builder == builder {
			count += 1
		}
	}

	fmt.Printf("binary took %+v \n", time.Now().Sub(start))
	fmt.Println("binary count", count)
}

func searchInMemory(props []*Property, builderMap map[string]map[*Property]bool, min, max int, builder string) {
	start := time.Now()
	matches := make(map[*Property]bool)
	ind, found := sort.Find(len(props), func(i int) int {
		if min < props[i].MinPrice {
			return -1
		} else if min == props[i].MinPrice {
			return 0
		} else {
			return 1
		}
	})
	j := 0
	if found {
		j = ind
	} else {
		j = ind + 1
	}
	for j = ind; j < len(props); j++ {
		if min > props[j].MinPrice {
			break
		}
		if max <= props[j].MaxPrice {
			matches[props[j]] = true
		}
	}
	builderProps, present := builderMap[builder]
	if !present {
		panic("bad builder")
	}

	newMatch := make(map[*Property]bool)
	for property, _ := range builderProps {
		_, present := matches[property]
		if present {
			newMatch[property] = true
		}
	}
	fmt.Printf("memory took %+v \n", time.Now().Sub(start))
}

func main() {
	size := 10000000
	props := make([]*Property, size)
	builderMap := make(map[string]map[*Property]bool)
	for i := 0; i < size; i++ {
		props[i] = NewProperty(i)
		val, present := builderMap[props[i].Builder]
		if !present {
			val = make(map[*Property]bool)
			val[props[i]] = true
			builderMap[props[i].Builder] = val
		} else {
			val[props[i]] = true
		}
	}

	sstart := time.Now()
	sort.Slice(props, func(i, j int) bool {
		return props[i].MinPrice < props[j].MinPrice
	})
	fmt.Printf("sorting took %+v\n", time.Now().Sub(sstart))
	min := 109
	max := 678
	searchLinear(props, min, max, "lodha")
	searchBinary(props, min, max, "lodha")
	//searchInMemory(props, builderMap, min, max, "lodha")
	searchLinearOptimzed(props, min, max, "lodha")
	// _, err := bleve.New("test", bleve.NewIndexMapping())
	// index, err := bleve.Open("test")

	// if err != nil {
	// 	panic(err)
	// }
	// batch := index.NewBatch()
	// for i, prop := range props {
	// 	if i%10000 == 0 {
	// 		fmt.Println(i)
	// 		index.Batch(batch)
	// 		batch = index.NewBatch()
	// 	}
	// 	batch.Index(fmt.Sprintf("%d", prop.Id), prop)
	// }
	// min := 0.02
	// max := 0.8
	// priceQuery := bleve.NewNumericRangeQuery(&min, &max)
	// builderQuery := bleve.NewPhraseQuery([]string{"lodha"}, "builder")
	// conjunctionQuery := bleve.NewConjunctionQuery(priceQuery, builderQuery)
	// searchRequest := bleve.NewSearchRequest(conjunctionQuery)
	// searchRequest.Fields = []string{"*"}
	// searchRequest.Size = 100
	// start := time.Now()
	// result, err := index.Search(searchRequest)
	// if err != nil {
	// 	panic(err)
	// }
	// end := time.Now()

	// fmt.Printf("took %+v\n", end.Sub(start))
	// fmt.Println("results ", result.Size())
}
