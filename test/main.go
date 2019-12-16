package main

import (
	"bytes"
	"fmt"
	"gaeadb/db"
	"log"
)

func main() {
	cfg := db.DefaultConfig()
	cfg.DirName = "test.db"
	db, err := db.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	{
		for i := 0; i < 100; i++ {
			if err := db.Set([]byte(fmt.Sprintf("/u/b/u_%v", i)), []byte(fmt.Sprintf("%v", i))); err != nil {
				log.Fatal(err)
			}
		}
	}
	{
		for i := 0; i < 100; i++ {
			if v, err := db.Get([]byte(fmt.Sprintf("/u/b/u_%v", i))); err != nil {
				log.Fatal(err)
			} else {
				if bytes.Compare(v, []byte(fmt.Sprintf("%v", i))) != 0 {
					log.Fatal(fmt.Errorf("%s is not %v - %v\n", fmt.Sprintf("/u/b/u_%v", i), fmt.Sprintf("%v", i), v))
				}
			}
		}
	}
	{
		tx, err := db.NewTransaction()
		if err != nil {
			log.Fatal(err)
		}
		defer tx.Rollback()
		itr, err := tx.NewForwardIterator(nil)
		if err != nil {
			log.Fatal(err)
		}
		for itr.Valid() {
			k := itr.Key()
			v, _ := itr.Value()
			fmt.Printf("%s: %s\n", string(k), string(v))
			itr.Next()
		}
		itr.Close()
	}
	{
		tx, err := db.NewTransaction()
		if err != nil {
			log.Fatal(err)
		}
		defer tx.Rollback()
		itr, err := tx.NewBackwardIterator(nil)
		if err != nil {
			log.Fatal(err)
		}
		for itr.Valid() {
			k := itr.Key()
			v, _ := itr.Value()
			fmt.Printf("%s: %s\n", string(k), string(v))
			itr.Next()
		}
		itr.Close()
	}
}
