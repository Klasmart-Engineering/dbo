package main

import (
	"context"
	"log"
	"gitlab.badanamu.com.cn/calmisland/dbo"
)

func main() {
	db, err := dbo.GetDB(context.TODO())
	if err != nil {
		log.Printf("get db failed due to %+v\n", err)
		return
	}

	rows, err := db.Table("users").Select("user_id, name").Where("user_id<?", 10).Rows()
	if err != nil {
		log.Printf("get db failed due to %+v\n", err)
		return
	}
	defer rows.Close()

	var id int
	var name string
	for rows.Next() {
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Printf("scan rows failed due to %+v\n", err)
			return
		}

		log.Printf("user_id: %d  name: %s\n", id, name)
	}
}
