package main

import (
	"database/sql"
	_ "github.com/lib/pq"
	"log"
	"os"
	"encoding/csv"
	"fmt"
	"flag"
	"strings"
)


func Querygen() string {
	billType := flag.String("t","dbr","Billing type: dbr or cur")
	billMonth := flag.String("m","201712","Billing month: yyyymm")
	billCustomer := flag.String("c","cmcm","Billing customer: cmcm or byte")

	flag.Parse()

	query := "SELECT SUBSTRING(resourceid,POSITION('distribution/' IN resourceid) +13) AS DistributionID,SUM(TO_NUMBER(TRIM(BOTH ' ' FROM unblendedcost),'9999999D99999999')) AS COST FROM CUSTOMER_BILLING_MONTH WHERE productname = 'Amazon CloudFront' AND resourceid NOT LIKE 'ASCA%' GROUP BY resourceid ORDER BY 1"
	
	query = strings.Replace(query,"MONTH",*billMonth,1)
	query = strings.Replace(query,"BILLING",*billType,1)
	query = strings.Replace(query,"CUSTOMER",*billCustomer,1)

	return query

}

func main() {
	connStr := "user=zzhe dbname=cmcmbilling port=5439 password=8uhb)OKM host=cmcmbilling.c2upikbi0jsb.ap-northeast-2.redshift.amazonaws.com "
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create("cf.csv")
	if err != nil {
		log.Fatal(err)
	}

	w := csv.NewWriter(f)


	var (
		distributionid string
		cost string
		)

	query := Querygen()


	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	w.Write([]string{"distributionid","cost"})
	w.Flush()

	for rows.Next() {
		err := rows.Scan(&distributionid,&cost)
		if err != nil {
			log.Fatal(err)
		}
		
		w.Write([]string{distributionid,cost})
		w.Flush()


		err = rows.Err()
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println(query)
	fmt.Println("Result has been writen in cf.csv ")
}