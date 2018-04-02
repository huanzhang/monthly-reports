package main

import (
	"database/sql"
	_ "github.com/lib/pq"
	"log"
	"fmt"
	"flag"
	"strings"
	"errors"
	"math"
	"sort"
)

func getInputPar() (string,string,string) {
	billType := flag.String("t","dbr","Billing type: dbr or cur")
	billMonth := flag.String("m","201712","Billing month: yyyymm")
	billCustomer := flag.String("c","cmcm","Billing customer: cmcm or byte")

	flag.Parse()

	return *billType, *billMonth, *billCustomer
}

func genDatatransferusageQuery(billtype string, billmonth string, billcustomer string) (string, error) {

	if billcustomer != "cmcm" {
		return "", errors.New("This tool is only support for customer Cheetah mobile")
	}

	if billtype == "dbr" {

	query := `SELECT 'MONTH' AS month,
       'AWS Data Transfer' AS productname,
       CASE
         WHEN POSITION('EU-' IN usagetype) = 1 THEN 'EU'
         WHEN POSITION('-DataTransfer-' IN usagetype) > 0 THEN SUBSTRING(usagetype,1,4)
         WHEN POSITION('AWS' IN usagetype) > 0 THEN SUBSTRING(usagetype,1,4)
         ELSE 'USE1'
       END AS region,
       usagetype,
       SUM(TO_NUMBER(NULLIF(BTRIM(usagequantity),''),'9999999999D9999999999')) AS quantity,
       SUM(TO_NUMBER(NULLIF(BTRIM(blendedcost),''),'9999999999D9999999999')) AS blendedcost
FROM CUSTOMER_BILLING_MONTH
WHERE recordtype = 'LineItem'
AND   productname != 'Amazon CloudFront'
AND itemdescription != 'Enterprise Program Discount'
AND   (usagetype LIKE '%DataTransfer-Out-Bytes' 
        OR usagetype LIKE '%DataTransfer-In-Bytes' 
       OR usagetype LIKE '%DataTransfer-Regional-Bytes' 
        OR usagetype LIKE '%-AWS-In-Bytes' 
        OR usagetype LIKE '%-AWS-Out-Bytes' 
        OR usagetype LIKE '%AWS-In-ABytes%' 
        OR usagetype LIKE '%AWS-Out-ABytes%' 
        OR usagetype LIKE '%DataTransfer-Out-ABytes%')
GROUP BY CASE
           WHEN POSITION('EU-' IN usagetype) = 1 THEN 'EU'
           WHEN POSITION('-DataTransfer-' IN usagetype) > 0 THEN SUBSTRING(usagetype,1,4)
           WHEN POSITION('AWS' IN usagetype) > 0 THEN SUBSTRING(usagetype,1,4)
           ELSE 'USE1'
         END,
         usagetype;`
	
	query = strings.Replace(query,"MONTH",billmonth,2)
	query = strings.Replace(query,"BILLING",billtype,1)
	query = strings.Replace(query,"CUSTOMER",billcustomer,1)

	return query, nil

	} else {
	return "",errors.New("CUR is currently not support by this tool")
	}
}

/* Calculate the original cost according to private contract 201708 */
func calDataTransferOriginCost(region string, usagetype string, usage float64, blendedcost float64) float64{
	
	var cost float64

	const f = math.MaxFloat64

/* Price version 20180305 */
	aps1_dataout_pub_price := map[float64]float64{
		1: 0.000,
		10240: 0.120,
		40960: 0.085,
		102400: 0.082,
		358400: 0.080,
		f: 0.080,
	}

	us_can_eu_dataout_pub_price := map[float64]float64{
		1: 0.000,
		10240: 0.090,
		40960: 0.085,
		102400: 0.070,
		358400: 0.050,
		f: 0.050,
	}

	var sortkey []float64

	switch {
	case (strings.Contains("USE1,USE2,USW1,USW2,CAN1,EUC1,EUW2,EU",region) && strings.Contains(usagetype,"DataTransfer-Out-Bytes")): {

		for key := range us_can_eu_dataout_pub_price {
		sortkey = append(sortkey, key)
		}

		sort.Float64s(sortkey)

		remain_usage := usage

		for _, key := range sortkey {
			if remain_usage > key {
				cost = key * us_can_eu_dataout_pub_price[key]
				remain_usage = remain_usage - key 
			} else {
				cost = remain_usage * us_can_eu_dataout_pub_price[key] + cost
				break
			}
		}
	}
		
	case (strings.Contains("APS1",region) && strings.Contains(usagetype,"DataTransfer-Out-Bytes")): {

		for key := range aps1_dataout_pub_price {
		sortkey = append(sortkey, key)
		}

		sort.Float64s(sortkey)

		remain_usage := usage

		for _, key := range sortkey {
			if remain_usage > key {
				cost = key * aps1_dataout_pub_price[key]
				remain_usage = remain_usage - key 
			} else {
				cost = remain_usage * aps1_dataout_pub_price[key] + cost
				break
			}
		}
	}
	case (strings.Contains("USE1,USE2,USW1,USW2,CAN1,EUC1,EUW2,EU",region) && strings.Contains(usagetype,"DataTransfer-Regional-Bytes")):
		cost = usage * 0.01
	default :
		cost = blendedcost
	}	
	return cost

}

/* Calculate the Cost base on CMCM Private Contract 201608 */
func calDataTransferPrivate201608(region string, usagetype string, usage float64, blendedcost float64) float64 {
	
	var cost float64

	switch {
	case (strings.Contains("USE1,USE2,USW1,USW2,EUC1,EU",region) && strings.Contains(usagetype,"DataTransfer-Out-Bytes")):
		cost = usage * 0.027
	case (strings.Contains("APS1",region) && strings.Contains(usagetype,"DataTransfer-Out-Bytes")):
		cost = usage * 0.066
	case (strings.Contains("USE1,USE2,USW1,USW2,EUC1,EU",region) && strings.Contains(usagetype,"DataTransfer-Regional-Bytes")):
		cost = usage * 0.005
	default :
		cost = blendedcost
	}	
	return cost
}


func main() {

	var (
		month string
		productname string
		region string
		usagetype string
		quantity float64
		blendedcost float64
		total_origin_blendedcost float64
		pri_201708_blendedcost float64
		pri_201608_blendedcost float64
		us_eu_dto_usage float64
		ap_dto_usage float64
		us_eu_regional_usage float64
		)


billType,billMonth,billCustomer := getInputPar()


fmt.Println("Generating",billCustomer,"Datatransfer saving Report from",billType,billMonth,"...")

/* Connect to database */	
	connStr := "user=zzhe dbname=cmcmbilling port=5439 password=8uhb)OKM host=cmcmbilling.c2upikbi0jsb.ap-northeast-2.redshift.amazonaws.com "
	db, err := sql.Open("postgres", connStr)

	if err != nil {
		log.Fatal(err)
	}

/* Get data from billing */	
	query, err:= genDatatransferusageQuery(billType,billMonth,billCustomer)
	if err != nil {
		log.Fatal(err)
	} 

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

/* Calculate cost */
	for rows.Next() {
		err := rows.Scan(&month,&productname,&region,&usagetype,&quantity,&blendedcost)
		if err != nil {
			log.Fatal(err)
		}
		
		total_origin_blendedcost = total_origin_blendedcost + calDataTransferOriginCost(region,usagetype,quantity,blendedcost)

		pri_201708_blendedcost = pri_201708_blendedcost + blendedcost

		pri_201608_blendedcost = pri_201608_blendedcost + calDataTransferPrivate201608(region,usagetype,quantity,blendedcost)

		switch {
			case (strings.Contains("USE1,USE2,USW1,USW2,EUC1,EU",region) && strings.Contains(usagetype,"DataTransfer-Out-Bytes")):
				us_eu_dto_usage = us_eu_dto_usage + quantity
			case (strings.Contains("APS1",region) && strings.Contains(usagetype,"DataTransfer-Out-Bytes")):
				ap_dto_usage = ap_dto_usage + quantity
			case (strings.Contains("USE1,USE2,USW1,USW2,EUC1,EU",region) && strings.Contains(usagetype,"DataTransfer-Regional-Bytes")):
				us_eu_regional_usage = us_eu_regional_usage + quantity
		}

		err = rows.Err()

		if err != nil {
			log.Fatal(err)
		}

	}

/* Check commitment base on Private contract 201708 */
	us_eu_dto_commitment := "Y"
	if us_eu_dto_usage < 3.5 * 1024 * 1024 {
		us_eu_dto_commitment = "N"
	}

	ap_dto_commitment := "Y"
	if ap_dto_usage < 300 * 1024 {
		ap_dto_commitment = "N"
	}

	us_eu_regional_commitment := "Y"
	if us_eu_regional_usage < 1024 * 1024 {
		us_eu_regional_commitment = "N"
	}

/* Print result */
	fmt.Println("Report Month:",billMonth)
	fmt.Println("US_EU_DTO_Commitment:",us_eu_dto_commitment,"AP_DTO_Commitment:",ap_dto_commitment,"US_EU_Regional_Commitment:",us_eu_regional_commitment)
	fmt.Println("Origin:",total_origin_blendedcost,"201608:",pri_201608_blendedcost,"201708:",pri_201708_blendedcost,"Saving", pri_201608_blendedcost - pri_201708_blendedcost )

}



