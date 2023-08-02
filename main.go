package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/api/support/bundler"
)

type Request struct {
	BankID    int
	CompanyID int
	InvID     int
	Output    chan string
}

var listActiveVA = make(map[string][]string)
var templateID = "%v_%v"

func main() {
	// chn := make(chan productannotation.AnnotationData)
	bnd := bundler.NewBundler(Request{}, Handle)
	bnd.DelayThreshold = 2 * time.Second
	bnd.HandlerLimit = 1
	bnd.BundleCountThreshold = 2

	listCompanyID := []int{1, 2}
	listBankID := []int{1}

	inv := 0
	for _, bankID := range listBankID {
		for _, companyID := range listCompanyID {
			for i := 1; i < 5; i++ {
				inv++
				go SendMessage(bnd, companyID, bankID, inv)
			}
		}
	}

	time.Sleep(5 * time.Second)
}

func SendMessage(service *bundler.Bundler, CompanyID int, BankID int, invID int) {
	res := make(chan string, 1)
	service.AddWait(context.Background(), Request{CompanyID: CompanyID, BankID: BankID, InvID: invID, Output: res}, 1)
	fmt.Printf("Comp-%v INV-%v got VA:%v \n", CompanyID, invID, <-res)
}

func Handle(entries interface{}) {
	fmt.Println("Handle")
	list := entries.([]Request)
	listData := make(map[string]int)
	mapCompanyToInvoiceID := make(map[string][]int)
	for _, x := range list {
		form := fmt.Sprintf(templateID, x.BankID, x.CompanyID)
		listData[form]++
		mapCompanyToInvoiceID[form] = append(mapCompanyToInvoiceID[form], x.InvID)
	}

	var mapCompGeneratedVA = make(map[string]map[int]string)
	for key, _ := range listData {
		mapCompGeneratedVA[key] = generateVA(key, mapCompanyToInvoiceID[key])
	}

	for _, x := range list {
		form := fmt.Sprintf(templateID, x.BankID, x.CompanyID)
		x.Output <- mapCompGeneratedVA[form][x.InvID]
	}
}

func generateVA(key string, listInvID []int) map[int]string {
	res := make(map[int]string, len(listInvID))
	lastActive := len(listActiveVA[key]) + 1
	for i, data := range listInvID {
		res[data] = fmt.Sprintf("VA-%v", lastActive+i)
		listActiveVA[key] = append(listActiveVA[key], fmt.Sprintf("VA-%v", lastActive+i))
	}

	return res
}
