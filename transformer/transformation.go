package transformer

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type UPF struct {
	ID            string    `json:"id"`
	DocType       string    `json:"doctype"`
	RecordType    string    `json:"recordtype"`
	AccountNumber string    `json:"accountnumber"`
	TodaysDate    string    `json:"todaysdate"`
	Amount        string    `json:"amount"`
	EntrySign     string    `json:"entrysign"`
	TransDateTime time.Time `json:"transdatetime"` //value date
	Reference1    string    `json:"ref1"`
	UetrId        string    `json:"uetr"`
	Reference2    string    `json:"ref2"`
	Reference4    string    `json:"ref4"`
	Status        string    `json:"status"`
	ReasonCode    string    `json:"reason"`
	Reconciled    bool      `json:"reconcile"`
	CreatedDt     string    `json:"crtdate"`
	UpdateDt      string    `json:"upddt"`
}

type GL struct {
	ID            string    `json:"id"`
	DocType       string    `json:"doctype"`
	Recon         string    `json:"recon"`
	RecordType    string    `json:"recordtype"`
	Account       string    `json:"account"`
	Currency      string    `json:"currency"`
	TransDateTime time.Time `json:"transdatetime"`
	Amount        float64   `json:"amount"`
	Entrysign     string    `json:"entrysign"`
	Date1         string    `json:"Date1"`
	Date2         string    `json:"Date2"`
	UetrId        string    `json:"uetr"`
	Branch        string    `json:"branch"`
	Reconciled    bool      `json:"reconcile"`
	CreatedDt     string    `json:"crtdate"`
	UpdateDt      string    `json:"upddt"`
}

type MarkOffAcc struct {
	ID                string    `json:"id"`
	DocType           string    `json:"doctype"`
	Product           string    `json:"product"`
	Scheme            string    `json:"scheme"`
	MsgID             string    `json:"msgid"`
	EndToEndID        string    `json:"endtoendid"`
	UetrId            string    `json:"uetr"`
	UetrId30          string    `json:"uetr30"`
	InstructBIC       string    `json:"instructbic"`
	CreditorAgentBIC  string    `json:"creditoragentbic"`
	Amount            float64   `json:"amount"`
	Currency          string    `json:"currency"`
	DebtorName        string    `json:"debtorname"`
	CreditorName      string    `json:"creditorname"`
	SettlementDate    string    `json:"settlementdate"`
	TransDateTime     time.Time `json:"transdatetime"`
	SettlementPoolRef string    `json:"settlementpoolref"`
	Entrysign         string    `json:"entrysign"`
	Reconciled        bool      `json:"reconcile"`
	Status            string    `json:"status"`
	CreatedDt         string    `json:"crtdate"`
	UpdateDt          string    `json:"upddt"`
}

type MarkOffRej struct {
	ID            string    `json:"id"`
	DocType       string    `json:"doctype"`
	Product       string    `json:"product"`
	Scheme        string    `json:"scheme"`
	MsgID         string    `json:"msgid"`
	EndToEndID    string    `json:"endtoendid"`
	UetrId        string    `json:"uetr"`
	InstructBIC   string    `json:"instructbic"`
	PayeeBIC      string    `json:"payeeagentbic"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	Category      string    `json:"category"`
	Status        string    `json:"status"`
	Reasoncode    string    `json:"reasoncode"`
	ReasonDesc    string    `json:"reasondesc"`
	TransDateTime time.Time `json:"transdatetime"`
	Entrysign     string    `json:"entrysign"`
	Reconciled    bool      `json:"reconcile"`
	CreatedDt     string    `json:"crtdate"`
	UpdateDt      string    `json:"upddt"`
}

func TransformRecord(record []string, dirName string, ChannelName string) ([]byte, string, error) {

	var jsonData []byte

	id, err := uuid.NewUUID()
	if err != nil {
		fmt.Println("Error generating UUID:", err)
		return nil, "", err
	}

	fmt.Println("record:", record[0])

	switch dirName {
	case "UPF", "CARD", "CASA":

		var rec UPF
		rec.ID = id.String()
		//rec.RecordType = record[0]

		logrus.Printf("Dirname %v", dirName)

		rec.DocType = dirName

		rec.RecordType = record[0][:1]
		rec.AccountNumber = record[0][1:36]
		rec.TodaysDate = record[0][36:42]
		rec.Amount = record[0][42:60]
		rec.EntrySign = strings.TrimRight(record[0][60:62], " ")
		rec.TransDateTime, _ = convertToISO8601("20"+record[0][62:68], "20060102")
		rec.Reference1 = record[0][68:118]

		rec.UetrId = record[0][118:154]
		rec.Reference2 = record[0][154:170]
		rec.Reference4 = record[0][170:560]

		rec.Status = record[0][560:564]
		rec.ReasonCode = record[0][564:568]
		rec.Reconciled = false
		rec.CreatedDt = GetCurrentTime()
		rec.UpdateDt = GetCurrentTime()

		jsonData, _ = json.Marshal(rec)

	case "MARKOFFACC":
		var rec MarkOffAcc

		rec.ID = id.String()
		//rec.RecordType = record[0]

		logrus.Printf("ID %v", rec.ID)

		rec.DocType = dirName
		rec.Product = record[0]
		rec.Scheme = record[1]
		rec.MsgID = record[2]
		rec.EndToEndID = record[3]
		rec.UetrId = record[4]
		rec.UetrId30 = record[4][0:30]
		rec.InstructBIC = record[5]
		rec.CreditorAgentBIC = record[6]

		rec.Amount, _ = strconv.ParseFloat(record[7], 64)

		rec.Currency = record[8]
		rec.DebtorName = record[9]
		rec.CreditorName = record[10]
		rec.SettlementDate = record[11]

		rec.TransDateTime, err = convertToISO8601(record[12]+":00", "2006/01/02 15:04:05")
		if err != nil {
			fmt.Println("Date convert error:", err)
			return nil, "", err
		}

		if rec.InstructBIC == "NEDSZAJJ" {
			rec.Entrysign = "C"
		} else {
			rec.Entrysign = "D"
		}
		rec.SettlementPoolRef = record[13]
		rec.Reconciled = false

		rec.Status = "ACCC"

		rec.CreatedDt = GetCurrentTime()
		rec.UpdateDt = GetCurrentTime()

		jsonData, _ = json.Marshal(rec)

	case "GL":
		var rec GL

		rec.ID = id.String()
		//rec.RecordType = record[0]

		logrus.Printf("ID %v", rec.ID)

		rec.DocType = dirName

		rec.Recon = ChannelName

		if record[0][97:100] == "RPN" {
			rec.UetrId = "RPN"
		} else {
			rec.UetrId = record[0][97:127]
		}

		if ChannelName == "recon2" && rec.UetrId == "RPN" {
			return nil, "", nil
		} else if ChannelName == "recon3" && rec.UetrId != "RPN" {
			return nil, "", nil
		}

		rec.RecordType = strings.TrimSpace(record[0][:3])

		if rec.RecordType != "2" {
			return nil, "", nil
		}

		rec.Account = record[0][8:18]

		logrus.Printf("Account %v", rec.Account)

		rec.Currency = record[0][43:46]

		rec.TransDateTime, err = convertToISO8601("20"+record[0][46:52], "20060102")
		if err != nil {
			fmt.Println("Date convert error:", err)
			return nil, "", err
		}

		Amount := record[0][65:83]

		rec.Amount, _ = strconv.ParseFloat(Amount[:len(Amount)-4]+"."+Amount[len(Amount)-4:], 32)

		rec.Entrysign = strings.TrimSpace(record[0][83:84])
		rec.Date1 = record[0][85:91]
		rec.Date2 = record[0][91:97]

		rec.Branch = record[0][160:164]
		rec.Reconciled = false
		rec.CreatedDt = GetCurrentTime()
		rec.UpdateDt = GetCurrentTime()

		fmt.Println("GL jsonData:", rec)

		jsonData, _ = json.Marshal(rec)

	case "MARKOFFREJ":
		var rec MarkOffRej

		rec.ID = id.String()
		//rec.RecordType = record[0]

		logrus.Printf("ID %v", rec.ID)

		rec.DocType = dirName
		rec.Product = record[0]
		rec.Scheme = record[1]
		rec.MsgID = record[2]
		rec.EndToEndID = record[3]
		rec.UetrId = record[4]
		rec.InstructBIC = record[5]
		rec.PayeeBIC = record[6]

		rec.Amount = record[7]
		rec.Currency = record[8]
		rec.Category = record[9]
		rec.Status = record[10]
		rec.Reasoncode = record[11]
		rec.ReasonDesc = record[12]

		fmt.Println("record[13]", record[13])
		rec.TransDateTime, err = convertToISO8601(record[13], "2006-01-02 15:04:05")
		if err != nil {
			fmt.Println("Date convert error:", err)
			return nil, "", err
		}

		if rec.InstructBIC == "NEDSZAJJ" {
			rec.Entrysign = "C"
		} else {
			rec.Entrysign = "D"
		}

		rec.Reconciled = false
		rec.CreatedDt = GetCurrentTime()
		rec.UpdateDt = GetCurrentTime()

		jsonData, _ = json.Marshal(rec)

	}

	return jsonData, id.String(), nil
}

func GetCurrentTime() string {

	currentTime := time.Now()
	currentTimeUnix64 := currentTime.Unix()
	return strconv.FormatInt(currentTimeUnix64, 10)

}

// convertToISO8601 converts a date string in YYYYMMDD format to ISO 8601 format
func convertToISO8601(dateStr string, layout string) (time.Time, error) {
	// Define the layout corresponding to the input format "YYYYMMDD"

	// Parse the date string into a time.Time object using the layout

	logrus.Printf("dateStr: %s", dateStr)
	logrus.Printf("layout: %s", layout)
	parsedTime, err := time.Parse(layout, dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse date: %v", err)
	}

	logrus.Printf("parsedTime: %v", parsedTime)

	// Format the time.Time object into ISO 8601 format
	//iso8601Format := parsedTime.Format(time.RFC3339)
	return parsedTime, nil
}
