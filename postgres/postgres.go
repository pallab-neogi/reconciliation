package postgresdb

import (
	"context"
	"encoding/json"
	"fmt"
	"reconciliation/transformer"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MatchedTrans struct {
	ID            string    `json:"id"`
	DocType       string    `json:"doctype"`
	Transdatetime time.Time `json:"transdatetime"`
	Uetr          string    `json:"uetr"`
}

// createPostgresConnectionPool creates a connection pool for PostgreSQL
func CreatePostgresConnectionPool(connStr string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %v", err)
	}

	config.MaxConns = 10
	config.MaxConnIdleTime = 5 * time.Minute
	config.MaxConnLifetime = 1 * time.Hour

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %v", err)
	}

	return pool, nil
}

// bulkInsertUsers inserts users into the PostgreSQL table
func BulkInsertTrans(pool *pgxpool.Pool, matchedTransByte []byte, tablename string) error {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("unable to acquire a connection: %v", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(context.Background())

	if err != nil {
		return fmt.Errorf("unable to start transaction: %v", err)
	}

	var matchedTrans []MatchedTrans

	if err := json.Unmarshal(matchedTransByte, &matchedTrans); err != nil {
		panic(err)
	}

	fmt.Printf("Printf Printf trans: %v", string(matchedTransByte))

	stmt := fmt.Sprintf(`INSERT INTO %s (id, doctype, transdatetime, uetr) VALUES (@id, @doctype, @transdatetime, @uetr)
	ON CONFLICT (id) DO UPDATE SET doctype = EXCLUDED.doctype, transdatetime=EXCLUDED.transdatetime, uetr=EXCLUDED.uetr`, tablename)

	batch := &pgx.Batch{}

	for _, trans := range matchedTrans {

		args := pgx.NamedArgs{
			"id":            trans.ID,
			"doctype":       trans.DocType,
			"transdatetime": trans.Transdatetime,
			"uetr":          trans.Uetr,
		}
		batch.Queue(stmt, args)
	}

	br := tx.SendBatch(context.Background(), batch)
	if err := br.Close(); err != nil {
		if rbErr := tx.Rollback(context.Background()); rbErr != nil {
			return fmt.Errorf("error rolling back transaction: %v", rbErr)
		}
		return fmt.Errorf("error executing batch: %v", err)
	}

	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("unable to commit transaction: %v", err)
	}

	return nil
}

// bulkInsertUsers inserts users into the PostgreSQL table
func BulkInsertUnmatchTrans(pool *pgxpool.Pool, unmatchedTransByte []byte, doctype string, Topic string) error {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("unable to acquire a connection: %v", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(context.Background())

	if err != nil {
		return fmt.Errorf("unable to start transaction: %v", err)
	}

	//var queryReturn []map[string]interface{}

	batch := &pgx.Batch{}

	var stmt string

	switch doctype {

	case "UPF", "CASA", "CARD":

		var queryReturnUPF []transformer.UPF

		stmt = fmt.Sprintf(`INSERT INTO unmatch_trans_UPF_CASA_CARD_%s (id, doctype, recordtype, accountnumber, todaysdate, amount, entrysign,transdatetime,ref1,uetr,ref2,ref4,status,reason,reconcile,crtdate,upddt) VALUES (@id, @doctype, @recordtype, @accountnumber, @todaysdate, @amount, @entrysign,@transdatetime,@ref1,@uetr,@ref2,@ref4,@status,@reason,@reconcile,@crtdate,@upddt)
		ON CONFLICT (id) DO NOTHING`, Topic)

		if err := json.Unmarshal(unmatchedTransByte, &queryReturnUPF); err != nil {
			panic(err)
		}

		for _, trans := range queryReturnUPF {
			fmt.Printf("Printf Printf trans: %v", trans)

			//transdt, _ := time.Parse(time.RFC3339, trans.TransDateTime)

			args := pgx.NamedArgs{
				"id":            trans.ID,
				"doctype":       trans.DocType,
				"recordtype":    trans.RecordType,
				"accountnumber": trans.AccountNumber,
				"todaysdate":    trans.TodaysDate,
				"amount":        trans.Amount,
				"entrysign":     trans.EntrySign,
				"transdatetime": trans.TransDateTime,
				"ref1":          trans.Reference1,
				"uetr":          trans.UetrId,
				"ref2":          trans.Reference2,
				"ref4":          trans.Reference4,
				"status":        trans.Status,
				"reason":        trans.ReasonCode,
				"reconcile":     trans.Reconciled,
				"crtdate":       trans.CreatedDt,
				"upddt":         trans.UpdateDt,
			}
			batch.Queue(stmt, args)
		}

	case "MARKOFFACC":

		var queryReturnMARKOFF []transformer.MarkOffAcc

		stmt = fmt.Sprintf(`INSERT INTO unmatch_trans_markoffacc_%s (id, doctype, product, scheme, msgid, endtoendid, uetr,instructbic,creditoragentbic,amount,currency,debtorname,
			creditorname,settlementdate,transdatetime,settlementpoolref,entrysign,reconcile,status,crtdate,upddt) 
			VALUES (@id, @doctype, @product, @scheme, @msgid, @endtoendid, @uetr,@instructbic,@creditoragentbic,@amount,@currency,@debtorname,@creditorname,
				@settlementdate,@transdatetime,@settlementpoolref,@entrysign,@reconcile,@status,@crtdate,@upddt)
			ON CONFLICT (id) DO NOTHING`, Topic)

		if err := json.Unmarshal(unmatchedTransByte, &queryReturnMARKOFF); err != nil {
			panic(err)
		}

		for _, trans := range queryReturnMARKOFF {
			fmt.Printf("Printf Printf trans: %v", trans)

			args := pgx.NamedArgs{
				"id":                trans.ID,
				"doctype":           trans.DocType,
				"product":           trans.Product,
				"scheme":            trans.Scheme,
				"msgid":             trans.MsgID,
				"endtoendid":        trans.EndToEndID,
				"uetr":              trans.UetrId,
				"instructbic":       trans.InstructBIC,
				"creditoragentbic":  trans.CreditorAgentBIC,
				"amount":            trans.Amount,
				"currency":          trans.Currency,
				"debtorname":        trans.DebtorName,
				"creditorname":      trans.CreditorName,
				"settlementdate":    trans.SettlementDate,
				"transdatetime":     trans.TransDateTime,
				"settlementpoolref": trans.SettlementPoolRef,
				"entrysign":         trans.Entrysign,
				"reconcile":         trans.Reconciled,
				"status":            trans.Status,
				"crtdate":           trans.CreatedDt,
				"upddt":             trans.UpdateDt,
			}
			batch.Queue(stmt, args)
		}
	case "MARKOFFREJ":

		var queryReturnMARKOFF []transformer.MarkOffRej

		stmt = fmt.Sprintf(`INSERT INTO unmatch_trans_markoffrej_%s (id, doctype, product, scheme, msgid, endtoendid, uetr,instructbic,payeeagentbic,amount,currency,category,status,reasoncode,reasondesc,transdatetime,entrysign,reconcile,crtdate,upddt) 
			VALUES (@id, @doctype, @product, @scheme, @msgid, @endtoendid, @uetr,@instructbic,@payeeagentbic,@amount,@currency,@category,@status,
				@reasoncode,@reasondesc,@transdatetime,@entrysign,@reconcile,@crtdate,@upddt)
			ON CONFLICT (id) DO NOTHING`, Topic)

		if err := json.Unmarshal(unmatchedTransByte, &queryReturnMARKOFF); err != nil {
			panic(err)
		}

		for _, trans := range queryReturnMARKOFF {
			fmt.Printf("Printf Printf trans: %v", trans)

			args := pgx.NamedArgs{
				"id":            trans.ID,
				"doctype":       trans.DocType,
				"product":       trans.Product,
				"scheme":        trans.Scheme,
				"msgid":         trans.MsgID,
				"endtoendid":    trans.EndToEndID,
				"uetr":          trans.UetrId,
				"instructbic":   trans.InstructBIC,
				"payeeagentbic": trans.PayeeBIC,
				"amount":        trans.Amount,
				"currency":      trans.Currency,
				"category":      trans.Category,
				"status":        trans.Status,
				"reasoncode":    trans.Reasoncode,
				"reasondesc":    trans.ReasonDesc,
				"transdatetime": trans.TransDateTime,
				"entrysign":     trans.Entrysign,
				"reconcile":     trans.Reconciled,
				"crtdate":       trans.CreatedDt,
				"upddt":         trans.UpdateDt,
			}
			batch.Queue(stmt, args)

		}
	case "GL":

		var queryReturnGL []transformer.GL

		stmt = fmt.Sprintf(`INSERT INTO unmatch_trans_GL_%s (id,	doctype,recon,	recordtype,account,currency,transdatetime,amount,entrysign,	Date1,Date2,uetr,branch,reconcile,crtdate,upddt) 
			VALUES (@id, @doctype,@recon,@recordtype,@account,@currency,@transdatetime,@amount,@entrysign,	@Date1,@Date2,@uetr,@branch,@reconcile,@crtdate,@upddt)
			ON CONFLICT (id) DO NOTHING`, Topic)

		if err := json.Unmarshal(unmatchedTransByte, &queryReturnGL); err != nil {
			panic(err)
		}

		for _, trans := range queryReturnGL {
			fmt.Printf("Printf Printf trans: %v", trans)

			args := pgx.NamedArgs{
				"id":            trans.ID,
				"doctype":       trans.DocType,
				"recon":         trans.Recon,
				"recordtype":    trans.RecordType,
				"account":       trans.Account,
				"currency":      trans.Currency,
				"transdatetime": trans.TransDateTime,
				"amount":        trans.Amount,
				"entrysign":     trans.Entrysign,
				"Date1":         trans.Date1,
				"Date2":         trans.Date2,
				"uetr":          trans.UetrId,
				"branch":        trans.Branch,
				"reconcile":     trans.Reconciled,
				"crtdate":       trans.CreatedDt,
				"upddt":         trans.UpdateDt,
			}
			batch.Queue(stmt, args)
		}

	}

	br := tx.SendBatch(context.Background(), batch)

	if err := br.Close(); err != nil {
		if rbErr := tx.Rollback(context.Background()); rbErr != nil {
			return fmt.Errorf("error rolling back transaction: %v", rbErr)
		}
		return fmt.Errorf("error executing batch: %v", err)
	}

	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("unable to commit transaction: %v", err)
	}

	return nil
}
