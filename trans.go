package dbo

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gitlab.badanamu.com.cn/calmisland/krypton/krconfig"

	log "gitlab.badanamu.com.cn/calmisland/common-cn/logger"
)

var (
	// dbTransactionTimeout transaction timeout
	dbTransactionTimeout = time.Second * 3
)

func getDBTransactionTimeout() time.Duration {
	second := krconfig.CommonShareConfig().Db.Mysql.TransactionTimeoutSecond
	if second > 0 {
		return time.Duration(int64(time.Second) * int64(second))
	}
	return dbTransactionTimeout
}

// GetTrans begin a transaction
func GetTrans(ctx context.Context, fn func(ctx context.Context, tx *DBContext) error) error {
	log.WithContext(ctx).Debug("begin transaction")

	ctxWithTimeout, cancel := context.WithTimeout(ctx, getDBTransactionTimeout())
	defer cancel()

	db, err := GetDB(ctxWithTimeout)
	if err != nil {
		return err
	}

	db.DB = db.BeginTx(ctxWithTimeout, &sql.TxOptions{})

	funcDone := make(chan error, 0)
	go func() {
		defer func() {
			if err1 := recover(); err1 != nil {
				log.WithStacks().
					WithField("recover error", err1).
					WithContext(ctxWithTimeout).
					Error("with transaction panic")
				funcDone <- fmt.Errorf("transaction panic: %+v", err1)
			}
		}()

		// call func
		funcDone <- fn(ctxWithTimeout, db)
	}()

	select {
	case err = <-funcDone:
		log.WithContext(ctxWithTimeout).Debug("transaction fn done")
	case <-ctxWithTimeout.Done():
		// context deadline exceeded
		err = ctxWithTimeout.Err()
		log.WithContext(ctxWithTimeout).
			WithError(err).
			WithStacks().
			Error("transaction context deadline exceeded")
	}

	if err != nil {
		err1 := db.RollbackUnlessCommitted().Error
		if err1 != nil {
			log.WithContext(ctxWithTimeout).
				WithError(err1).
				WithStacks().
				WithField("outer error", err.Error()).
				Error("rollback transaction failed")
		} else {
			log.WithContext(ctxWithTimeout).Debug("rollback transaction success")
		}
		return err
	}

	err = db.Commit().Error
	if err != nil {
		log.WithContext(ctxWithTimeout).
			WithError(err).
			WithStacks().
			Error("commit transaction failed")
		return err
	}

	log.WithContext(ctxWithTimeout).Debug("commit transaction success")

	return nil
}

type transactionResult struct {
	Result interface{}
	Error  error
}

// GetTransResult begin a transaction, get result of callback
func GetTransResult(ctx context.Context, fn func(ctx context.Context, tx *DBContext) (interface{}, error)) (interface{}, error) {
	log.WithStacks().WithContext(ctx).Debug("begin transaction")

	ctxWithTimeout, cancel := context.WithTimeout(ctx, getDBTransactionTimeout())
	defer cancel()

	db, err := GetDB(ctxWithTimeout)
	if err != nil {
		return nil, err
	}

	db.DB = db.BeginTx(ctxWithTimeout, &sql.TxOptions{})

	funcDone := make(chan *transactionResult, 0)
	go func() {
		defer func() {
			if err1 := recover(); err1 != nil {
				log.WithStacks().
					WithField("recover error", err1).
					WithContext(ctxWithTimeout).
					Error("with transaction panic")
				funcDone <- &transactionResult{Error: fmt.Errorf("transaction panic: %+v", err1)}
			}
		}()

		// call func
		result, err := fn(ctxWithTimeout, db)
		funcDone <- &transactionResult{Result: result, Error: err}
	}()

	var funcResult *transactionResult
	select {
	case funcResult = <-funcDone:
		log.WithStacks().
			WithContext(ctxWithTimeout).
			Debug("transaction fn done")
	case <-ctxWithTimeout.Done():
		// context deadline exceeded
		funcResult = &transactionResult{Error: ctxWithTimeout.Err()}
		log.WithStacks().
			WithContext(ctxWithTimeout).
			WithError(ctxWithTimeout.Err()).
			Error("transaction context deadline exceeded")
	}

	if funcResult.Error != nil {
		log.WithStacks().
			WithContext(ctxWithTimeout).
			WithError(funcResult.Error).
			Debug("transaction failed")

		err1 := db.RollbackUnlessCommitted().Error
		if err1 != nil {
			log.WithStacks().
				WithContext(ctxWithTimeout).
				WithError(err1).
				WithField("transaction error", funcResult.Error.Error()).
				Error("rollback transaction failed")
		} else {
			log.WithContext(ctxWithTimeout).
				WithStacks().
				WithError(funcResult.Error).
				Debug("rollback transaction success")
		}
		return nil, funcResult.Error
	}

	err = db.Commit().Error
	if err != nil {
		log.WithStacks().
			WithContext(ctxWithTimeout).
			WithError(err).
			Error("commit transaction failed")
		return nil, err
	}

	log.WithStacks().WithContext(ctxWithTimeout).Debug("commit transaction success")

	return funcResult.Result, nil
}
