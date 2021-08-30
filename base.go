package dbo

import (
	"context"
	"errors"
	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"strings"
	"time"

	"gitlab.badanamu.com.cn/calmisland/common-log/log"
)

type BaseDA struct{}

func (s BaseDA) Insert(ctx context.Context, value interface{}) (interface{}, error) {
	db, err := GetDB(ctx)
	if err != nil {
		return nil, err
	}

	return s.InsertTx(ctx, db, value)
}


func (s BaseDA) InsertTx(ctx context.Context, db *DBContext, value interface{}) (interface{}, error) {
	start := time.Now()
	err := db.WithContext(ctx).Create(value).Error
	if err != nil {
		me, ok := err.(*mysql.MySQLError)
		if ok && me.Number == 1062 {
			log.Error(ctx, "insert duplicate record",
				log.Err(me),
				log.String("tableName", db.getTableName(value)),
				log.Any("value", value),
				log.Duration("duration", time.Since(start)))
			return 0, ErrDuplicateRecord
		}

		log.Error(ctx, "insert failed",
			log.Err(err),
			log.String("tableName",  db.getTableName(value)),
			log.Any("value", value),
			log.Duration("duration", time.Since(start)))
		return nil, err
	}

	log.Debug(ctx, "insert success",
		log.String("tableName",  db.getTableName(value)),
		log.Any("value", value),
		log.Duration("duration", time.Since(start)))

	return value, nil
}

func (s BaseDA) InsertInBatches(ctx context.Context, value interface{},num int) (interface{}, error) {
	db, err := GetDB(ctx)
	if err != nil {
		return nil, err
	}

	return s.InsertInBatchesTx(ctx, db, value,num)
}


func (s BaseDA) InsertInBatchesTx(ctx context.Context, db *DBContext, value interface{},num int) (interface{}, error) {
	start := time.Now()
	err := db.WithContext(ctx).CreateInBatches(value,num).Error
	if err != nil {
		me, ok := err.(*mysql.MySQLError)
		if ok && me.Number == 1062 {
			log.Error(ctx, "insertBatches duplicate record",
				log.Err(me),
				log.String("tableName", db.getTableName(value)),
				log.Any("value", value),
				log.Duration("duration", time.Since(start)))
			return 0, ErrDuplicateRecord
		}

		log.Error(ctx, "insertBatches failed",
			log.Err(err),
			log.String("tableName",  db.getTableName(value)),
			log.Any("value", value),
			log.Duration("duration", time.Since(start)))
		return nil, err
	}

	log.Debug(ctx, "insertBatches success",
		log.String("tableName",  db.getTableName(value)),
		log.Any("value", value),
		log.Duration("duration", time.Since(start)))

	return value, nil
}

func (s BaseDA) Update(ctx context.Context, value interface{}) (int64, error) {
	db, err := GetDB(ctx)
	if err != nil {
		return 0, err
	}

	return s.UpdateTx(ctx, db, value)
}

func (s BaseDA) UpdateTx(ctx context.Context, db *DBContext, value interface{}) (int64, error) {
	start := time.Now()
	newDB := db.WithContext(ctx).Save(value)
	if newDB.Error != nil {

		me, ok := newDB.Error.(*mysql.MySQLError)
		if ok && me.Number == 1062 {
			log.Error(ctx, "update duplicate record",
				log.Err(me),
				log.String("tableName", db.getTableName(value)),
				log.Any("value", value),
				log.Duration("duration", time.Since(start)))
			return 0, ErrDuplicateRecord
		}

		log.Error(ctx, "update failed",
			log.Err(newDB.Error),
			log.String("tableName", db.getTableName(value)),
			log.Any("value", value),
			log.Duration("duration", time.Since(start)))
		return 0, newDB.Error
	}

	log.Debug(ctx, "update success",
		log.String("tableName", db.getTableName(value)),
		log.Any("value", value),
		log.Duration("duration", time.Since(start)))

	return newDB.RowsAffected, nil
}

func (s BaseDA) Save(ctx context.Context, value interface{}) error {
	db, err := GetDB(ctx)
	if err != nil {
		return err
	}

	return s.SaveTx(ctx, db, value)
}

func (s BaseDA) SaveTx(ctx context.Context, db *DBContext, value interface{}) error {
	start := time.Now()
	err := db.WithContext(ctx).Save(value).Error
	if err != nil {
		log.Error(ctx, "save failed",
			log.Err(err),
			log.String("tableName", db.getTableName(value)),
			log.Any("value", value),
			log.Duration("duration", time.Since(start)))
		return err
	}

	log.Debug(ctx, "save success",
		log.String("tableName", db.getTableName(value)),
		log.Any("value", value),
		log.Duration("duration", time.Since(start)))

	return nil
}

func (s BaseDA) Get(ctx context.Context, id interface{}, value interface{}) error {
	db, err := GetDB(ctx)
	if err != nil {
		return err
	}

	return s.GetTx(ctx, db, id, value)
}

func (s BaseDA) GetTx(ctx context.Context, db *DBContext, id interface{}, value interface{}) error {
	start := time.Now()
	err := db.WithContext(ctx).Where("id=?", id).First(value).Error
	if err == nil {
		log.Debug(ctx, "get by id success",
			log.Any("id", id),
			log.String("tableName", db.getTableName(value)),
			log.Any("value", value),
			log.Duration("duration", time.Since(start)))
		return nil
	}

	log.Error(ctx, "get by id failed",
		log.Err(err),
		log.Any("id", id),
		log.String("tableName", db.getTableName(value)),
		log.Any("value", value),
		log.Duration("duration", time.Since(start)))

	if errors.Is(err,gorm.ErrRecordNotFound) {
		return ErrRecordNotFound
	}

	return err
}

func (s BaseDA) Query(ctx context.Context, condition Conditions, values interface{}) error {
	db, err := GetDB(ctx)
	if err != nil {
		return err
	}

	return s.QueryTx(ctx, db, condition, values)
}

func (s BaseDA) QueryTx(ctx context.Context, db *DBContext, condition Conditions, values interface{}) error {
	wheres, parameters := condition.GetConditions()
	tx := db.WithContext(ctx)
	if len(wheres) > 0 {
		db.DB =tx.Where(strings.Join(wheres, " and "), parameters...)
	}

	orderBy := condition.GetOrderBy()
	if orderBy != "" {
		db.DB =tx.Order(orderBy)
	}

	pager := condition.GetPager()
	if pager != nil && pager.Enable() {
		// pagination
		offset, limit := pager.Offset()
		db.DB = tx.Offset(offset).Limit(limit)
	}

	start := time.Now()
	err := tx.Find(values).Error
	if err != nil {
		log.Error(ctx, "query values failed",
			log.Err(err),
			log.String("tableName", db.getTableName(values)),
			log.Any("condition", condition),
			log.Any("pager", pager),
			log.String("orderBy", orderBy),
			log.Duration("duration", time.Since(start)))
		return err
	}

	log.Debug(ctx, "query values success",
		log.String("tableName",db.getTableName(values)),
		log.Any("condition", condition),
		log.Any("pager", pager),
		log.String("orderBy", orderBy),
		log.Duration("duration", time.Since(start)))

	return nil
}

func (s BaseDA) Count(ctx context.Context, condition Conditions, values interface{}) (int, error) {
	db, err := GetDB(ctx)
	if err != nil {
		return 0, err
	}

	return s.CountTx(ctx, db, condition, values)
}

func (s BaseDA) CountTx(ctx context.Context, db *DBContext, condition Conditions, value interface{}) (int, error) {
	wheres, parameters := condition.GetConditions()
	tx := db.WithContext(ctx)
	if len(wheres) > 0 {
		db.DB = tx.Where(strings.Join(wheres, " and "), parameters...)
	}

	start := time.Now()
	var total int64
	tableName := db.getTableName(value)
	err := tx.Table(tableName).Count(&total).Error
	if err != nil {
		log.Error(ctx, "count failed",
			log.Err(err),
			log.String("tableName", tableName),
			log.Any("condition", condition),
			log.Duration("duration", time.Since(start)))
		return 0, err
	}

	log.Debug(ctx, "count success",
		log.String("tableName", tableName),
		log.Any("condition", condition),
		log.Duration("duration", time.Since(start)))

	return int(total), nil
}

func (s BaseDA) Page(ctx context.Context, condition Conditions, values interface{}) (int, error) {
	db, err := GetDB(ctx)
	if err != nil {
		return 0, err
	}

	return s.PageTx(ctx, db, condition, values)
}

func (s BaseDA) PageTx(ctx context.Context, db *DBContext, condition Conditions, values interface{}) (int, error) {
	total, err := s.CountTx(ctx, db, condition, values)
	if err != nil {
		return 0, err
	}

	err = s.QueryTx(ctx, db, condition, values)
	if err != nil {
		return 0, err
	}

	return total, nil
}
