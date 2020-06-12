package dbo

import (
	"context"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	log "gitlab.badanamu.com.cn/calmisland/common-cn/logger"
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
	err := db.Clone().Create(value).Error
	if err != nil {
		me, ok := err.(*mysql.MySQLError)
		if ok && me.Number == 1062 {
			log.WithError(err).
				WithContext(ctx).
				WithField("tableName", db.NewScope(value).TableName()).
				WithField("value", value).
				WithField("duration", time.Now().Sub(start).String()).
				Error("insert duplicate record")
			return 0, ErrDuplicateRecord
		}

		log.WithError(err).
			WithContext(ctx).
			WithField("tableName", db.NewScope(value).TableName()).
			WithField("value", value).
			WithField("duration", time.Now().Sub(start).String()).
			Error("insert failed")
		return nil, err
	}

	log.WithContext(ctx).
		WithField("tableName", db.NewScope(value).TableName()).
		WithField("value", value).
		WithField("duration", time.Now().Sub(start).String()).
		Debug("insert success")

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
	newDB := db.Clone().Save(value)
	if newDB.Error != nil {
		me, ok := newDB.Error.(*mysql.MySQLError)
		if ok && me.Number == 1062 {
			log.WithError(newDB.Error).
				WithContext(ctx).
				WithField("tableName", db.NewScope(value).TableName()).
				WithField("value", value).
				WithField("duration", time.Now().Sub(start).String()).
				Error("update duplicate record")
			return 0, ErrDuplicateRecord
		}

		log.WithError(newDB.Error).
			WithContext(ctx).
			WithField("tableName", db.NewScope(value).TableName()).
			WithField("value", value).
			WithField("duration", time.Now().Sub(start).String()).
			Error("update failed")
		return 0, newDB.Error
	}

	log.WithContext(ctx).
		WithField("tableName", db.NewScope(value).TableName()).
		WithField("value", value).
		WithField("duration", time.Now().Sub(start).String()).
		Debug("update success")

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
	err := db.Clone().Save(value).Error
	if err != nil {
		log.WithError(err).
			WithContext(ctx).
			WithField("tableName", db.NewScope(value).TableName()).
			WithField("value", value).
			WithField("duration", time.Now().Sub(start).String()).
			Error("save failed")
		return err
	}

	log.WithContext(ctx).
		WithField("tableName", db.NewScope(value).TableName()).
		WithField("value", value).
		WithField("duration", time.Now().Sub(start).String()).
		Debug("save success")

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
	err := db.Clone().Where("id=?", id).First(value).Error
	if err == nil {
		log.WithContext(ctx).
			WithField("tableName", db.NewScope(value).TableName()).
			WithField("id", id).
			WithField("value", value).
			WithField("duration", time.Now().Sub(start).String()).
			Debug("get by id success")
		return nil
	}

	log.WithError(err).
		WithContext(ctx).
		WithField("tableName", db.NewScope(value).TableName()).
		WithField("id", id).
		WithField("value", value).
		WithField("duration", time.Now().Sub(start).String()).
		Error("get by id failed")

	if gorm.IsRecordNotFoundError(err) {
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
	db = db.Clone()
	if len(wheres) > 0 {
		db.DB = db.Where(strings.Join(wheres, " and "), parameters...)
	}

	orderBy := condition.GetOrderBy()
	if orderBy != "" {
		db.DB = db.Order(orderBy)
	}

	pager := condition.GetPager()
	if pager != nil && pager.Enable() {
		// pagination
		offset, limit := pager.Offset()
		db.DB = db.Offset(offset).Limit(limit)
	}

	start := time.Now()
	err := db.Find(values).Error
	if err != nil {
		log.WithError(err).
			WithContext(ctx).
			WithField("tableName", db.NewScope(values).TableName()).
			WithField("condition", condition).
			WithField("pager", pager).
			WithField("orderBy", orderBy).
			Error("query values failed")
		return err
	}

	log.WithContext(ctx).
		WithField("tableName", db.NewScope(values).TableName()).
		WithField("condition", condition).
		WithField("pager", pager).
		WithField("orderBy", orderBy).
		WithField("duration", time.Now().Sub(start).String()).
		Debug("query values success")

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
	db = db.Clone()
	if len(wheres) > 0 {
		db.DB = db.Where(strings.Join(wheres, " and "), parameters...)
	}

	start := time.Now()
	var total int
	tableName := db.NewScope(value).TableName()
	err := db.Table(tableName).Count(&total).Error
	if err != nil {
		log.WithError(err).
			WithContext(ctx).
			WithField("tableName", tableName).
			WithField("condition", condition).
			Error("count failed")
		return 0, err
	}

	log.WithContext(ctx).
		WithField("tableName", tableName).
		WithField("condition", condition).
		WithField("duration", time.Now().Sub(start).String()).
		Debug("count success")

	return total, nil
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
	db = db.Clone()
	if err != nil {
		return 0, err
	}

	err = s.QueryTx(ctx, db, condition, values)
	if err != nil {
		return 0, err
	}

	return total, nil
}
