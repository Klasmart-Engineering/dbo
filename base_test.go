package dbo
import (
	"context"
	"fmt"
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
	"gorm.io/gorm/logger"
	"testing"
)
type Class struct {
	ID        uint `gorm:"column:id;primaryKey;autoIncrement:true;autoIncrementIncrement:1"`
	Name    string `gorm:"column:name;type:varchar(64);" json:"name"`


}
func (Class) TableName() string {
	return "class"
}

type ClassConditions struct {
	ID int
	Name string
	Pager   Pager
}

func (c *ClassConditions) GetConditions() ([]string, []interface{}) {
	var wheres []string
	var params []interface{}
	if c.ID >0 {
		wheres=append(wheres,"id= ?")
		params=append(params,c.ID)
	}
	if len(c.Name) >0{
		wheres=append(wheres,"name= ?")
		params=append(params,c.Name)
	}
	return wheres, params
}

func (c *ClassConditions) GetPager() *Pager {
	return &c.Pager
}

func (c *ClassConditions) GetOrderBy() string {
	return ""
}

func initDB()  {
	dboHandler, err := NewWithConfig(func(c *Config) {
		c.ShowLog = true
		c.ShowSQL = true
		c.MaxIdleConns = 10
		c.MaxOpenConns = 10
		c.ConnectionString = "root:123456@tcp(127.0.0.1:3306)/ai_facerecognition?charset=utf8mb4&parseTime=True&loc=Local"
		c.LogLevel=logger.Info
	})
	if err != nil {
		log.Error(context.TODO(), "create dbo failed", log.Err(err))
		panic(err)
	}
	ReplaceGlobal(dboHandler)
}

func TestMain(m *testing.M) {
	fmt.Println("begin test")
	initDB()
	m.Run()
	fmt.Println("end test")
}

func TestFind(t *testing.T) {
	ctx := context.Background()
	db:=MustGetDB(ctx)
	var class []Class
	err:=db.DB.Find(&class, "id in (1,2,3)").Error
	fmt.Println(err,class)
}

func TestInsertSingle(t *testing.T) {
	ctx := context.Background()
	class:=Class{Name: "班级四"}
	_,err:=BaseDA{}.Insert(ctx,&class)
	fmt.Println(err,class)
}
func TestInsertMany(t *testing.T) {
	ctx := context.Background()
	var classes []Class
	classes=append(classes, Class{Name: "班级101"})
	classes=append(classes, Class{Name: "班级102"})
	_,err:=BaseDA{}.Insert(ctx,&classes)
	fmt.Println(err,classes)
}

func TestInsertBatches(t *testing.T)  {
	ctx := context.Background()
	var classes []Class
	classes=append(classes, Class{Name: "班级110"})
	classes=append(classes, Class{Name: "班级111"})
	classes=append(classes, Class{Name: "班级112"})
	classes=append(classes, Class{Name: "班级113"})
	_,err:=BaseDA{}.InsertInBatches(ctx,&classes,3)
	fmt.Println(err,classes)
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	class:=Class{ID:31,Name: "班级三十"}
	_,err:=BaseDA{}.Update(ctx,&class)
	fmt.Println(err,class)
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	var class Class
	var id =31
	err:=BaseDA{}.Get(ctx,id,&class)
	fmt.Println(err,class)
}

func TestQuery(t *testing.T) {
	ctx := context.Background()
	cty:=context.WithValue(ctx,"test","test1111")
	var classes []Class
	//condition:=ClassConditions{ID: 1}
	condition:=ClassConditions{Name: "班级三十"}
	err:=BaseDA{}.Query(cty,&condition,&classes)
	fmt.Println(err,classes)
}
func TestCount(t *testing.T) {
	ctx := context.Background()
	var class []Class
	//condition:=ClassConditions{ID: 1}
	condition:=ClassConditions{}
	count,err:=BaseDA{}.Count(ctx,&condition,&class)
	fmt.Println(err,count)
}
func TestPage(t *testing.T) {
	ctx := context.Background()
	var class []Class
	//condition:=ClassConditions{ID: 1}
	condition:=ClassConditions{Pager:Pager{Page:1,PageSize: 2}}
	count,err:=BaseDA{}.Page(ctx,&condition,&class)
	fmt.Println(err,count,class)
}

func TestTrans(t *testing.T) {
	ctx := context.Background()
	err:=GetTrans(ctx, func(ctx context.Context, tx *DBContext) error {
		db:=MustGetDB(ctx)
		classInsertSingle:=Class{Name: "班级三十七"}
		_,errInsertSingle:=BaseDA{}.InsertTx(ctx,db,&classInsertSingle)
		fmt.Println(errInsertSingle,classInsertSingle)
		if errInsertSingle!=nil{
			return errInsertSingle
		}

		classInsertMany:=[]Class{{Name: "班级三十七"},{Name: "班级三十八"}}
		_,errInsertMany:=BaseDA{}.InsertTx(ctx,db,&classInsertMany)
		fmt.Println(errInsertMany,classInsertMany)
		if errInsertMany!=nil{
			return errInsertMany
		}
		classInsertInBatches:=[]Class{{Name: "班级三十七"},{Name: "班级三十八"},{Name: "班级三十九"}}
		_,errInsertInBatches:=BaseDA{}.InsertInBatchesTx(ctx,db,&classInsertInBatches,2)
		fmt.Println(errInsertInBatches,classInsertInBatches)
		if errInsertInBatches!=nil{
			return errInsertInBatches
		}



		var classGet Class
		var id =31
		errGet:=BaseDA{}.GetTx(ctx,db,id,&classGet)
		fmt.Println(errGet,classGet)
		if errGet!=nil{
			return errGet
		}

		class:=Class{ID:31,Name: "班级三十"}
		_,errUpdate:=BaseDA{}.Update(ctx,&class)
		fmt.Println(errUpdate,class)
		if errUpdate!=nil{
			return errUpdate
		}

		var classes []Class
		conditionClasses:=ClassConditions{Name: "班级三十"}
		errQuery:=BaseDA{}.QueryTx(ctx,db,&conditionClasses,&classes)
		fmt.Println(errQuery,classes)
		if errQuery!=nil{
			return errQuery
		}

		var classCount Class
		conditionClassCount:=ClassConditions{}
		count,errCount:=BaseDA{}.Count(ctx,&conditionClassCount,classCount)
		fmt.Println(errCount,count)
		if errCount!=nil{
			return errCount
		}

		var classPage []Class
		conditionPage:=ClassConditions{Pager:Pager{Page:1,PageSize: 2}}
		countPage,errPage:=BaseDA{}.Page(ctx,&conditionPage,&classPage)
		fmt.Println(errPage,countPage,classPage)
		if errPage!=nil{
			return errPage
		}
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}
}
