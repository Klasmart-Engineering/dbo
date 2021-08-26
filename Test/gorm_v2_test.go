package Test

import (
	"context"
	"fmt"
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
	"gitlab.badanamu.com.cn/calmisland/dbo"
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
	Pager   dbo.Pager
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

func (c *ClassConditions) GetPager() *dbo.Pager {
	return &c.Pager
}

func (c *ClassConditions) GetOrderBy() string {
	return ""
}

func initDB()  {
	dboHandler, err := dbo.NewWithConfig(func(c *dbo.Config) {
		c.ShowLog = true
		c.ShowSQL = true
		c.MaxIdleConns = 10
		c.MaxOpenConns = 10
		c.ConnectionString = "root:123456@tcp(127.0.0.1:3306)/ai_facerecognition?charset=utf8mb4&parseTime=True&loc=Local"
	})
	if err != nil {
		log.Error(context.TODO(), "create dbo failed", log.Err(err))
		panic(err)
	}
	dbo.ReplaceGlobal(dboHandler)
}

func TestMain(m *testing.M) {
	fmt.Println("begin test")
	initDB()
	m.Run()
	fmt.Println("end test")
}

func TestFind(t *testing.T) {
	ctx := context.Background()
	db:=dbo.MustGetDB(ctx)
	var class []Class
	err:=db.DB.Find(&class, "id in (1,2,3)").Error
	fmt.Println(err,class)
}

func TestInsert(t *testing.T) {
	ctx := context.Background()
	class:=Class{Name: "班级四"}
	_,err:=dbo.BaseDA{}.Insert(ctx,&class)
	fmt.Println(err,class)
}


func TestUpdate(t *testing.T) {
	ctx := context.Background()
	class:=Class{ID:31,Name: "班级三十"}
	_,err:=dbo.BaseDA{}.Update(ctx,&class)
	fmt.Println(err,class)
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	var class Class
	var id =31
	err:=dbo.BaseDA{}.Get(ctx,id,&class)
	fmt.Println(err,class)
}

func TestQuery(t *testing.T) {
	ctx := context.Background()
	var classes []Class
    //condition:=ClassConditions{ID: 1}
	condition:=ClassConditions{Name: "班级三十"}
	err:=dbo.BaseDA{}.Query(ctx,&condition,&classes)
	fmt.Println(err,classes)
}
func TestCount(t *testing.T) {
	ctx := context.Background()
	var class []Class
	//condition:=ClassConditions{ID: 1}
	condition:=ClassConditions{}
	count,err:=dbo.BaseDA{}.Count(ctx,&condition,&class)
	fmt.Println(err,count)
}
func TestPage(t *testing.T) {
	ctx := context.Background()
	var class []Class
	//condition:=ClassConditions{ID: 1}
	condition:=ClassConditions{Pager:dbo.Pager{Page:1,PageSize: 2}}
	count,err:=dbo.BaseDA{}.Page(ctx,&condition,&class)
	fmt.Println(err,count,class)
}

func TestTrans(t *testing.T) {
	ctx := context.Background()
	err:=dbo.GetTrans(ctx, func(ctx context.Context, tx *dbo.DBContext) error {
		db:=dbo.MustGetDB(ctx)
		classInsert:=Class{Name: "班级三十七"}
		_,errInsert:=dbo.BaseDA{}.InsertTx(ctx,db,&classInsert)
		fmt.Println(errInsert,classInsert)
        if errInsert!=nil{
        	return errInsert
		}


		var classGet Class
		var id =31
		errGet:=dbo.BaseDA{}.GetTx(ctx,db,id,&classGet)
		fmt.Println(errGet,classGet)
		if errGet!=nil{
			return errGet
		}

		class:=Class{ID:31,Name: "班级三十"}
		_,errUpdate:=dbo.BaseDA{}.Update(ctx,&class)
		fmt.Println(errUpdate,class)
		if errUpdate!=nil{
			return errUpdate
		}

		var classes []Class
		conditionClasses:=ClassConditions{Name: "班级三十"}
		errQuery:=dbo.BaseDA{}.QueryTx(ctx,db,&conditionClasses,&classes)
		fmt.Println(errQuery,classes)
		if errQuery!=nil{
			return errQuery
		}



		var classCount []Class
		conditionClassCount:=ClassConditions{}
		count,errCount:=dbo.BaseDA{}.Count(ctx,&conditionClassCount,&classCount)
		fmt.Println(errCount,count)
		if errCount!=nil{
			return errCount
		}



		var classPage []Class
		//condition:=ClassConditions{ID: 1}
		conditionPage:=ClassConditions{Pager:dbo.Pager{Page:1,PageSize: 2}}
		countPage,errPage:=dbo.BaseDA{}.Page(ctx,&conditionPage,&classPage)
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