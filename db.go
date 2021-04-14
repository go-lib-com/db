package db

import (
    "reflect"
	_ "github.com/godror/godror"
	"github.com/jmoiron/sqlx"
	"github.com/JulioVecino/logger"
)

type Connection struct {
    Db   *sqlx.DB
    InTx bool
    Tx   *sqlx.Tx
}

func NewConnection(strConn string, transaction bool) (*Connection, error) {
    db, err := sqlx.Connect("godror", strConn)
    if err != nil {
        return nil, err
    }
    var tx *sqlx.Tx
    inTx := false
    if transaction {
        inTx = true
        tx = db.MustBegin()
    }
    return &Connection{
        Db: db,
        InTx: inTx,
        Tx: tx,
    }, nil
}

func (c *Connection) Exec(query string, arg ...interface{}) interface{} {
    if c.InTx {
       return c.Tx.MustExec(query, arg...)
    }
   return c.Db.MustExec(query, arg...)
}

func (c *Connection) Close() {
    if c.InTx {
        c.InTx = false;
        c.Tx.Commit()
    }
    c.Db.Close()
}

func (c *Connection) Error (err error) error {
    if c.InTx {
        c.InTx = false;
        c.Tx.Rollback()
    }
    return err
}

func (c *Connection) Select(obj interface {}, query string, arg ...interface{}) error {
   tp := reflect.ValueOf(obj)
   if tp.Elem().Kind() == reflect.Struct {
      logger.Info("->",query)
      logger.Json("OBJ GET", obj)
      return c.Db.Get(obj, query, arg...)
   }
   return c.Db.Select(obj, query, arg...)
}


