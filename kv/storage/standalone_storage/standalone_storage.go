package standalone_storage

import (
	_ "fmt"

	. "github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	. "github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db   *DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opt := DefaultOptions
	opt.Dir = conf.DBPath
	opt.ValueDir = conf.DBPath
	db, err := Open(opt)
	if err != nil {
		return nil
	}
	return &StandAloneStorage{db, conf}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []Modify) error {
	if len(batch) == 0 {
		return nil
	}

	writeBatch := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch m.Data.(type) {
		case Put:
			writeBatch.SetCF(m.Cf(), m.Key(), m.Data.(Put).Value)
		case Delete:
			writeBatch.DeleteCF(m.Cf(), m.Key())
		}
	}
	err := writeBatch.WriteToDB(s.db)
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (StorageReader, error) {
	return &SimpleStorageReader{s.db}, nil
}

type SimpleStorageReader struct {
	db *DB
}

func (self *SimpleStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	v, err := engine_util.GetCF(self.db, cf, key)
	if err == ErrKeyNotFound {
		return nil, nil
	}
	return v, err
}

func (self *SimpleStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := self.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

func (self *SimpleStorageReader) Close() {

}
