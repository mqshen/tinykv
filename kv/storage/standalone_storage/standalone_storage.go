package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
)

type badgerReader struct {
	txn *badger.Txn
}

func (br *badgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(br.txn, cf, key)
	if err != nil && errors.IsNotFound(err) {
		return nil, nil
	}
	return value, err
}

func (br *badgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, br.txn)
}

func (br *badgerReader) Close() {
	br.txn.Commit()
}
// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	opts badger.Options
	db *badger.DB
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath

	// Your Code Here (1).
	return &StandAloneStorage{
		opts: opts,
	}
}

func (s *StandAloneStorage) Start() error {
	db, err := badger.Open(s.opts)
	if err != nil {
		log.Fatal(err)
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &badgerReader{
		txn: s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	for _, modify := range batch {
		var err error = nil
		switch modify.Data.(type) {
		case storage.Put:
			err = txn.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value())
		case storage.Delete:
			err = txn.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key()))
		}
		if err != nil {
			txn.Discard()
			return err
		}
	}
	return txn.Commit()
}
