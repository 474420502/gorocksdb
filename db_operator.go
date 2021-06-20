package rocks

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"sync/atomic"
)

// Operator easy to operate rocksdb
type Operator struct {
	db   *DB
	opts *Options

	ropt *ReadOptions
	wopt *WriteOptions

	cfhs map[string]*ColumnFamilyHandle

	isDestory int32
}

// OpenDbColumnFamiliesEx Auto destory with call runtime.SetFinalizer.
// Operator must be Referenced before OperatorColumnFamily finish all the operate
func OpenDbColumnFamiliesEx(opts *Options, name string) (*Operator, error) {

	cfnames, err := ListColumnFamilies(opts, name)
	if err != nil {
		ndb, err := OpenDb(opts, name)
		if err != nil {
			return nil, err
		}
		ndb.Close()
		cfnames, err = ListColumnFamilies(opts, name)
		if err != nil {
			return nil, err
		}
	}

	var cfOpts []*Options
	for range cfnames {
		cfOpts = append(cfOpts, opts)
	}

	db, cfs, err := OpenDbColumnFamilies(opts, name, cfnames, cfOpts)
	if err != nil {
		return nil, err
	}

	op := &Operator{
		db:   db,
		opts: opts,
		cfhs: make(map[string]*ColumnFamilyHandle),
	}
	for i, name := range cfnames {
		op.cfhs[name] = cfs[i]
	}

	runtime.SetFinalizer(op, func(op *Operator) {
		op.Destory()
	})
	return op, nil
}

// GetSelf get object  *DB *ColumnFamilyHandle
func (op *Operator) GetSelf() (*DB, map[string]*ColumnFamilyHandle) {
	return op.db, op.cfhs
}

// GetColumnFamilyHandles get the names of ColumnFamilyHandles
func (op *Operator) GetColumnFamilyHandles() []string {
	var result []string
	for key := range op.cfhs {
		result = append(result, key)
	}

	return result
}

// GetProperty returns the value of a database property.
func (op *Operator) GetProperty(propName string) string {
	return op.db.GetProperty(propName)
}

// SetDefaultWriteOptions set deufalt WriteOptions
func (op *Operator) SetDefaultWriteOptions(wopt *WriteOptions) {
	if op.wopt != nil {
		op.wopt.Destroy()
	}
	op.wopt = wopt
}

// SetDefaultReadOptions set deufalt ReadOptions
func (op *Operator) SetDefaultReadOptions(ropt *ReadOptions) {
	if op.ropt != nil {
		op.ropt.Destroy()
	}
	op.ropt = ropt
}

// ColumnFamily return a OperatorColumnFamily
func (op *Operator) ColumnFamily(name string) *OperatorColumnFamily {
	if cfh, ok := op.cfhs[name]; ok {
		if op.wopt == nil {
			op.wopt = NewDefaultWriteOptions()
		}

		if op.ropt == nil {
			op.ropt = NewDefaultReadOptions()
		}

		return &OperatorColumnFamily{
			operator: op,
			cfh:      cfh,
		}
	}
	return nil
}

// ColumnFamilyMissCreate return a OperatorColumnFamily, if miss ColumnFamily, will create ColumnFamily
func (op *Operator) ColumnFamilyMissCreate(name string) *OperatorColumnFamily {
	var cfh *ColumnFamilyHandle
	var ok bool

	// not safe. multi goroutine is can error
	if cfh, ok = op.cfhs[name]; !ok {
		var err error
		cfh, err = op.db.CreateColumnFamily(op.opts, name)
		if err != nil {
			panic(err)
		}
		op.cfhs[name] = cfh
	}

	if op.wopt == nil {
		op.wopt = NewDefaultWriteOptions()
	}

	if op.ropt == nil {
		op.ropt = NewDefaultReadOptions()
	}

	return &OperatorColumnFamily{
		operator: op,
		cfh:      cfh,
	}
}

// CreateColumnFamily create a ColumnFamily
func (op *Operator) CreateColumnFamily(name string) error {
	cf, err := op.db.CreateColumnFamily(op.opts, name)
	if err != nil {
		return err
	}
	op.cfhs[name] = cf
	return nil
}

// Destory  destory  the objects in operator
func (op *Operator) Destory() {
	if atomic.CompareAndSwapInt32(&op.isDestory, 0, 1) {
		if op.cfhs != nil {
			for _, cfh := range op.cfhs {
				cfh.Destroy()
			}
		}

		if op.ropt != nil {
			op.ropt.Destroy()
		}
		if op.wopt != nil {
			op.wopt.Destroy()
		}
		if op.db != nil {
			op.db.Close()
		}
	}
}

// OperatorColumnFamily easy to operate the ColumnFamily of  rocksdb
type OperatorColumnFamily struct {
	operator *Operator
	cfh      *ColumnFamilyHandle
}

// Put
func (opcf *OperatorColumnFamily) Put(key, value []byte) error {
	return opcf.operator.db.PutCF(opcf.operator.wopt, opcf.cfh, key, value)
}

// PutObject
func (opcf *OperatorColumnFamily) PutObject(key []byte, value interface{}) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(value)
	if err != nil {
		return err
	}
	return opcf.operator.db.PutCF(opcf.operator.wopt, opcf.cfh, key, buf.Bytes())
}

// GetObject is safe
func (opcf *OperatorColumnFamily) GetObject(key []byte, value interface{}) error {
	s, err := opcf.operator.db.GetCF(opcf.operator.ropt, opcf.cfh, key)
	if err != nil {
		return err
	}
	defer s.Free()
	if s.Exists() {
		return gob.NewDecoder(bytes.NewReader(s.Data())).Decode(value)
	}
	return nil
}

// MultiGetObject is safe
func (opcf *OperatorColumnFamily) MultiGetObject(value interface{}, key ...[]byte) error {
	rtype := reflect.TypeOf(value)
	if rtype.Kind() != reflect.Slice {
		return fmt.Errorf("value must be the type of slice")
	}
	rtype = rtype.Elem()
	if rtype.Kind() == reflect.Ptr {
		rtype = rtype.Elem()

		rvalue := reflect.ValueOf(value)
		zero := reflect.Zero(rtype)

		ss, err := opcf.operator.db.MultiGetCF(opcf.operator.ropt, opcf.cfh, key...)
		if err != nil {
			return nil
		}
		defer ss.Destroy()
		for _, s := range ss {
			if s.Exists() {
				item := reflect.New(rtype)
				err = gob.NewDecoder(bytes.NewReader(s.Data())).DecodeValue(item)
				if err != nil {
					return err
				}
				rvalue = reflect.Append(rvalue, item.Addr())
			} else {
				rvalue = reflect.Append(rvalue, zero)
			}
		}
	} else {
		rvalue := reflect.ValueOf(value)
		zero := reflect.Zero(rtype)

		ss, err := opcf.operator.db.MultiGetCF(opcf.operator.ropt, opcf.cfh, key...)
		if err != nil {
			return nil
		}
		defer ss.Destroy()
		for _, s := range ss {
			if s.Exists() {
				item := reflect.New(rtype)
				err = gob.NewDecoder(bytes.NewReader(s.Data())).DecodeValue(item)
				if err != nil {
					return err
				}
				rvalue = reflect.Append(rvalue, item)
			} else {
				rvalue = reflect.Append(rvalue, zero)
			}
		}
	}
	return nil
}

// Delete
func (opcf *OperatorColumnFamily) Delete(key []byte) error {
	return opcf.operator.db.DeleteCF(opcf.operator.wopt, opcf.cfh, key)
}

// Get Slice is need free
func (opcf *OperatorColumnFamily) KeySize() uint64 {
	size, err := strconv.ParseUint(opcf.GetProperty("rocksdb.estimate-num-keys"), 10, 64)
	if err != nil {
		panic(err)
	}
	return size
}

// Get Slice is need free
func (opcf *OperatorColumnFamily) Get(key []byte) (*Slice, error) {
	return opcf.operator.db.GetCF(opcf.operator.ropt, opcf.cfh, key)
}

// MultiGet Slices is need Destory
func (opcf *OperatorColumnFamily) MultiGet(key ...[]byte) (Slices, error) {
	return opcf.operator.db.MultiGetCF(opcf.operator.ropt, opcf.cfh, key...)
}

// GetSafe not need to free object
func (opcf *OperatorColumnFamily) GetSafe(key []byte, do func(value []byte)) error {
	s, err := opcf.operator.db.GetCF(opcf.operator.ropt, opcf.cfh, key)
	if err != nil {
		return err
	}
	defer s.Free()
	do(s.Data())
	return nil
}

// Get Slices is not needed to free object
func (opcf *OperatorColumnFamily) MultiGetSafe(do func(i int, value []byte) bool, key ...[]byte) error {
	ss, err := opcf.operator.db.MultiGetCF(opcf.operator.ropt, opcf.cfh, key...)
	if err != nil {
		return err
	}
	defer ss.Destroy()
	for i, s := range ss {
		if !do(i, s.Data()) {
			break
		}
	}
	return nil
}

// WriteBatch  批量Put(安全释放内存)
func (opcf *OperatorColumnFamily) WriteBatch(do func(batch *WriteBatchColumnFamily)) error {
	batch := newWriteBatchColumnFamily(opcf)
	defer batch.wb.Destroy()
	do(batch)
	err := opcf.operator.db.Write(opcf.operator.wopt, batch.wb)
	if err != nil {
		return err
	}
	return err
}

// NewIterator returns an Iterator over the the database and column family that uses the ReadOptions.
// Warning! iterator must be closed.
func (opcf *OperatorColumnFamily) NewIterator() *OperatorIterator {
	return &OperatorIterator{iter: opcf.operator.db.NewIteratorCF(opcf.operator.ropt, opcf.cfh)}
}

// NewIterator returns an Iterator over the the database and column family that uses the ReadOptions
func (opcf *OperatorColumnFamily) IteratorSafe(do func(iter *OperatorIteratorSafe)) {
	opiter := &OperatorIteratorSafe{iter: opcf.operator.db.NewIteratorCF(opcf.operator.ropt, opcf.cfh)}
	defer opiter.close()
	do(opiter)
}

// GetProperty returns the value of a database property.
func (opcf *OperatorColumnFamily) GetProperty(propName string) string {
	return opcf.operator.db.GetPropertyCF(propName, opcf.cfh)
}

type WriteBatchColumnFamily struct {
	wb *WriteBatch
	op *OperatorColumnFamily
}

func newWriteBatchColumnFamily(opcf *OperatorColumnFamily) *WriteBatchColumnFamily {
	return &WriteBatchColumnFamily{
		wb: NewWriteBatch(),
		op: opcf,
	}
}

// Put
func (wbcf *WriteBatchColumnFamily) Put(key, value []byte) {
	wbcf.wb.PutCF(wbcf.op.cfh, key, value)
}

// Merge   queues a merge of "value" with the existing value of "key" in a column family.
func (wbcf *WriteBatchColumnFamily) Merge(key, value []byte) {
	wbcf.wb.MergeCF(wbcf.op.cfh, key, value)
}

// Data  returns the serialized version of this batch
func (wbcf *WriteBatchColumnFamily) Data() []byte {
	return wbcf.wb.Data()
}

// Count returns the number of updates in the batch.
func (wbcf *WriteBatchColumnFamily) Count() int {
	return wbcf.wb.Count()
}

// Clear removes all the enqueued Put and Deletes.
func (wbcf *WriteBatchColumnFamily) Clear() {
	wbcf.wb.Clear()
}

// NewIterator returns a iterator to iterate over the records in the batch.
func (wbcf *WriteBatchColumnFamily) NewIterator() *WriteBatchIterator {
	return wbcf.wb.NewIterator()
}

// PutObject
func (wbcf *WriteBatchColumnFamily) PutObject(key []byte, value interface{}) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(value)
	if err != nil {
		return err
	}
	wbcf.wb.PutCF(wbcf.op.cfh, key, buf.Bytes())
	return nil
}

func (wbcf *WriteBatchColumnFamily) DeleteRange(start, end []byte) {
	wbcf.wb.DeleteRangeCF(wbcf.op.cfh, start, end)
}
