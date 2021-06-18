package rocks

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type OperatorIterator struct {
	iter *Iterator
}

var ErrKeyNotExists = fmt.Errorf("key not exists")
var ErrValueNotExists = fmt.Errorf("key not exists")

func (opiter *OperatorIterator) KeyObject(key interface{}) error {
	k := opiter.iter.Key()
	defer k.Free()
	if k.Exists() {
		return gob.NewDecoder(bytes.NewReader(k.Data())).Decode(key)
	}
	return ErrKeyNotExists
}

func (opiter *OperatorIterator) ValueObject(value interface{}) error {
	k := opiter.iter.Value()
	defer k.Free()
	if k.Exists() {
		return gob.NewDecoder(bytes.NewReader(k.Data())).Decode(value)
	}
	return ErrValueNotExists
}

func (opiter *OperatorIterator) SeekObject(key interface{}) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(key)
	if err != nil {
		panic(err)
	}
	opiter.iter.Seek(buf.Bytes())
}

func (opiter *OperatorIterator) SeekObjectForPrev(key interface{}) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(key)
	if err != nil {
		panic(err)
	}
	opiter.iter.SeekForPrev(buf.Bytes())
}

func (opiter *OperatorIterator) ValidObjectForPrefix(prefix interface{}) bool {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(prefix)
	if err != nil {
		panic(err)
	}
	return opiter.iter.ValidForPrefix(buf.Bytes())
}

func (opiter *OperatorIterator) Valid() bool {
	return opiter.iter.Valid()
}
func (opiter *OperatorIterator) ValidForPrefix(prefix []byte) bool {
	return opiter.iter.ValidForPrefix(prefix)
}
func (opiter *OperatorIterator) Key() *Slice {
	return opiter.iter.Key()
}
func (opiter *OperatorIterator) Value() *Slice {
	return opiter.iter.Value()
}
func (opiter *OperatorIterator) Next() {
	opiter.iter.Next()
}
func (opiter *OperatorIterator) Prev() {
	opiter.iter.Prev()
}
func (opiter *OperatorIterator) SeekToFirst() {
	opiter.iter.SeekToFirst()
}
func (opiter *OperatorIterator) SeekToLast() {
	opiter.iter.SeekToLast()
}
func (opiter *OperatorIterator) Seek(key []byte) {
	opiter.iter.Seek(key)
}
func (opiter *OperatorIterator) SeekForPrev(key []byte) {
	opiter.iter.SeekForPrev(key)
}
func (opiter *OperatorIterator) Err() error {
	return opiter.iter.Err()
}
func (opiter *OperatorIterator) Close() {
	opiter.iter.Close()
}

type OperatorIteratorSafe struct {
	iter *Iterator
}

func (opiter *OperatorIteratorSafe) KeyObject(key interface{}) error {
	k := opiter.iter.Key()
	defer k.Free()
	if k.Exists() {
		return gob.NewDecoder(bytes.NewReader(k.Data())).Decode(key)
	}
	return ErrKeyNotExists
}

func (opiter *OperatorIteratorSafe) ValueObject(value interface{}) error {
	k := opiter.iter.Value()
	defer k.Free()
	if k.Exists() {
		return gob.NewDecoder(bytes.NewReader(k.Data())).Decode(value)
	}
	return ErrValueNotExists
}

func (opiter *OperatorIteratorSafe) SeekObject(key interface{}) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(key)
	if err != nil {
		panic(err)
	}
	opiter.iter.Seek(buf.Bytes())
}

func (opiter *OperatorIteratorSafe) SeekObjectForPrev(key interface{}) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(key)
	if err != nil {
		panic(err)
	}
	opiter.iter.SeekForPrev(buf.Bytes())
}

func (opiter *OperatorIteratorSafe) ValidObjectForPrefix(prefix interface{}) bool {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(prefix)
	if err != nil {
		panic(err)
	}
	return opiter.iter.ValidForPrefix(buf.Bytes())
}

func (opiter *OperatorIteratorSafe) Valid() bool {
	return opiter.iter.Valid()
}

func (opiter *OperatorIteratorSafe) ValidForPrefix(prefix []byte) bool {
	return opiter.iter.ValidForPrefix(prefix)
}

func (opiter *OperatorIteratorSafe) Key() []byte {
	var buf []byte
	k := opiter.iter.Key()
	copy(buf, k.Data())
	k.Free()
	return buf
}

func (opiter *OperatorIteratorSafe) Value() []byte {
	var buf []byte
	k := opiter.iter.Value()
	copy(buf, k.Data())
	k.Free()
	return buf
}

func (opiter *OperatorIteratorSafe) Next() {
	opiter.iter.Next()
}
func (opiter *OperatorIteratorSafe) Prev() {
	opiter.iter.Prev()
}
func (opiter *OperatorIteratorSafe) SeekToFirst() {
	opiter.iter.SeekToFirst()
}
func (opiter *OperatorIteratorSafe) SeekToLast() {
	opiter.iter.SeekToLast()
}
func (opiter *OperatorIteratorSafe) Seek(key []byte) {
	opiter.iter.Seek(key)
}
func (opiter *OperatorIteratorSafe) SeekForPrev(key []byte) {
	opiter.iter.SeekForPrev(key)
}

func (opiter *OperatorIteratorSafe) Err() error {
	return opiter.iter.Err()
}

func (opiter *OperatorIteratorSafe) close() {
	opiter.iter.Close()
}
