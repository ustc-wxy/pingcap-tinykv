package engine_util

import (
	"github.com/Connor1996/badger"
	"github.com/jmhodges/levigo"
)

type CFItem struct {
	item      *badger.Item
	prefixLen int
}

// String returns a string representation of Item
func (i *CFItem) String() string {
	return i.item.String()
}

func (i *CFItem) Key() []byte {
	return i.item.Key()[i.prefixLen:]
}

func (i *CFItem) KeyCopy(dst []byte) []byte {
	return i.item.KeyCopy(dst)[i.prefixLen:]
}

func (i *CFItem) Version() uint64 {
	return i.item.Version()
}

func (i *CFItem) IsEmpty() bool {
	return i.item.IsEmpty()
}

func (i *CFItem) Value() ([]byte, error) {
	return i.item.Value()
}

func (i *CFItem) ValueSize() int {
	return i.item.ValueSize()
}

func (i *CFItem) ValueCopy(dst []byte) ([]byte, error) {
	return i.item.ValueCopy(dst)
}

func (i *CFItem) IsDeleted() bool {
	return i.item.IsDeleted()
}

func (i *CFItem) EstimatedSize() int64 {
	return i.item.EstimatedSize()
}

func (i *CFItem) UserMeta() []byte {
	return i.item.UserMeta()
}

type BadgerIterator struct {
	iter   *badger.Iterator
	prefix string
}

func NewCFIterator(cf string, txn *badger.Txn) *BadgerIterator {
	return &BadgerIterator{
		iter:   txn.NewIterator(badger.DefaultIteratorOptions),
		prefix: cf + "_",
	}
}

func (it *BadgerIterator) Item() DBItem {
	return &CFItem{
		item:      it.iter.Item(),
		prefixLen: len(it.prefix),
	}
}

func (it *BadgerIterator) Valid() bool { return it.iter.ValidForPrefix([]byte(it.prefix)) }

func (it *BadgerIterator) ValidForPrefix(prefix []byte) bool {
	return it.iter.ValidForPrefix(append([]byte(it.prefix), prefix...))
}

func (it *BadgerIterator) Close() {
	it.iter.Close()
}

func (it *BadgerIterator) Next() {
	it.iter.Next()
}

func (it *BadgerIterator) Seek(key []byte) {
	it.iter.Seek(append([]byte(it.prefix), key...))
}

func (it *BadgerIterator) Rewind() {
	it.iter.Rewind()
}

type DBIterator interface {
	// Item returns pointer to the current key-value pair.
	Item() DBItem
	// Valid returns false when iteration is done.
	Valid() bool
	// Next would advance the iterator by one. Always check it.Valid() after a Next()
	// to ensure you have access to a valid it.Item().
	Next()
	// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
	// greater than provided.
	Seek([]byte)

	// Close the iterator
	Close()
}

type DBItem interface {
	// Key returns the key.
	Key() []byte
	// KeyCopy returns a copy of the key of the item, writing it to dst slice.
	// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
	// returned.
	KeyCopy(dst []byte) []byte
	// Value retrieves the value of the item.
	Value() ([]byte, error)
	// ValueSize returns the size of the value.
	ValueSize() int
	// ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice.
	// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
	// returned.
	ValueCopy(dst []byte) ([]byte, error)
}

type LevelDBIterator struct {
	iter      *levigo.Iterator
	prefix    string
	prefixlen int
}

func NewLevelDBIterator(cf string, db *levigo.DB) *LevelDBIterator {
	prefix := cf + "_"
	return &LevelDBIterator{
		iter:      db.NewIterator(levigo.NewReadOptions()),
		prefix:    prefix,
		prefixlen: len(prefix),
	}
}

func (i *LevelDBIterator) Value() []byte {
	return i.iter.Value()
}

func (i *LevelDBIterator) Valid() bool {
	return i.iter.Valid()
}

func (i *LevelDBIterator) Next() {
	i.iter.Next()
}

func (i *LevelDBIterator) Prev() {
	i.iter.Prev()
}

func (i *LevelDBIterator) Key() []byte {
	return i.iter.Key()[i.prefixlen:]
}

func (i *LevelDBIterator) Seek(key []byte) {
	i.iter.Seek(key)
}

func (i *LevelDBIterator) SeekToFirst() {
	i.iter.SeekToFirst()
}

func (i *LevelDBIterator) SeekToLast() {
	i.iter.SeekToLast()
}

func (i *LevelDBIterator) Error() {
	i.iter.GetError()
}

func (i *LevelDBIterator) Close() {
	i.iter.Close()
}

type DBIter interface {
	// Key returns the value of iter.
	Key() []byte
	// Value returns the value of iter.
	Value() []byte
	// Valid returns false when iteration is done.
	Valid() bool
	// Prev moves the iterator to the previous sequential key in the database.
	// Always check it.Valid() after a Next() to ensure you have access to a valid iter.
	Prev()
	// Next moves the iterator to the next sequential key in the database.
	// Always check it.Valid() after a Next() to ensure you have access to a valid iter.
	Next()
	// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
	// greater than provided.
	Seek([]byte)
	// Close the iterator
	Close()
}
