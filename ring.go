// Package consistenthashing - консистентное хеширование.
//
// Что делает:
// Consistent hashing — это способ распределять ключи по нескольким серверам так, чтобы
// при добавлении/удалении сервера перераспределялась только небольшая часть ключей, а не почти все.
// То есть это не KV-хранилище. Это routing/partitioning structure: она определяет, какому узлу принадлежит ключ.
//
// Используется:
// Когда нужно распределять записи по нескольким хранилищам и быть готовым к тому что хранилища могут добавляться или удаляться.
// Данная структура не хранит kev+value, а просто говорит в какую node попадает value.
//
// Идея:
// В отличие от распределения на основе обычного хеширования (hash mod N), здесь при добавлении или удалении
// перераспределяется небольшая часть ключей.
// Достигается это так: для каждой ноды считается хеш и размещается на круге, далее вычисляется хеш ключа и в качестве
// ноды выбирается первая чей хеш больше (то есть идем по часовой стрелке от key hash).
// Здесь представлен улучшенный алгоритм: введены виртуальных узлы (virtual nodes), это дает:
// - равномерного распределения ключей из-за большего количества узлов
// - количеством виртуальных узлов можно регулировать вес настоящего узла
//
// Реализация:
// - hashFn - принимаем снаружи, потому что нет встроенного представление comparable в []byte (не даёт способа без reflection
// узнать структуру произвольного K и превратить его в канонические байты). Как варианты: 1) fmt.Sprintf в строку потом
// в байти внутри рефлексия и аллокации 2) reflection - самостоятельно 3) type switch - то список типов жестко ограничен
// 4) hash функцию передает клиент
// Здесь нет кода, который перераспределяет ключи. Это делает клиент сам, после добавления/удаления ноды.
//
// Альтернатива: rendezvous hashing
package consistenthashing

import (
	"errors"
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/exp/slices"
)

var (
	ErrNodeAlreadyExists = errors.New("node already exists")
	ErrNodeNotFound      = errors.New("node not found")
	ErrInvalidWeight     = errors.New("invalid weight")
	ErrRingEmpty         = errors.New("ring is empty")
)

type Ring[T any] struct {
	mu     sync.RWMutex
	hasher Hasher[T]
	// map вместо слайса указателей для O(1) lookup, в обоих случаях указатели будут стабильны
	nodes  map[string]*node
	vnodes []vnode
}

type Hasher[T any] interface {
	Hash(key T) uint64
}

type node struct {
	key    string
	weight int
}

type vnode struct {
	hash  uint64
	owner *node
}

func NewRing[T any](hasher Hasher[T]) *Ring[T] {
	return &Ring[T]{
		hasher: hasher,
		nodes:  make(map[string]*node),
	}
}

func (h *Ring[T]) AddNode(key string, weight int) error {
	if weight <= 0 {
		return ErrInvalidWeight
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.nodes[key]; ok {
		return ErrNodeAlreadyExists
	}

	n := &node{
		key:    key,
		weight: weight,
	}

	h.nodes[key] = n
	h.addVnodes(n)

	return nil
}

// UpdateNode атомарно меняет вес существующей ноды или добавляет новую.
// Атомарность важна: пара RemoveNode+AddNode на время «выключает» ноду и мигрирует ключи дважды.
func (h *Ring[T]) UpdateNode(key string, weight int) error {
	if weight <= 0 {
		return ErrInvalidWeight
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	n, ok := h.nodes[key]
	if ok {
		h.vnodes = slices.DeleteFunc(h.vnodes, func(vn vnode) bool {
			return vn.owner == n
		})
		n.weight = weight
	} else {
		n = &node{key: key, weight: weight}
		h.nodes[key] = n
	}

	h.addVnodes(n)

	return nil
}

// Nodes возвращает ключи всех нод (для дебага/метрик).
func (h *Ring[T]) Nodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	res := make([]string, 0, len(h.nodes))
	for key := range h.nodes {
		res = append(res, key)
	}

	return res
}

// addVnodes создаёт vnode'ы ноды и вставляет их в отсортированное кольцо.
// Вызывается только под h.mu.Lock().
func (h *Ring[T]) addVnodes(n *node) {
	vn := make([]vnode, n.weight)
	for i := range n.weight {
		vk := fmt.Sprintf("%s#%d", n.key, i)
		vn[i] = vnode{
			hash:  hash([]byte(vk)),
			owner: n,
		}
	}

	// здесь защита от коллизий хеша, в случае если будет дублирование, тогда у нас есть детерминированное правило
	// сортировки, которое сделает так что на второй и последующие дубль никогда не будут выдавать в ответах
	h.vnodes = append(h.vnodes, vn...)
	slices.SortFunc(h.vnodes, func(a, b vnode) int {
		if a.hash == b.hash {
			// детерминированный tie-breaker
			if a.owner.key < b.owner.key {
				return -1
			}
			if a.owner.key > b.owner.key {
				return 1
			}
			return 0
		}

		// вычитать нельзя, так как int переполняется
		if a.hash < b.hash {
			return -1
		}
		if a.hash > b.hash {
			return 1
		}

		return 0
	})
}

func (h *Ring[T]) RemoveNode(key string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	n, ok := h.nodes[key]
	if !ok {
		return ErrNodeNotFound
	}

	delete(h.nodes, key)

	h.vnodes = slices.DeleteFunc(h.vnodes, func(vn vnode) bool {
		return vn.owner == n
	})

	return nil
}

func (h *Ring[T]) Get(key T) (string, error) {
	hsh := h.hasher.Hash(key)

	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.vnodes) == 0 {
		return "", ErrRingEmpty
	}

	idx, _ := slices.BinarySearchFunc(h.vnodes, hsh, func(v vnode, hash uint64) int {
		// вычитать нельзя: uint64 переполняется и d < 0 никогда не сработает
		switch {
		case v.hash < hash:
			return -1
		case v.hash > hash:
			return 1
		}

		return 0
	})

	// Если дошли до конца, то зануляем потому что кольцо
	// вопрос: не будет ли перегружена нода с 0 - нет, это стандартный wrap-around кольца. Нода на позиции 0
	//обслуживает только свою дугу (от последнего vnode до неё), как и все остальные.
	if idx >= len(h.vnodes) {
		idx = 0
	}

	// BinarySearchFunc возвращает found или индекс где мог бы быть (таким образом сразу +1 вперед)
	return h.vnodes[idx].owner.key, nil
}

func hash(key []byte) uint64 {
	// return crc32.ChecksumIEEE(key)
	// этот хеш лучше, меньше коллизий, автор пишет что самый быстрый из всех
	return xxhash.Sum64(key)
}
