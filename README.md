# go-consistent-hashing

Простая и потокобезопасная реализация [consistent hashing](https://en.wikipedia.org/wiki/Consistent_hashing) с
виртуальными узлами на Go.

## Особенности

- **Консистентное хеширование** — при добавлении или удалении узла перераспределяется только небольшая часть ключей.
- **Виртуальные узлы** — равномерное распределение ключей и возможность задавать веса реальным нодам.
- **Потокобезопасность** — чтение и изменение кольца защищены `sync.RWMutex`.
- **Generic API** — тип ключа задаётся через параметр `[T any]`.
- **Внешняя хеш-функция** — клиент предоставляет реализацию `Hasher[T]`, чтобы избежать reflection и лишних аллокаций.
- **Атомарное изменение веса** — `UpdateNode` обновляет вес без промежуточного удаления ноды.

## Установка

```bash
go get github.com/gallyamow/go-consistent-hashing
```

## Использование

```go
package main

import (
	"fmt"
	"log"

	"github.com/cespare/xxhash/v2"
	"github.com/gallyamow/go-consistent-hashing"
)

// StringHasher - реализация Hasher для строковых ключей.
type StringHasher struct{}

func (StringHasher) Hash(key string) uint64 {
	return xxhash.Sum64String(key)
}

func main() {
	ring := consistenthashing.NewRing[string](StringHasher{})

	_ = ring.AddNode("node-a", 100)
	_ = ring.AddNode("node-b", 100)
	_ = ring.AddNode("node-c", 200) // node-c получит примерно в 2 раза больше ключей

	node, err := ring.Get("my-key")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("key routed to:", node)
}
```

## API

### Создание кольца

```go
ring := consistenthashing.NewRing[T](hasher Hasher[T])
```

`Hasher[T]` должен реализовать один метод:

```go
type Hasher[T any] interface {
    Hash(key T) uint64
}
```

### Добавление узла

```go
err := ring.AddNode("node-a", 100)
```

- `weight` — количество виртуальных узлов; чем больше вес, тем больше ключей достаётся ноде.
- Вес должен быть больше нуля.

### Обновление веса

```go
err := ring.UpdateNode("node-a", 200)
```

Атомарно меняет вес существующей ноды либо создаёт новую. Это удобнее, чем `RemoveNode` + `AddNode`, потому что нода не
исчезает из кольца на время обновления.

### Удаление узла

```go
err := ring.RemoveNode("node-a")
```

Возвращает `ErrNodeNotFound`, если узла нет.

### Получение узла по ключу

```go
node, err := ring.Get(key)
```

Возвращает ключ ноды, которой принадлежит ключ. Если кольцо пусто — `ErrRingEmpty`.

### Список узлов

```go
nodes := ring.Nodes()
```

## Веса и виртуальные узлы

Каждая реальная нода представлена на кольце `weight` виртуальными узлами. Благодаря этому:

- ключи распределяются равномернее, чем по одному хешу на ноду;
- вес позволяет отдавать более мощной ноде большую долю ключей;
- при изменении веса перераспределяется только часть дуги кольца.

## Ошибки

```go
var (
    ErrNodeAlreadyExists = errors.New("node already exists")
    ErrNodeNotFound = errors.New("node not found")
    ErrInvalidWeight     = errors.New("invalid weight")
    ErrRingEmpty = errors.New("ring is empty")
)
```

## Tests и benchmarks

```bash
go test ./...
go test -race ./...
go test -bench=. -benchmem
```

```shell
goos: darwin
goarch: arm64
pkg: github.com/gallyamow/go-consistent-hashing
cpu: Apple M4
BenchmarkGet-10                 12730850                87.86 ns/op           15 B/op          1 allocs/op
BenchmarkGetParallel-10         10293666               119.2 ns/op            23 B/op          1 allocs/op
BenchmarkAddNode-10                10000            920862 ns/op            1508 B/op         23 allocs/op
PASS
```

## Когда использовать

- Распределение данных по нескольким шардам/серверам.
- Построение routing layer перед кластером хранилищ.
- Ситуации, где узлы часто добавляются или удаляются, и нужно минимизировать перемещение ключей.

## Когда НЕ использовать

- Если нужно хранить сами ключи и значения — это не KV-хранилище.
- Если требуется строгая балансировка с гарантированными равными долями; consistent hashing даёт приблизительное
  равномерное распределение.
