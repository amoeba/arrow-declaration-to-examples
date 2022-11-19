# arrow-declaration-to-examples

TODO

Currently just an example of using `StartAndCollect`.

## Running

### Pre-requisites

- `pkg-config` (`brew install pkg-config`)
- A >=C++17 compiler (e.g., clang 14)
- Arrow C++ headers (`brew install apache-arrow`)

## Compile

A `Makefile` is provided to make compiling both examples a single command:

```sh
make
```

## Run

```
./example
```

## Output

The above should produce:

```
a: int32
b: bool
----
a:
  [
    [
      0,
      4
    ],
    [
      5,
      6,
      7
    ],
    [
      8,
      9,
      10
    ]
  ]
b:
  [
    [
      false,
      true
    ],
    [
      true,
      false,
      true
    ],
    [
      false,
      true,
      false
    ]
  ]
```
