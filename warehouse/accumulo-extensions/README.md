# DataWave Accumulo Extensions

Server-side and shell extensions for Accumulo: the sharded table balancers, the date-based tiered
volume chooser, and the DataWave shell extension described below.

## Shell extension

The stock `scan` command filters columns out of a whole-row scan and takes option values literally,
which makes DataWave's null-delimited keys awkward to reach from the shell. `dw::scan` adds the
key-range scoping proposed in [apache/accumulo#5959](https://github.com/apache/accumulo/pull/5959)
along with escape handling for those keys.

### Enabling it

Put this module's jar on the shell classpath (`accumulo-shell` picks up `lib/` and `lib/ext/`), then
enable extensions once per shell session:

```
root@accumulo> extensions -e
root@accumulo> extensions -l
dw
```

The commands are then available under the extension name:

```
root@accumulo shard> dw::scan -r 20260818_0 -cf datatype\0uid -cq FIELD\0value
```

### Escapes

Every row, column family and column qualifier value is decoded before use, so key components can be
typed directly: `\0` for the null delimiter, `\xHH` for any byte, plus `\n`, `\r`, `\t` and `\\`. An
unrecognized escape is rejected rather than passed through, since a typo would otherwise silently
scan the wrong range. Pass `--no-escapes` to take values literally.

### Scoping the range

Where the stock command scans the row and discards non-matching columns, `dw::scan` narrows the scan
range itself:

| Command | Range scanned |
| --- | --- |
| `dw::scan -r <row> -cf <cf>` | that column family within the row |
| `dw::scan -r <row> -cf <cf> -cq <cq>` | that exact column within the row |
| `dw::scan -r <row> -cfp <prefix>` | the column families starting with the prefix |
| `dw::scan -r <row> -cf <cf> -cqp <prefix>` | the column qualifiers starting with the prefix |
| `dw::scan -b <row> -e <row> -cf <cf>` | the row range, with `-cf` filtering columns as before |

A column family cannot bound a range that spans rows, so `-cf` keeps its stock filtering behavior
when a row range is given rather than a single `-r`.

The `-bkcf`, `-bkcq`, `-bkts`, `-ekcf`, `-ekcq` and `-ekts` options set the range endpoints directly
and override the scoping above. An end key is interpreted at the granularity it was given — `-ekcf`
stops after the last key in that column family, `-ekcq` after the last key in that column qualifier
— so the column named is included. Pass `-ee` to use the endpoint literally instead.

### Examples

Read one event's fields:

```
dw::scan -t shard -r 20260818_0 -cf datatype\0uid
```

Read one field of one event:

```
dw::scan -t shard -r 20260818_0 -cf datatype\0uid -cq FIELD\0value
```

Read every field index entry in a shard:

```
dw::scan -t shard -r 20260818_0 -cfp fi\0
```

Read the field index entries for one value of one field:

```
dw::scan -t shard -r 20260818_0 -cf fi\0FIELD -cqp value\0
```
