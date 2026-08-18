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
range itself. The last component named may be partial, and bounds the range exactly the way a partial
key does when building a `Range` by hand — the scan starts at that key and runs through every key
sorting under it:

```java
new Range(new Key("20260818_0", "tf", "datatype\0uid"))
```

So `-cf tf -cq datatype\0uid` reaches `tf:datatype\0uid\0FIELD\0value`, and `-cf fi\0` reaches every
`fi\0FIELD` column family in the row. A complete value works the same way; it simply has less sorting
under it.

Only the last component named carries the bound, so **specifying `-cq` makes `-cf` fully qualified**.
That is what makes a term frequency scan work: `-cf tf` is matched exactly while the qualifier is
partial.

| Command | Range scanned |
| --- | --- |
| `dw::scan -r <row> -cf <cf>` | that column family, and any sorting under it |
| `dw::scan -r <row> -cf <cf> -cq <cq>` | that exact family, and the qualifiers sorting under `<cq>` |
| `dw::scan -b <row> -e <row> -cf <cf>` | the row range, with `-cf` filtering columns as before |

A column family cannot bound a range that spans rows, so `-cf` keeps its stock exact-match filtering
behavior when a row range is given rather than a single `-r`.

One consequence of partial bounds is that a component covers anything extending it. In the shard
table a child uid extends its parent, so `-cf datatype\0-abc.def.ghi` also returns the fields of
`datatype\0-abc.def.ghi.1`. Use `-ekcf` with `-ee` to stop short of that.

The `-bkcf`, `-bkcq`, `-bkts`, `-ekcf`, `-ekcq` and `-ekts` options set the range endpoints directly
and override the scoping above. An end key covers the keys sorting under it on the same terms; pass
`-ee` to stop at the endpoint literally instead. Timestamps are fixed-width and always bound exactly.

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
dw::scan -t shard -r 20260818_0 -cf fi\0
```

Read the field index entries for one value of one field:

```
dw::scan -t shard -r 20260818_0 -cf fi\0FIELD -cq value
```

Read the term frequency entries for one event:

```
dw::scan -t shard -r 20260818_0 -cf tf -cq datatype\0uid
```
