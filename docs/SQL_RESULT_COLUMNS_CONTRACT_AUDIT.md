# Duplicate SQL result-column contract audit

Issue [#157](https://github.com/nowledge-co/hawdb/issues/157) requires
`SELECT * FROM a, b WHERE a.id = b.id` and `SELECT * FROM a CROSS JOIN b` to execute
with PostgreSQL-compatible results. HAVING and FROM scope support do not fully
satisfy the first example when both tables expose `id`.

## Existing representation

`crates/executor/src/profile.rs` defines `Row` as `BTreeMap<String, Value>`.
`QuerySchema::try_new` rejects duplicate column names. `QueryRow::get`, string
indexing and `QueryRowRef::to_owned_row` rely on that unique-name contract.
`crates/relational/src/query/expression.rs::select_output_schema` names the
SELECT output during preparation and rejects duplicate names before any row is
read, so such a statement fails even when no row matches, and so does its
EXPLAIN. `project_bound_row` and the map-based aggregate and blocking-projection
paths still build name-keyed rows, which `QueryRows::try_from_named_rows` orders
by that schema.

Refusing repeated names during preparation is a deliberate behavior change
([#1225](https://github.com/nowledge-co/hawdb/issues/1225)). Before it, the
columnar aggregate collapsed `SELECT count(*), count(email) FROM t` into one
`count` column, and `SELECT a AS x, b AS x FROM t WHERE false` returned an
empty result with no columns. PostgreSQL accepts both. HawDB refuses them for as
long as result rows are keyed by name. The same check refuses output names
that start with the internal sort-key prefix `__hawdb_relational_sort_`, which
blocking projection strips from its rows.

For tables `a(id, value_a)` and `b(id, value_b)`, PostgreSQL retains both `id`
columns in distinct result positions. A map cannot represent that schema. Letting
one overwrite the other loses data; renaming them to `a.id` and `b.id` changes
result labels. Either would conceal an unmet acceptance requirement. Explicit
projection aliases work with the current interface but are not the issue's exact
wildcard example.

## Delivery boundary and next decision

Keep #157 open for this contract requirement. The current implementation preserves
unique result names and delivers the executable HAVING and FROM behavior that fits
that contract. It does not claim PostgreSQL-compatible duplicate labels.

A separate result-interface design must decide how positional SQL rows coexist
with the embedded facade's name-based rows. Two coherent choices remain:

1. Add an opt-in positional SQL result interface while preserving existing query
   outputs. It must expose duplicate labels and ordinal values, and make conversion
   to a unique-name map fallible. Reuse the query/aggregation/spill owners with a
   positional projection contract; do not add a second executor or route-specific
   query API. This best preserves current caller behavior but adds a public result
   capability and requires a concrete facade design before implementation.
2. Extend existing QuerySchema/QueryRows to represent duplicate labels. This needs
   explicit semantics for get/indexing, iteration, schema equality, output order,
   map conversion and every current consumer. The change is broader than SELECT
   syntax and should not be inferred from the current issue's parser work.

Neither interface change is implemented or approved by this audit. The existing
#196 cache-ownership approval and #159/#206 decisions do not decide SQL result
semantics. Source-compatible aliases are an interim caller option, not evidence
that the exact acceptance example is complete.

Verification for the chosen design must retain every column, position, label and
value through Cartesian/filtered/outer joins, blocking and streaming output,
empty schemas/results, duplicate aliases, pagination, payload admission and
explicit map-conversion errors. Query behavior for existing unique-name results
must remain unchanged.
