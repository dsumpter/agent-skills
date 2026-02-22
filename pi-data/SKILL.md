---
name: pi-data
description: Use pi-data tools to explore DuckDB datasets, run SQL, and build charts.
---

Use `data_session` to create/load the workspace before other tools.

Common workflow:
1. `data_search` / `data_list_tables` to find tables.
2. `data_describe` to inspect schema.
3. `data_write_sql` + `data_validate` to draft safe SQL.
4. `data_execute` to materialize results (returns `resultId`).
5. `data_preview` / `data_profile` / `data_aggregate` for analysis.
6. `data_viz_suggest` + `data_viz_inline` or `data_viz_html` for charts.
7. `data_nb_*` tools for notebooks.

Never stream full datasets in responses. Prefer result IDs and previews.
