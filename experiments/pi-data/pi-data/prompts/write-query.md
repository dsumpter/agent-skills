---
name: write-query
description: Generate optimized SQL
---

Generate an optimized SQL query for the user's request.

Workflow:
1. Search for relevant tables: `data_search`
2. Describe tables: `data_describe`
3. Generate SQL: `data_write_sql`
4. Validate: `data_validate`
5. Explain query structure and assumptions

Request: $1
