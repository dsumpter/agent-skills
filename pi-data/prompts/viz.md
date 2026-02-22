---
name: viz
description: Create a visualization
---

Create a visualization for the specified data or question.

Workflow:
1. If no resultId provided, execute necessary query
2. Suggest appropriate chart: `data_viz_suggest`
3. Render inline: `data_viz_inline`
4. Explain what the chart shows

Request: $1
