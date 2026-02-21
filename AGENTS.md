# Agent Instructions – P&C Insurance Data Workspace

This project is a **test harness** for evaluating AI data-analyst agents against a P&C insurance DuckDB database.

## Project Structure

- `insurance_pc.duckdb` – DuckDB database with P&C insurance data
- `generate_insurance_data.py` – Script that generates the fake data
- `evals/` – Evaluation framework (questions, gold answers, harness)
- `pi_data_extension.md` – Design doc for the pi-data agent extension

## Database

The DuckDB database is at `insurance_pc.duckdb`. Use the local `./duckdb` CLI to query it.

## Key Files

- `evals/agent_prompt.md` – The system prompt given to the agent-under-test (schema rules, metric definitions)
- `evals/questions.yaml` – Eval questions (no answers)
- `evals/gold_answers.yaml` – Expected answers (harness-only, never shown to agent-under-test)
- `evals/run_eval.py` – Evaluation runner
