#!/usr/bin/env python3
"""
P&C Insurance Agent Evaluation Harness

Workflow:
  1. Load questions from questions.yaml
  2. For each question, call the agent (stubbed – plug in your agent)
  3. Run the gold_query to get the expected answer
  4. Compare agent answer to gold answer within tolerance
  5. Produce a scorecard

Usage:
    uv run python evals/run_eval.py                    # run all questions
    uv run python evals/run_eval.py --ids loss_ratio_ho_2023 freq_sev_auto_2022
    uv run python evals/run_eval.py --category loss_ratio
    uv run python evals/run_eval.py --difficulty easy
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import yaml

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "insurance_pc.duckdb"
QUESTIONS_PATH = Path(__file__).resolve().parent / "questions.yaml"
GOLD_ANSWERS_PATH = Path(__file__).resolve().parent / "gold_answers.yaml"
AGENT_PROMPT_PATH = Path(__file__).resolve().parent / "agent_prompt.md"
RESULTS_DIR = Path(__file__).resolve().parent / "results"


# ---------------------------------------------------------------------------
# Gold answer retrieval
# ---------------------------------------------------------------------------

def get_gold_answer(question: dict) -> dict | None:
    """Run the gold_query and return the result as a dict."""
    gold_query = question.get("gold_query")
    if not gold_query:
        return None

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        result = con.execute(gold_query).fetchall()
        columns = [desc[0] for desc in con.description]
        if not result:
            return None
        # Return all rows
        rows = [dict(zip(columns, row)) for row in result]
        return {"columns": columns, "rows": rows}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Answer comparison
# ---------------------------------------------------------------------------

def compare_numeric(actual: float, expected: float, tolerance: float) -> dict:
    """Compare two numeric values within a relative tolerance."""
    if expected == 0:
        abs_err = abs(actual)
        passed = abs_err < 0.01
        return {"passed": passed, "actual": actual, "expected": expected,
                "abs_error": abs_err, "rel_error": None}
    rel_err = abs(actual - expected) / abs(expected)
    passed = rel_err <= tolerance
    return {"passed": passed, "actual": actual, "expected": expected,
            "abs_error": round(abs(actual - expected), 6),
            "rel_error": round(rel_err, 6)}


def extract_number(text: str) -> float | None:
    """Try to extract a numeric value from agent response text."""
    # Remove commas, $, %
    cleaned = text.replace(",", "").replace("$", "").replace("%", "")
    # Find all numbers (including negatives and decimals)
    matches = re.findall(r"-?\d+\.?\d*", cleaned)
    if matches:
        return float(matches[0])
    return None


def score_question(question: dict, agent_answer: dict) -> dict:
    """
    Score an agent's answer against the gold truth.

    agent_answer should be a dict with:
      - "response": str  (the agent's text response)
      - "sql_used": str | None  (the SQL the agent ran, if any)
      - "numeric_result": float | dict | None  (parsed numeric answer)
    """
    q_id = question["id"]
    tolerance = question.get("tolerance", 0.05)

    # Qualitative questions – return for manual review
    if tolerance == "qualitative":
        return {
            "id": q_id,
            "category": question["category"],
            "difficulty": question["difficulty"],
            "passed": None,  # requires manual review
            "reason": "qualitative – manual review required",
            "agent_response": agent_answer.get("response", ""),
        }

    gold = get_gold_answer(question)
    if gold is None:
        return {
            "id": q_id,
            "passed": None,
            "reason": "no gold query defined",
        }

    # If agent provided numeric_result, compare directly
    numeric = agent_answer.get("numeric_result")
    result = {
        "id": q_id,
        "category": question["category"],
        "difficulty": question["difficulty"],
        "agent_response": agent_answer.get("response", "")[:500],
        "sql_used": agent_answer.get("sql_used"),
        "gold_answer": gold,
    }

    # Single expected value
    if "expected_value" in question and numeric is not None:
        expected = float(question["expected_value"])
        tol = float(tolerance) if tolerance != "exact" else 0.0
        cmp = compare_numeric(float(numeric), expected, tol)
        result.update(cmp)
        return result

    # Multiple expected values
    if "expected_values" in question and isinstance(numeric, dict):
        comparisons = {}
        all_passed = True
        for key, expected_val in question["expected_values"].items():
            if key in numeric:
                tol = float(tolerance) if tolerance != "exact" else 0.0
                try:
                    cmp = compare_numeric(float(numeric[key]), float(expected_val), tol)
                    comparisons[key] = cmp
                    if not cmp["passed"]:
                        all_passed = False
                except (ValueError, TypeError):
                    # String comparison (e.g., LOB name)
                    passed = str(numeric[key]).strip().upper() == str(expected_val).strip().upper()
                    comparisons[key] = {"passed": passed, "actual": numeric[key], "expected": expected_val}
                    if not passed:
                        all_passed = False
            else:
                comparisons[key] = {"passed": False, "reason": "key not in agent answer"}
                all_passed = False
        result["passed"] = all_passed
        result["comparisons"] = comparisons
        return result

    # Fallback: try to extract number from response text
    if "expected_value" in question:
        response_text = agent_answer.get("response", "")
        extracted = extract_number(response_text)
        if extracted is not None:
            expected = float(question["expected_value"])
            tol = float(tolerance) if tolerance != "exact" else 0.0
            cmp = compare_numeric(extracted, expected, tol)
            cmp["extraction_method"] = "regex_from_response"
            result.update(cmp)
            return result

    result["passed"] = None
    result["reason"] = "could not compare – no numeric result from agent"
    return result


# ---------------------------------------------------------------------------
# Agent stub – replace with your actual agent call
# ---------------------------------------------------------------------------

def call_agent(question_text: str) -> dict:
    """
    Stub: call the agent with a question and return its answer.

    Replace this with your actual agent integration. The agent should:
      1. Receive the question as natural language
      2. Explore the database (only allowed schemas!)
      3. Write and execute SQL
      4. Return a response

    Expected return format:
        {
            "response": "The loss ratio for HO 2023 is 4.21...",
            "sql_used": "SELECT ... FROM core.claims ...",
            "numeric_result": 4.212433  # or dict for multi-value
        }
    """
    # TODO: replace with real agent call
    return {
        "response": "[AGENT NOT CONNECTED] – stub response",
        "sql_used": None,
        "numeric_result": None,
    }


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

def load_questions(
    ids: list[str] | None = None,
    category: str | None = None,
    difficulty: str | None = None,
) -> list[dict]:
    """Load questions and merge with gold answers for scoring."""
    with open(QUESTIONS_PATH) as f:
        questions = yaml.safe_load(f)

    with open(GOLD_ANSWERS_PATH) as f:
        gold_answers = yaml.safe_load(f)

    # Index gold answers by id
    gold_by_id = {g["id"]: g for g in gold_answers}

    # Merge gold answer fields into each question
    for q in questions:
        gold = gold_by_id.get(q["id"], {})
        for key in ("gold_query", "expected_value", "expected_values",
                     "tolerance", "tolerance_claim_count", "notes"):
            if key in gold:
                q[key] = gold[key]

    if ids:
        questions = [q for q in questions if q["id"] in ids]
    if category:
        questions = [q for q in questions if q["category"] == category]
    if difficulty:
        questions = [q for q in questions if q["difficulty"] == difficulty]

    return questions


def run_eval(questions: list[dict]) -> list[dict]:
    """Run evaluation on a list of questions."""
    results = []
    for i, q in enumerate(questions, 1):
        print(f"\n[{i}/{len(questions)}] {q['id']} ({q['difficulty']})")
        print(f"  Q: {q['question'][:100].strip()}...")

        agent_answer = call_agent(q["question"])
        score = score_question(q, agent_answer)
        results.append(score)

        status = "✅" if score.get("passed") is True else \
                 "❌" if score.get("passed") is False else "⚠️  MANUAL"
        print(f"  {status}  {score.get('reason', '')}")
        if "rel_error" in score and score["rel_error"] is not None:
            print(f"      expected={score['expected']}  actual={score['actual']}  rel_err={score['rel_error']}")

    return results


def print_scorecard(results: list[dict]):
    """Print a summary scorecard."""
    total = len(results)
    passed = sum(1 for r in results if r.get("passed") is True)
    failed = sum(1 for r in results if r.get("passed") is False)
    manual = sum(1 for r in results if r.get("passed") is None)

    print("\n" + "=" * 60)
    print("EVALUATION SCORECARD")
    print("=" * 60)
    print(f"  Total questions:    {total}")
    print(f"  ✅ Passed:          {passed}")
    print(f"  ❌ Failed:          {failed}")
    print(f"  ⚠️  Manual review:  {manual}")
    if total - manual > 0:
        print(f"  Score:              {passed}/{total - manual} ({100 * passed / (total - manual):.1f}%)")
    print("=" * 60)

    # By category
    categories = sorted(set(r.get("category", "unknown") for r in results))
    if len(categories) > 1:
        print("\nBy category:")
        for cat in categories:
            cat_results = [r for r in results if r.get("category") == cat]
            cat_passed = sum(1 for r in cat_results if r.get("passed") is True)
            cat_total = sum(1 for r in cat_results if r.get("passed") is not None)
            print(f"  {cat:<25} {cat_passed}/{cat_total}")

    # By difficulty
    print("\nBy difficulty:")
    for diff in ["easy", "medium", "hard"]:
        diff_results = [r for r in results if r.get("difficulty") == diff]
        if diff_results:
            diff_passed = sum(1 for r in diff_results if r.get("passed") is True)
            diff_total = sum(1 for r in diff_results if r.get("passed") is not None)
            print(f"  {diff:<25} {diff_passed}/{diff_total}")


def save_results(results: list[dict]):
    """Save results to a timestamped JSON file."""
    RESULTS_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    outpath = RESULTS_DIR / f"eval_{ts}.json"
    with open(outpath, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to: {outpath}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="P&C Insurance Agent Evaluation Harness")
    parser.add_argument("--ids", nargs="+", help="Run specific question IDs")
    parser.add_argument("--category", help="Filter by category")
    parser.add_argument("--difficulty", choices=["easy", "medium", "hard"])
    parser.add_argument("--list", action="store_true", help="List questions without running")
    parser.add_argument("--dry-run", action="store_true", help="Show gold answers only")
    args = parser.parse_args()

    questions = load_questions(ids=args.ids, category=args.category, difficulty=args.difficulty)

    if not questions:
        print("No questions matched filters.")
        sys.exit(1)

    if args.list:
        for q in questions:
            print(f"  [{q['difficulty']:<6}] {q['id']:<35} {q['category']}")
        print(f"\n{len(questions)} questions")
        sys.exit(0)

    if args.dry_run:
        for q in questions:
            print(f"\n{'=' * 60}")
            print(f"ID: {q['id']}  ({q['difficulty']})")
            print(f"Q:  {q['question'].strip()}")
            gold = get_gold_answer(q)
            if gold:
                print(f"Gold: {json.dumps(gold['rows'], default=str)}")
            else:
                print("Gold: (qualitative – no numeric answer)")
        sys.exit(0)

    print(f"Running {len(questions)} evaluation questions...")
    results = run_eval(questions)
    print_scorecard(results)
    save_results(results)


if __name__ == "__main__":
    main()
