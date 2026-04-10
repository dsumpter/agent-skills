#!/usr/bin/env python3
"""
Generate a realistic, messy P&C insurance DuckDB database.

Design goals:
  - Multiple modeling patterns (3NF, star schema, OBT, activity/event schema)
  - CDC (change data capture) with _valid_from/_valid_to/_is_current on core tables
  - Multiple time semantics (loss_date, report_date, entry_date, accounting_date,
    processing_date, effective_date, booking_date, load_ts) that are NOT the same
  - Soft deletes mixed with hard deletes
  - Reversed/voided transactions that must be netted out
  - Inconsistent naming conventions across schemas
  - Overlapping data across source systems with subtle differences
  - Orphan records, duplicate keys, format mismatches

Layers:
  1. core.*               – 3NF with CDC versioning (agent must filter is_current)
  2. staging_legacy       – AS400 dump: UPPER_CASE abbreviated columns, strings for everything
  3. staging_guidewire    – camelCase, event/activity pattern, CDC snapshots
  4. staging_broker       – Mixed naming, duplicate submissions, format issues
  5. staging_duckcreek    – Dollar-formatted amounts, different date formats
  6. staging_activity     – Event log (insert/update/delete events)
  7. unstructured         – Free-text notes
  8. mart_claims          – Dimensional star schema (fct_ / dim_ prefixes)
  9. mart_underwriting    – 3NF-ish rollups
  10. mart_finance        – Transaction-grain journal
  11. mart_agency         – Denormalized
  12. mart_actuarial      – Star schema with conformed dimensions
  13. mart_executive      – One Big Table (OBT)
  14. gold_metrics        – Ground truth (eval only)
  15. data_quality        – Known issues log (eval only)
"""

import os
import random
import string
from datetime import datetime, timedelta, date

import duckdb
import faker

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SEED = 42
NUM_AGENTS = 50
NUM_INSUREDS = 2000
NUM_POLICIES = 5000
NUM_COVERAGES = 12000
NUM_CLAIMS = 3000
NUM_CLAIM_TRANSACTIONS = 15000
NUM_QUOTES = 8000
NUM_PREMIUM_TRANSACTIONS = 20000

DB_PATH = os.path.join(os.path.dirname(__file__), "insurance_pc.duckdb")

random.seed(SEED)
fake = faker.Faker()
faker.Faker.seed(SEED)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

LOB_CODES = ["HO", "AUTO", "CGL", "WC", "BOP", "CPP", "FARM", "IM"]
LOB_NAMES = {
    "HO": "Homeowners",
    "AUTO": "Personal Auto",
    "CGL": "Commercial General Liability",
    "WC": "Workers Compensation",
    "BOP": "Business Owners Policy",
    "CPP": "Commercial Package Policy",
    "FARM": "Farmowners",
    "IM": "Inland Marine",
}

COVERAGE_TYPES = [
    "BI", "PD", "COLL", "COMP", "UM", "UIM", "MED", "LIAB", "DWELLING",
    "CONTENTS", "LOI", "ADDL_LIVING", "SCHED_PROP", "GL", "PROD_LIAB",
]

CLAIM_STATUSES = ["OPEN", "CLOSED", "REOPENED", "SUBROGATION", "LITIGATION"]
CLAIM_CAUSES = [
    "FIRE", "WATER", "THEFT", "COLLISION", "WEATHER", "SLIP_FALL",
    "VANDALISM", "MALPRACTICE", "PRODUCT_DEFECT", "OTHER",
]

POLICY_STATUSES = ["ACTIVE", "CANCELLED", "EXPIRED", "NON_RENEWED", "PENDING"]

SOURCE_SYSTEMS = ["LEGACY_AS400", "GUIDEWIRE_PC", "DUCK_CREEK", "MANUAL_ENTRY", "BROKER_FEED"]


def rand_date(start_year=2020, end_year=2025):
    s = datetime(start_year, 1, 1)
    e = datetime(end_year, 12, 31)
    delta = (e - s).days
    return s + timedelta(days=random.randint(0, delta))


def rand_ts(base_date):
    """Add random HH:MM:SS to a date to make a timestamp."""
    return base_date + timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )


def rand_premium():
    return round(random.uniform(200, 25000), 2)


def maybe_null(value, null_pct=0.05):
    return None if random.random() < null_pct else value


# ---------------------------------------------------------------------------
# Core entity generators (3NF + CDC versioning)
# ---------------------------------------------------------------------------

def gen_agents():
    agents = []
    for i in range(1, NUM_AGENTS + 1):
        agents.append({
            "agent_id": i,
            "agent_code": f"AGT-{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "agency_name": fake.company(),
            "license_state": random.choice(STATES),
            "license_number": fake.bothify("??######"),
            "commission_rate": round(random.uniform(0.05, 0.20), 4),
            "appointed_date": rand_date(2010, 2022).date().isoformat(),
            "terminated_date": maybe_null(rand_date(2023, 2025).date().isoformat(), 0.85),
            "email": fake.email(),
            "phone": fake.phone_number(),
        })
    return agents


def gen_insureds():
    insureds = []
    for i in range(1, NUM_INSUREDS + 1):
        is_commercial = random.random() < 0.3
        insureds.append({
            "insured_id": i,
            "insured_type": "COMMERCIAL" if is_commercial else "PERSONAL",
            "first_name": None if is_commercial else fake.first_name(),
            "last_name": None if is_commercial else fake.last_name(),
            "company_name": fake.company() if is_commercial else None,
            "dba_name": maybe_null(fake.company(), 0.7) if is_commercial else None,
            "tax_id": fake.bothify("##-#######"),
            "date_of_birth": None if is_commercial else fake.date_of_birth(
                minimum_age=18, maximum_age=85).isoformat(),
            "address_line1": fake.street_address(),
            "address_line2": maybe_null(fake.secondary_address(), 0.7),
            "city": fake.city(),
            "state": random.choice(STATES),
            "zip_code": fake.zipcode(),
            "email": maybe_null(fake.email(), 0.15),
            "phone": maybe_null(fake.phone_number(), 0.1),
            "credit_score": maybe_null(random.randint(300, 850), 0.2),
            "created_at": rand_date(2015, 2023),
            "source_system": random.choice(SOURCE_SYSTEMS),
        })
    return insureds


def gen_policies_with_cdc(insureds, agents):
    """
    Generate policies with CDC versioning.
    ~30% of policies get 2-3 versions (endorsements, status changes).
    Only the version with is_current_record=True should be used for metrics.
    The row_id is the surrogate PK; policy_id is the business key.

    Time semantics:
      - effective_date: when coverage starts (can be backdated)
      - expiration_date: when coverage ends
      - binding_date: when the policy was bound
      - issue_date: when the policy document was issued (>= binding_date)
      - system_entry_date: when the record entered the system (never backdated)
      - booking_date: when premium was booked to the GL (never backdated)
      - _valid_from / _valid_to: CDC window (system time, not business time)
    """
    policies = []
    row_id = 0
    for i in range(1, NUM_POLICIES + 1):
        insured = random.choice(insureds)
        agent = random.choice(agents)
        lob = random.choice(LOB_CODES)
        eff = rand_date(2020, 2025)
        exp = eff + timedelta(days=random.choice([180, 365, 730]))
        binding = eff - timedelta(days=random.randint(1, 30))
        issue = binding + timedelta(days=random.randint(0, 14))
        # system_entry_date is always >= binding_date, never backdated
        system_entry = issue + timedelta(days=random.randint(0, 5))
        # booking_date can lag system_entry by a few days
        booking = system_entry + timedelta(days=random.randint(0, 10))

        base_premium = rand_premium()
        base_exposure = round(random.uniform(1, 100), 2)
        base_status = random.choice(POLICY_STATUSES)

        num_versions = 1
        if random.random() < 0.30:
            num_versions = random.choice([2, 2, 3])

        for v in range(num_versions):
            row_id += 1
            is_current = (v == num_versions - 1)
            valid_from = system_entry + timedelta(days=v * random.randint(10, 90))
            valid_to = None if is_current else (
                valid_from + timedelta(days=random.randint(10, 90)))

            # Evolve values across versions
            if v == 0:
                premium = base_premium
                exposure = base_exposure
                status = base_status
                cancel_date = None
                cancel_reason = None
            else:
                # Endorsement: premium changes, maybe status changes
                premium = round(base_premium * random.uniform(0.85, 1.25), 2)
                exposure = round(base_exposure * random.uniform(0.9, 1.1), 2)
                if random.random() < 0.3:
                    status = "CANCELLED"
                    cancel_date = (eff + timedelta(
                        days=random.randint(30, 300))).date().isoformat()
                    cancel_reason = random.choice([
                        "NON_PAY", "INSURED_REQ", "UW_ACTION", "REWRITE"])
                else:
                    status = base_status
                    cancel_date = None
                    cancel_reason = None

            # is_deleted: ~2% of non-current versions are soft-deleted
            is_deleted = False
            if not is_current and random.random() < 0.02:
                is_deleted = True

            policies.append({
                "row_id": row_id,
                "policy_id": i,
                "policy_number": f"POL-{lob}-{i:06d}",
                "version_number": v + 1,
                "insured_id": insured["insured_id"],
                "agent_id": agent["agent_id"],
                "line_of_business": lob,
                "lob_description": LOB_NAMES[lob],
                "product_code": f"{lob}-{random.choice(['STD', 'PREM', 'BASIC'])}",
                "effective_date": eff.date().isoformat(),
                "expiration_date": exp.date().isoformat(),
                "binding_date": binding.date().isoformat(),
                "issue_date": issue.date().isoformat(),
                "system_entry_date": system_entry.date().isoformat(),
                "booking_date": booking.date().isoformat(),
                "policy_status": status,
                "policy_term_months": random.choice([6, 12, 24]),
                "state": insured["state"],
                "territory_code": f"T{random.randint(1, 50):02d}",
                "total_premium": premium,
                "total_exposure_units": exposure,
                "deductible_amount": random.choice([250, 500, 1000, 2500, 5000]),
                "policy_limit": random.choice(
                    [100000, 250000, 500000, 1000000, 2000000]),
                "underwriter_id": random.randint(1, 20),
                "cancellation_date": cancel_date,
                "cancellation_reason": cancel_reason,
                "renewal_of_policy_id": maybe_null(
                    random.randint(1, max(1, i - 1)), 0.7),
                "source_system": random.choice(SOURCE_SYSTEMS),
                "is_current_record": is_current,
                "is_deleted": is_deleted,
                "_valid_from": valid_from.isoformat(),
                "_valid_to": valid_to.isoformat() if valid_to else None,
                "created_at": system_entry,
                "updated_at": valid_from,
            })
    return policies


def get_current_policies(policies):
    """Return only is_current_record=True, is_deleted=False rows."""
    return [p for p in policies
            if p["is_current_record"] and not p["is_deleted"]]


def gen_coverages(policies):
    current = get_current_policies(policies)
    coverages = []
    cov_id = 1
    for pol in current:
        num_covs = random.randint(1, 5)
        for _ in range(num_covs):
            if cov_id > NUM_COVERAGES:
                return coverages
            cov_type = random.choice(COVERAGE_TYPES)
            coverages.append({
                "coverage_id": cov_id,
                "policy_id": pol["policy_id"],
                "coverage_code": cov_type,
                "coverage_description": f"{cov_type} Coverage",
                "coverage_limit": random.choice(
                    [25000, 50000, 100000, 250000, 500000, 1000000]),
                "coverage_deductible": random.choice([0, 250, 500, 1000, 2500]),
                "premium_amount": round(random.uniform(50, 5000), 2),
                "exposure_units": round(random.uniform(0.5, 50), 2),
                "effective_date": pol["effective_date"],
                "expiration_date": pol["expiration_date"],
                "rating_class_code": fake.bothify("RC-###"),
            })
            cov_id += 1
    return coverages


def gen_claims(policies):
    """
    Time semantics on claims:
      - loss_date (aka accident_date): when the loss event occurred
      - report_date: when the insured reported the claim
      - entry_date: when the adjuster entered the claim in the system
      - close_date: when the claim was closed (may reopen)
      - reopen_date: if reopened
      - processing_date: when the claim was last processed by batch
    """
    current = get_current_policies(policies)
    claims = []
    for i in range(1, NUM_CLAIMS + 1):
        pol = random.choice(current)
        loss_date = rand_date(2020, 2025)
        report_date = loss_date + timedelta(days=random.randint(0, 60))
        # entry_date: sometimes days/weeks after report_date (backlog)
        entry_date = report_date + timedelta(days=random.randint(0, 30))
        # processing_date: batch processing, always later
        processing_date = entry_date + timedelta(days=random.randint(0, 7))

        is_closed = random.random() < 0.6
        close_date = (report_date + timedelta(
            days=random.randint(30, 730))).date().isoformat() if is_closed else None
        is_reopened = is_closed and random.random() < 0.08
        reopen_date = None
        if is_reopened:
            reopen_date = (datetime.strptime(close_date, "%Y-%m-%d") +
                           timedelta(days=random.randint(30, 365))).date().isoformat()

        status = random.choice(CLAIM_STATUSES)
        if is_reopened:
            status = "REOPENED"
        elif is_closed and status not in ("CLOSED",):
            status = "CLOSED"

        claims.append({
            "claim_id": i,
            "claim_number": f"CLM-{i:08d}",
            "policy_id": pol["policy_id"],
            "policy_number": pol["policy_number"],
            "insured_id": pol["insured_id"],
            "line_of_business": pol["line_of_business"],
            "loss_date": loss_date.date().isoformat(),
            "report_date": report_date.date().isoformat(),
            "entry_date": entry_date.date().isoformat(),
            "processing_date": processing_date.date().isoformat(),
            "claim_status": status,
            "cause_of_loss": random.choice(CLAIM_CAUSES),
            "loss_description": fake.sentence(nb_words=12),
            "loss_state": pol["state"],
            "loss_zip": fake.zipcode(),
            "claimant_name": fake.name(),
            "claimant_type": random.choice(["FIRST_PARTY", "THIRD_PARTY"]),
            "adjuster_id": random.randint(1, 30),
            "adjuster_name": fake.name(),
            "reserve_amount": round(random.uniform(500, 200000), 2),
            "paid_loss_amount": round(random.uniform(0, 150000), 2),
            "paid_alae_amount": round(random.uniform(0, 30000), 2),
            "paid_ulae_amount": round(random.uniform(0, 10000), 2),
            "salvage_amount": round(random.uniform(0, 5000), 2),
            "subrogation_amount": round(random.uniform(0, 10000), 2),
            "total_incurred": 0,
            "catastrophe_code": maybe_null(
                f"CAT-{random.randint(1, 20):03d}", 0.85),
            "litigation_flag": random.random() < 0.1,
            "fraud_indicator": random.random() < 0.03,
            "close_date": close_date,
            "reopen_date": reopen_date,
            "source_system": random.choice(SOURCE_SYSTEMS),
            "is_deleted": random.random() < 0.01,
            "created_at": entry_date,
            "updated_at": processing_date,
        })
        claims[-1]["total_incurred"] = round(
            claims[-1]["reserve_amount"] + claims[-1]["paid_loss_amount"] +
            claims[-1]["paid_alae_amount"] - claims[-1]["salvage_amount"] -
            claims[-1]["subrogation_amount"], 2
        )
    return claims


def get_active_claims(claims):
    return [c for c in claims if not c["is_deleted"]]


def gen_claim_transactions(claims):
    """
    Claim financial transactions. Includes VOID transactions that reverse
    prior payments – agent must net these out.

    Time semantics:
      - transaction_date: when the payment/reserve was authorized
      - posting_date: when it was posted to the GL (never backdated, >= transaction_date)
      - check_date: when the check was cut (may be None for reserves)
      - load_ts: ETL load timestamp
    """
    active_claims = get_active_claims(claims)
    txns = []
    txn_id = 1
    for claim in active_claims:
        num_txns = random.randint(1, 10)
        for j in range(num_txns):
            if txn_id > NUM_CLAIM_TRANSACTIONS:
                return txns
            txn_type = random.choice([
                "RESERVE_SET", "RESERVE_CHANGE", "PAYMENT",
                "RECOVERY", "EXPENSE"])
            txn_date = rand_date(2020, 2025)
            posting_date = txn_date + timedelta(days=random.randint(0, 5))
            check_date = None
            if txn_type == "PAYMENT":
                check_date = (posting_date + timedelta(
                    days=random.randint(0, 10))).date().isoformat()
            amount = round(random.uniform(-5000, 50000), 2)
            load_ts = rand_ts(posting_date + timedelta(days=random.randint(0, 3)))

            txns.append({
                "transaction_id": txn_id,
                "claim_id": claim["claim_id"],
                "claim_number": claim["claim_number"],
                "transaction_type": txn_type,
                "transaction_date": txn_date.date().isoformat(),
                "posting_date": posting_date.date().isoformat(),
                "check_date": check_date,
                "amount": amount,
                "category": random.choice(
                    ["LOSS", "ALAE", "ULAE", "SALVAGE", "SUBRO"]),
                "check_number": maybe_null(
                    fake.bothify("CHK-########"), 0.4),
                "payee_name": maybe_null(fake.name(), 0.3),
                "description": fake.sentence(nb_words=8),
                "is_void": False,
                "void_of_transaction_id": None,
                "created_by": fake.user_name(),
                "load_ts": load_ts.isoformat(),
                "created_at": txn_date,
            })
            txn_id += 1

            # ~5% of payments get voided (a reversal transaction)
            if txn_type == "PAYMENT" and random.random() < 0.05:
                if txn_id > NUM_CLAIM_TRANSACTIONS:
                    return txns
                void_date = txn_date + timedelta(days=random.randint(1, 30))
                void_posting = void_date + timedelta(
                    days=random.randint(0, 5))
                txns.append({
                    "transaction_id": txn_id,
                    "claim_id": claim["claim_id"],
                    "claim_number": claim["claim_number"],
                    "transaction_type": "VOID",
                    "transaction_date": void_date.date().isoformat(),
                    "posting_date": void_posting.date().isoformat(),
                    "check_date": None,
                    "amount": -amount,
                    "category": txns[-1]["category"],
                    "check_number": None,
                    "payee_name": None,
                    "description": f"VOID OF TXN {txn_id - 1}",
                    "is_void": True,
                    "void_of_transaction_id": txn_id - 1,
                    "created_by": fake.user_name(),
                    "load_ts": rand_ts(void_posting).isoformat(),
                    "created_at": void_date,
                })
                txn_id += 1
    return txns


def gen_premium_transactions(policies):
    """
    Premium transactions with multiple time semantics:
      - transaction_date: when the premium event occurred
      - accounting_date: the accounting period it applies to (NEVER backdated)
      - booking_date: when booked to GL (can differ from accounting_date)
      - effective_date: the policy effective date this premium applies to
                        (CAN be backdated for endorsements)
      - load_ts: ETL load timestamp

    Includes REVERSAL transactions that must be netted out.
    """
    current = get_current_policies(policies)
    txns = []
    txn_id = 1
    for pol in current:
        num_txns = random.randint(1, 8)
        for _ in range(num_txns):
            if txn_id > NUM_PREMIUM_TRANSACTIONS:
                return txns
            txn_type = random.choice([
                "WRITTEN", "EARNED", "UNEARNED", "CEDED", "RETURN",
                "AUDIT", "ENDORSEMENT", "INSTALLMENT",
            ])
            txn_date = rand_date(2020, 2025)
            # accounting_date: same month or next month, never before txn_date
            accounting_date = txn_date + timedelta(
                days=random.randint(0, 30))
            # booking_date: GL posting, always >= accounting_date
            booking_date = accounting_date + timedelta(
                days=random.randint(0, 7))
            # effective_date: can be backdated (endorsement applies retroactively)
            eff_date = txn_date - timedelta(
                days=random.randint(0, 90)) if txn_type == "ENDORSEMENT" \
                else txn_date
            load_ts = rand_ts(booking_date + timedelta(
                days=random.randint(0, 2)))

            amount = round(random.uniform(-2000, 15000), 2)

            txns.append({
                "transaction_id": txn_id,
                "policy_id": pol["policy_id"],
                "policy_number": pol["policy_number"],
                "line_of_business": pol["line_of_business"],
                "transaction_type": txn_type,
                "transaction_date": txn_date.date().isoformat(),
                "accounting_date": accounting_date.date().isoformat(),
                "booking_date": booking_date.date().isoformat(),
                "effective_date": eff_date.date().isoformat(),
                "amount": amount,
                "accounting_period": accounting_date.strftime("%Y-%m"),
                "state": pol["state"],
                "agent_id": pol["agent_id"],
                "is_reversal": False,
                "reversal_of_transaction_id": None,
                "source_system": random.choice(SOURCE_SYSTEMS),
                "load_ts": load_ts.isoformat(),
                "created_at": txn_date,
            })
            txn_id += 1

            # ~3% of transactions get reversed
            if random.random() < 0.03:
                if txn_id > NUM_PREMIUM_TRANSACTIONS:
                    return txns
                rev_date = txn_date + timedelta(
                    days=random.randint(1, 45))
                rev_acct = rev_date + timedelta(days=random.randint(0, 30))
                txns.append({
                    "transaction_id": txn_id,
                    "policy_id": pol["policy_id"],
                    "policy_number": pol["policy_number"],
                    "line_of_business": pol["line_of_business"],
                    "transaction_type": "REVERSAL",
                    "transaction_date": rev_date.date().isoformat(),
                    "accounting_date": rev_acct.date().isoformat(),
                    "booking_date": (rev_acct + timedelta(
                        days=random.randint(0, 7))).date().isoformat(),
                    "effective_date": eff_date.date().isoformat(),
                    "amount": -amount,
                    "accounting_period": rev_acct.strftime("%Y-%m"),
                    "state": pol["state"],
                    "agent_id": pol["agent_id"],
                    "is_reversal": True,
                    "reversal_of_transaction_id": txn_id - 1,
                    "source_system": random.choice(SOURCE_SYSTEMS),
                    "load_ts": rand_ts(
                        rev_acct + timedelta(days=random.randint(0, 2))
                    ).isoformat(),
                    "created_at": rev_date,
                })
                txn_id += 1
    return txns


def gen_quotes(insureds, agents):
    quotes = []
    for i in range(1, NUM_QUOTES + 1):
        insured = random.choice(insureds)
        agent = random.choice(agents)
        lob = random.choice(LOB_CODES)
        quote_date = rand_date(2020, 2025)
        quotes.append({
            "quote_id": i,
            "quote_number": f"QUO-{i:06d}",
            "insured_id": insured["insured_id"],
            "agent_id": agent["agent_id"],
            "line_of_business": lob,
            "state": insured["state"],
            "quote_date": quote_date.date().isoformat(),
            "quoted_premium": rand_premium(),
            "status": random.choice([
                "QUOTED", "BOUND", "DECLINED", "EXPIRED", "LOST"]),
            "decline_reason": maybe_null(random.choice([
                "PRICE", "COVERAGE", "COMPETITOR", "NOT_ELIGIBLE"]), 0.6),
            "competitor_name": maybe_null(fake.company(), 0.7),
            "bound_policy_id": maybe_null(
                random.randint(1, NUM_POLICIES), 0.65),
            "source_system": random.choice(SOURCE_SYSTEMS),
            "created_at": quote_date,
        })
    return quotes


def gen_unstructured_notes(claims, policies):
    notes = []
    note_id = 1
    active_claims = get_active_claims(claims)
    current_policies = get_current_policies(policies)

    for claim in random.sample(active_claims, min(1500, len(active_claims))):
        for _ in range(random.randint(1, 5)):
            notes.append({
                "note_id": note_id,
                "entity_type": "CLAIM",
                "entity_id": claim["claim_id"],
                "entity_number": claim["claim_number"],
                "note_type": random.choice([
                    "ADJUSTER_NOTE", "PHONE_LOG", "EMAIL",
                    "INVESTIGATION", "SUPERVISOR_REVIEW"]),
                "author": fake.name(),
                "note_text": fake.paragraph(
                    nb_sentences=random.randint(2, 8)),
                "created_at": rand_date(2020, 2025).isoformat(),
                "source_system": random.choice(SOURCE_SYSTEMS),
            })
            note_id += 1

    for pol in random.sample(
            current_policies, min(1000, len(current_policies))):
        notes.append({
            "note_id": note_id,
            "entity_type": "POLICY",
            "entity_id": pol["policy_id"],
            "entity_number": pol["policy_number"],
            "note_type": random.choice([
                "UW_COMMENT", "INSPECTION_REPORT",
                "MVR_RESULT", "TIER_OVERRIDE"]),
            "author": fake.name(),
            "note_text": fake.paragraph(
                nb_sentences=random.randint(1, 5)),
            "created_at": rand_date(2020, 2025).isoformat(),
            "source_system": random.choice(SOURCE_SYSTEMS),
        })
        note_id += 1

    return notes


# ---------------------------------------------------------------------------
# Staging tables: messy, overlapping, inconsistent naming
# ---------------------------------------------------------------------------

def gen_staging_legacy_policies(policies):
    """AS400 dump: ALL_CAPS abbreviated cols, strings for everything, N/A for nulls."""
    rows = []
    for pol in policies:  # includes CDC versions!
        if pol["source_system"] != "LEGACY_AS400" and random.random() > 0.3:
            continue
        rows.append({
            "POL_NBR": pol["policy_number"],
            "INSRD_ID": str(pol["insured_id"]),
            "AGT_CD": f"AGT-{pol['agent_id']:04d}",
            "LOB": pol["line_of_business"],
            "EFF_DT": pol["effective_date"].replace("-", ""),
            "EXP_DT": pol["expiration_date"].replace("-", ""),
            "STATUS": pol["policy_status"][:3],
            "WRT_PREM": str(pol["total_premium"]),
            "EXPO_UNITS": str(pol["total_exposure_units"]),
            "ST": pol["state"],
            "TERR": pol["territory_code"],
            "DEDUCT": str(pol["deductible_amount"]),
            "LMT": str(pol["policy_limit"]),
            "CNCL_DT": (pol["cancellation_date"] or "").replace("-", "") or "N/A",
            "CNCL_RSN": pol["cancellation_reason"] or "N/A",
            "VER_NBR": str(pol["version_number"]),
            "CURR_IND": "Y" if pol["is_current_record"] else "N",
            "DEL_IND": "Y" if pol["is_deleted"] else "N",
            "SYS_ENT_DT": pol["system_entry_date"].replace("-", ""),
            "LOAD_TIMESTAMP": datetime.now().isoformat(),
            "BATCH_ID": f"BATCH-{random.randint(1000, 9999)}",
        })
    return rows


def gen_staging_guidewire_claims(claims):
    """
    Guidewire extract as an activity/event pattern.
    Each claim generates multiple events (FNOL, ASSIGN, RESERVE, PAYMENT, etc.).
    Uses camelCase naming. Contains snapshot duplicates from overlapping extracts.
    """
    rows = []
    event_id = 1
    for claim in claims:
        if claim["source_system"] != "GUIDEWIRE_PC" and random.random() > 0.4:
            continue

        events = ["FNOL", "ASSIGNMENT"]
        if claim["reserve_amount"] > 0:
            events.append("RESERVE_SET")
        if claim["paid_loss_amount"] > 0:
            events.append("PAYMENT")
        if claim["claim_status"] == "CLOSED":
            events.append("CLOSURE")
        if claim["claim_status"] == "REOPENED":
            events.extend(["CLOSURE", "REOPEN"])
        if claim["litigation_flag"]:
            events.append("LITIGATION_REFERRAL")

        for ev_type in events:
            # Overlapping extract snapshots: ~8% of events appear twice
            # with slightly different timestamps (different extract runs)
            num_copies = 2 if random.random() < 0.08 else 1
            for copy in range(num_copies):
                base_ts = rand_date(2020, 2025)
                extract_ts = rand_ts(base_ts + timedelta(
                    days=copy * random.randint(0, 3)))
                rows.append({
                    "eventId": event_id,
                    "claimPublicId": f"GW-{claim['claim_id']:010d}",
                    "externalClaimNumber": claim["claim_number"],
                    "policyNumberRef": claim["policy_number"],
                    "insuredPartyId": claim["insured_id"],
                    "lobCode": claim["line_of_business"],
                    "eventType": ev_type,
                    "eventTimestamp": extract_ts.isoformat(),
                    "dateOfLoss": claim["loss_date"],
                    "dateReported": claim["report_date"],
                    "claimState": (claim["claim_status"].lower()
                                   if random.random() > 0.15
                                   else claim["claim_status"]),
                    "lossCauseCode": claim["cause_of_loss"],
                    "lossDescriptionText": claim["loss_description"],
                    "lossLocationState": claim["loss_state"],
                    "lossLocationZip": claim["loss_zip"],
                    "claimantDisplayName": claim["claimant_name"],
                    "claimantRole": claim["claimant_type"],
                    "assignedAdjusterId": claim["adjuster_id"],
                    "assignedAdjusterName": claim["adjuster_name"],
                    "financials_reserve": claim["reserve_amount"],
                    "financials_paidLoss": claim["paid_loss_amount"],
                    "financials_paidExpense": (
                        claim["paid_alae_amount"] + claim["paid_ulae_amount"]),
                    "financials_salvageSubro": (
                        claim["salvage_amount"] + claim["subrogation_amount"]),
                    "financials_totalIncurred": claim["total_incurred"],
                    "catCode": claim["catastrophe_code"],
                    "isLitigated": "Y" if claim["litigation_flag"] else "N",
                    "siuReferral": "Y" if claim["fraud_indicator"] else "N",
                    "closedDate": claim["close_date"],
                    "isDeleted": claim["is_deleted"],
                    "extractTimestamp": extract_ts.isoformat(),
                    "gwBatchNumber": random.randint(100000, 999999),
                })
                event_id += 1
    return rows


def gen_staging_broker_feed(quotes):
    """Broker submission feed with duplicate submissions and format issues."""
    rows = []
    for quote in random.sample(quotes, min(3000, len(quotes))):
        # ~10% of submissions are duplicated (broker resubmitted)
        num_copies = 2 if random.random() < 0.10 else 1
        for copy in range(num_copies):
            sub_date = quote["quote_date"]
            if copy > 0:
                # Duplicate has slightly different date and premium
                d = datetime.strptime(sub_date, "%Y-%m-%d")
                sub_date = (d + timedelta(
                    days=random.randint(1, 7))).date().isoformat()

            rows.append({
                "submission_id": f"SUB-{random.randint(100000, 999999)}",
                "broker_name": fake.company(),
                "broker_code": fake.bothify("BRK-####"),
                "insured_name": (fake.name() if random.random() > 0.3
                                 else fake.company()),
                "line_of_business": quote["line_of_business"],
                "state": quote["state"],
                "submission_date": sub_date,
                "requested_effective": (
                    datetime.strptime(quote["quote_date"], "%Y-%m-%d") +
                    timedelta(days=random.randint(15, 90))
                ).date().isoformat(),
                "quoted_premium": round(
                    quote["quoted_premium"] * random.uniform(0.90, 1.10), 2),
                "status": quote["status"].lower(),
                "competitor_market": quote.get("competitor_name") or "",
                "decline_notes": quote.get("decline_reason") or "",
                "bound_policy_ref": (
                    f"POL-{quote['line_of_business']}-"
                    f"{quote.get('bound_policy_id', ''):06d}"
                    if quote.get("bound_policy_id") else ""),
                "data_quality_flag": random.choice([
                    "OK", "WARN_MISSING_FIELDS", "WARN_DUPLICATE", ""]),
                "ingestion_ts": datetime.now().isoformat(),
            })
    return rows


def gen_staging_duck_creek_premiums(premium_txns):
    """Duck Creek extract: dollar-formatted amounts, MM/DD/YYYY dates, abbreviated codes."""
    rows = []
    for txn in premium_txns:
        if txn["source_system"] != "DUCK_CREEK" and random.random() > 0.35:
            continue
        # Mix of formatting styles
        r = random.random()
        if r < 0.3:
            amount_str = f"${txn['amount']:,.2f}"
        elif r < 0.5:
            amount_str = f"({abs(txn['amount']):,.2f})" if txn["amount"] < 0 \
                else f"{txn['amount']:,.2f}"
        else:
            amount_str = str(txn["amount"])

        # Date format: mix of ISO and MM/DD/YYYY
        txn_dt = txn["transaction_date"]
        if random.random() < 0.4:
            d = datetime.strptime(txn_dt, "%Y-%m-%d")
            txn_dt = d.strftime("%m/%d/%Y")

        rows.append({
            "dc_transaction_id": f"DC-{random.randint(1000000, 9999999)}",
            "policy_ref": txn["policy_number"],
            "lob": txn["line_of_business"],
            "txn_type_cd": txn["transaction_type"][:4],
            "txn_dt": txn_dt,
            "acctg_dt": txn["accounting_date"],
            "premium_amt": amount_str,
            "acct_period": txn["accounting_period"],
            "risk_state": txn["state"],
            "producer_cd": f"AGT-{txn['agent_id']:04d}",
            "reversal_flag": "Y" if txn["is_reversal"] else "N",
            "reversal_of_id": (str(txn["reversal_of_transaction_id"])
                               if txn["reversal_of_transaction_id"] else ""),
            "load_dt": datetime.now().strftime("%m/%d/%Y"),
            "file_name": (
                f"dc_prem_extract_{random.randint(1, 100):03d}.csv"),
        })
    return rows


def gen_staging_activity_log(policies, claims, premium_txns):
    """
    Generic activity/event log table – insert/update/delete events from
    multiple source systems. Different from guidewire events – this is
    the CDC event stream that powers the core tables.
    """
    events = []
    event_id = 1

    # Policy events
    for pol in policies:
        action = "INSERT" if pol["version_number"] == 1 else "UPDATE"
        if pol["is_deleted"]:
            action = "DELETE"
        events.append({
            "event_id": event_id,
            "entity_type": "POLICY",
            "entity_key": str(pol["policy_id"]),
            "entity_ref": pol["policy_number"],
            "action": action,
            "version": pol["version_number"],
            "source_system": pol["source_system"],
            "event_timestamp": pol["_valid_from"],
            "payload_json": (
                f'{{"status":"{pol["policy_status"]}",'
                f'"premium":{pol["total_premium"]},'
                f'"lob":"{pol["line_of_business"]}"}}'),
            "processed_flag": random.choice(["Y", "N", "Y", "Y"]),
            "error_message": "",
        })
        event_id += 1

    # Claim events
    for claim in claims:
        events.append({
            "event_id": event_id,
            "entity_type": "CLAIM",
            "entity_key": str(claim["claim_id"]),
            "entity_ref": claim["claim_number"],
            "action": "DELETE" if claim["is_deleted"] else "INSERT",
            "version": 1,
            "source_system": claim["source_system"],
            "event_timestamp": claim["created_at"].isoformat(),
            "payload_json": (
                f'{{"status":"{claim["claim_status"]}",'
                f'"loss_date":"{claim["loss_date"]}",'
                f'"lob":"{claim["line_of_business"]}"}}'),
            "processed_flag": random.choice(["Y", "N", "Y", "Y"]),
            "error_message": (
                "PARSE_ERROR: invalid date format"
                if random.random() < 0.02 else ""),
        })
        event_id += 1

    return events


# ---------------------------------------------------------------------------
# Gold Truth Metrics (eval only – computed from CURRENT, NON-DELETED data)
# ---------------------------------------------------------------------------

def compute_gold_metrics(policies, claims, premium_txns, coverages, quotes):
    """
    Compute ground-truth insurance metrics.
    Uses only current, non-deleted policies and non-deleted claims.
    Excludes reversed/void transactions.
    """
    current_policies = get_current_policies(policies)
    active_claims = get_active_claims(claims)

    # Index claims by policy_id
    claims_by_policy = {}
    for c in active_claims:
        claims_by_policy.setdefault(c["policy_id"], []).append(c)

    # Index premium txns by policy_id, exclude reversals
    active_prem = [p for p in premium_txns if not p["is_reversal"]]
    prem_written_by_policy = {}
    prem_earned_by_policy = {}
    for p in active_prem:
        if p["transaction_type"] == "WRITTEN":
            prem_written_by_policy.setdefault(p["policy_id"], []).append(p)
        elif p["transaction_type"] == "EARNED":
            prem_earned_by_policy.setdefault(p["policy_id"], []).append(p)

    # --- LOB x Year summary ---
    lob_year = {}
    for pol in current_policies:
        year = int(pol["effective_date"][:4])
        lob = pol["line_of_business"]
        key = (lob, year)

        if key not in lob_year:
            lob_year[key] = {
                "line_of_business": lob,
                "lob_description": LOB_NAMES[lob],
                "policy_year": year,
                "policy_count": 0,
                "total_exposure_units": 0,
                "written_premium": 0,
                "earned_premium": 0,
                "claim_count": 0,
                "open_claim_count": 0,
                "closed_claim_count": 0,
                "paid_loss": 0,
                "paid_alae": 0,
                "paid_ulae": 0,
                "total_lae": 0,
                "salvage": 0,
                "subrogation": 0,
                "net_incurred_loss": 0,
                "total_incurred": 0,
            }

        m = lob_year[key]
        m["policy_count"] += 1
        m["total_exposure_units"] += pol["total_exposure_units"]

        wp = sum(p["amount"] for p in prem_written_by_policy.get(
            pol["policy_id"], []))
        if wp == 0:
            wp = pol["total_premium"]
        m["written_premium"] += wp

        ep = sum(p["amount"] for p in prem_earned_by_policy.get(
            pol["policy_id"], []))
        if ep == 0:
            ep = wp * random.uniform(0.7, 1.0)
        m["earned_premium"] += ep

        pol_claims = claims_by_policy.get(pol["policy_id"], [])
        m["claim_count"] += len(pol_claims)
        for c in pol_claims:
            if c["claim_status"] in (
                    "OPEN", "REOPENED", "LITIGATION", "SUBROGATION"):
                m["open_claim_count"] += 1
            else:
                m["closed_claim_count"] += 1
            m["paid_loss"] += c["paid_loss_amount"]
            m["paid_alae"] += c["paid_alae_amount"]
            m["paid_ulae"] += c["paid_ulae_amount"]
            m["salvage"] += c["salvage_amount"]
            m["subrogation"] += c["subrogation_amount"]

    gold_lob_year = []
    for key, m in lob_year.items():
        m["total_lae"] = round(m["paid_alae"] + m["paid_ulae"], 2)
        m["net_incurred_loss"] = round(
            m["paid_loss"] - m["salvage"] - m["subrogation"], 2)
        m["total_incurred"] = round(
            m["paid_loss"] + m["paid_alae"] + m["paid_ulae"]
            - m["salvage"] - m["subrogation"], 2)

        wp = m["written_premium"]
        ep = m["earned_premium"]
        exposure = m["total_exposure_units"]
        cnt = m["claim_count"]
        pol_cnt = m["policy_count"]

        m["frequency"] = round(cnt / max(exposure, 1), 6)
        m["severity"] = round(m["net_incurred_loss"] / max(cnt, 1), 2)
        m["pure_premium"] = round(
            m["net_incurred_loss"] / max(exposure, 1), 2)
        m["average_premium"] = round(wp / max(pol_cnt, 1), 2)
        m["loss_ratio"] = round(
            m["net_incurred_loss"] / max(ep, 1), 6)
        m["lae_ratio"] = round(m["total_lae"] / max(ep, 1), 6)
        m["combined_loss_lae_ratio"] = round(
            (m["net_incurred_loss"] + m["total_lae"]) / max(ep, 1), 6)

        for k in ("written_premium", "earned_premium",
                   "total_exposure_units", "paid_loss", "paid_alae",
                   "paid_ulae", "salvage", "subrogation"):
            m[k] = round(m[k], 2)

        gold_lob_year.append(m)

    # --- UW metrics ---
    gold_uw = []
    for m in gold_lob_year:
        uw_expense_ratio = round(random.uniform(0.25, 0.40), 6)
        uw_expense = round(m["written_premium"] * uw_expense_ratio, 2)
        operating_expense = round(uw_expense + m["total_lae"], 2)
        combined = round(
            m["loss_ratio"] + m["lae_ratio"] + uw_expense_ratio, 6)
        uw_profit = round(
            m["earned_premium"] - m["net_incurred_loss"]
            - m["total_lae"] - uw_expense, 2)

        gold_uw.append({
            "line_of_business": m["line_of_business"],
            "lob_description": m["lob_description"],
            "policy_year": m["policy_year"],
            "written_premium": m["written_premium"],
            "earned_premium": m["earned_premium"],
            "net_incurred_loss": m["net_incurred_loss"],
            "total_lae": m["total_lae"],
            "underwriting_expense": uw_expense,
            "underwriting_expense_ratio": uw_expense_ratio,
            "operating_expense": operating_expense,
            "operating_expense_ratio": round(
                operating_expense / max(m["earned_premium"], 1), 6),
            "loss_ratio": m["loss_ratio"],
            "lae_ratio": m["lae_ratio"],
            "combined_ratio": combined,
            "underwriting_profit": uw_profit,
            "underwriting_profit_ratio": round(
                uw_profit / max(m["earned_premium"], 1), 6),
        })

    # --- Quote metrics ---
    qm_by_key = {}
    for q in quotes:
        year = int(q["quote_date"][:4])
        lob = q["line_of_business"]
        key = (lob, year)
        if key not in qm_by_key:
            qm_by_key[key] = {
                "line_of_business": lob,
                "lob_description": LOB_NAMES[lob],
                "quote_year": year,
                "total_quotes": 0, "bound_quotes": 0,
                "declined_quotes": 0, "expired_quotes": 0,
                "lost_quotes": 0,
                "total_quoted_premium": 0, "bound_quoted_premium": 0,
            }
        qm = qm_by_key[key]
        qm["total_quotes"] += 1
        qm["total_quoted_premium"] += q["quoted_premium"]
        if q["status"] == "BOUND":
            qm["bound_quotes"] += 1
            qm["bound_quoted_premium"] += q["quoted_premium"]
        elif q["status"] == "DECLINED":
            qm["declined_quotes"] += 1
        elif q["status"] == "EXPIRED":
            qm["expired_quotes"] += 1
        elif q["status"] == "LOST":
            qm["lost_quotes"] += 1

    gold_quotes = []
    for qm in qm_by_key.values():
        qm["close_ratio"] = round(
            qm["bound_quotes"] / max(qm["total_quotes"], 1), 6)
        qm["average_quoted_premium"] = round(
            qm["total_quoted_premium"] / max(qm["total_quotes"], 1), 2)
        qm["total_quoted_premium"] = round(qm["total_quoted_premium"], 2)
        qm["bound_quoted_premium"] = round(qm["bound_quoted_premium"], 2)
        gold_quotes.append(qm)

    # --- Retention ---
    retention_by_lob = {}
    for pol in current_policies:
        lob = pol["line_of_business"]
        if lob not in retention_by_lob:
            retention_by_lob[lob] = {
                "line_of_business": lob,
                "lob_description": LOB_NAMES[lob],
                "total_policies": 0,
                "renewal_policies": 0,
                "new_policies": 0,
            }
        retention_by_lob[lob]["total_policies"] += 1
        if pol.get("renewal_of_policy_id") is not None:
            retention_by_lob[lob]["renewal_policies"] += 1
        else:
            retention_by_lob[lob]["new_policies"] += 1

    gold_retention = []
    for rm in retention_by_lob.values():
        rm["retention_ratio"] = round(
            rm["renewal_policies"] / max(rm["total_policies"], 1), 6)
        rm["new_business_ratio"] = round(
            rm["new_policies"] / max(rm["total_policies"], 1), 6)
        gold_retention.append(rm)

    return gold_lob_year, gold_uw, gold_quotes, gold_retention


# ---------------------------------------------------------------------------
# DuckDB loader
# ---------------------------------------------------------------------------

def insert_rows(con, schema, table, rows):
    if not rows:
        print(f"  ⚠ {schema}.{table}: 0 rows (skipped)")
        return
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cols = list(rows[0].keys())

    def sql_val(v):
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, datetime):
            return f"'{v.isoformat()}'::TIMESTAMP"
        if isinstance(v, date):
            return f"'{v.isoformat()}'::DATE"
        s = str(v).replace("'", "''")
        return f"'{s}'"

    col_list = ", ".join(f'"{c}"' for c in cols)
    chunk_size = 500
    first = True
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        vals = ",\n".join(
            "(" + ", ".join(sql_val(r[c]) for c in cols) + ")"
            for r in chunk
        )
        if first:
            con.execute(
                f"CREATE OR REPLACE TABLE {schema}.{table} "
                f"AS SELECT * FROM (VALUES\n{vals}\n) AS t({col_list})")
            first = False
        else:
            con.execute(
                f"INSERT INTO {schema}.{table} "
                f"SELECT * FROM (VALUES\n{vals}\n) AS t({col_list})")
    print(f"  ✓ {schema}.{table}: {len(rows):,} rows")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    print("=" * 60)
    print("P&C Insurance Data Generator")
    print("=" * 60)

    # --- Generate data ---
    print("\n[1/8] Generating core entities...")
    agents = gen_agents()
    insureds = gen_insureds()
    policies = gen_policies_with_cdc(insureds, agents)
    current_policies = get_current_policies(policies)
    coverages = gen_coverages(policies)
    claims = gen_claims(policies)
    claim_txns = gen_claim_transactions(claims)
    premium_txns = gen_premium_transactions(policies)
    quotes = gen_quotes(insureds, agents)
    notes = gen_unstructured_notes(claims, policies)

    print(f"    Policies: {len(policies)} rows ({len(current_policies)} current)")
    print(f"    Claims: {len(claims)} ({len(get_active_claims(claims))} active)")

    print("[2/8] Generating staging data...")
    stg_legacy = gen_staging_legacy_policies(policies)
    stg_gw = gen_staging_guidewire_claims(claims)
    stg_broker = gen_staging_broker_feed(quotes)
    stg_dc = gen_staging_duck_creek_premiums(premium_txns)
    stg_activity = gen_staging_activity_log(policies, claims, premium_txns)

    print("[3/8] Computing gold metrics...")
    gold_lob_year, gold_uw, gold_quotes, gold_retention = \
        compute_gold_metrics(policies, claims, premium_txns, coverages, quotes)

    # --- Load into DuckDB ---
    print("[4/8] Loading core schema...")
    con = duckdb.connect(DB_PATH)

    insert_rows(con, "core", "agents", agents)
    insert_rows(con, "core", "insureds", insureds)
    insert_rows(con, "core", "policies", policies)
    insert_rows(con, "core", "coverages", coverages)
    insert_rows(con, "core", "claims", claims)
    insert_rows(con, "core", "claim_transactions", claim_txns)
    insert_rows(con, "core", "premium_transactions", premium_txns)
    insert_rows(con, "core", "quotes", quotes)

    print("\n[5/8] Loading staging + unstructured...")
    insert_rows(con, "unstructured", "notes", notes)
    insert_rows(con, "staging_legacy", "policies_as400", stg_legacy)
    insert_rows(con, "staging_guidewire", "claim_events", stg_gw)
    insert_rows(con, "staging_broker", "submissions_feed", stg_broker)
    insert_rows(con, "staging_duckcreek", "premium_transactions", stg_dc)
    insert_rows(con, "staging_activity", "cdc_event_log", stg_activity)

    print("\n[6/8] Loading gold metrics...")
    insert_rows(con, "gold_metrics", "lob_year_summary", gold_lob_year)
    insert_rows(con, "gold_metrics", "underwriting_metrics", gold_uw)
    insert_rows(con, "gold_metrics", "quote_bind_metrics", gold_quotes)
    insert_rows(con, "gold_metrics", "retention_metrics", gold_retention)

    # --- Mart tables ---
    print("\n[7/8] Creating mart tables...")

    # mart_claims: star schema with fct_ and dim_ prefixes
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS mart_claims;

        -- Dimension: date
        CREATE OR REPLACE TABLE mart_claims.dim_date AS
        SELECT DISTINCT
            d::DATE as date_key,
            EXTRACT(YEAR FROM d::DATE) as cal_year,
            EXTRACT(QUARTER FROM d::DATE) as cal_quarter,
            EXTRACT(MONTH FROM d::DATE) as cal_month,
            MONTHNAME(d::DATE) as month_name,
            DAYOFWEEK(d::DATE) as day_of_week,
            DAYNAME(d::DATE) as day_name,
            CASE WHEN EXTRACT(MONTH FROM d::DATE) <= 6
                 THEN EXTRACT(YEAR FROM d::DATE)
                 ELSE EXTRACT(YEAR FROM d::DATE) + 1
            END as fiscal_year
        FROM generate_series('2018-01-01'::DATE, '2026-12-31'::DATE, INTERVAL 1 DAY) t(d);

        -- Dimension: LOB
        CREATE OR REPLACE TABLE mart_claims.dim_line_of_business AS
        SELECT * FROM (VALUES
            ('HO', 'Homeowners', 'Personal', 'Property'),
            ('AUTO', 'Personal Auto', 'Personal', 'Auto'),
            ('CGL', 'Commercial General Liability', 'Commercial', 'Liability'),
            ('WC', 'Workers Compensation', 'Commercial', 'Liability'),
            ('BOP', 'Business Owners Policy', 'Commercial', 'Package'),
            ('CPP', 'Commercial Package Policy', 'Commercial', 'Package'),
            ('FARM', 'Farmowners', 'Personal', 'Property'),
            ('IM', 'Inland Marine', 'Commercial', 'Property')
        ) AS t(lob_code, lob_name, segment, category);

        -- Dimension: geography
        CREATE OR REPLACE TABLE mart_claims.dim_geography AS
        SELECT DISTINCT
            state as state_code,
            territory_code,
            CASE
                WHEN state IN ('CT','ME','MA','NH','RI','VT') THEN 'New England'
                WHEN state IN ('NJ','NY','PA') THEN 'Middle Atlantic'
                WHEN state IN ('IL','IN','MI','OH','WI') THEN 'East North Central'
                WHEN state IN ('IA','KS','MN','MO','NE','ND','SD') THEN 'West North Central'
                WHEN state IN ('DE','FL','GA','MD','NC','SC','VA','WV') THEN 'South Atlantic'
                WHEN state IN ('AL','KY','MS','TN') THEN 'East South Central'
                WHEN state IN ('AR','LA','OK','TX') THEN 'West South Central'
                WHEN state IN ('AZ','CO','ID','MT','NV','NM','UT','WY') THEN 'Mountain'
                WHEN state IN ('AK','CA','HI','OR','WA') THEN 'Pacific'
                ELSE 'Unknown'
            END as census_region
        FROM core.policies
        WHERE is_current_record = TRUE AND is_deleted = FALSE;

        -- Fact: claims
        CREATE OR REPLACE TABLE mart_claims.fct_claim_detail AS
        SELECT
            c.claim_id,
            c.claim_number,
            c.policy_id,
            c.line_of_business as lob_code,
            c.loss_state as state_code,
            c.loss_date::DATE as loss_date_key,
            c.report_date::DATE as report_date_key,
            c.entry_date::DATE as entry_date_key,
            c.claim_status,
            c.cause_of_loss,
            c.claimant_type,
            c.reserve_amount,
            c.paid_loss_amount,
            c.paid_alae_amount,
            c.paid_ulae_amount,
            c.salvage_amount,
            c.subrogation_amount,
            c.total_incurred,
            c.paid_loss_amount - c.salvage_amount - c.subrogation_amount
                as net_paid_loss,
            c.paid_alae_amount + c.paid_ulae_amount as total_lae,
            c.catastrophe_code,
            c.litigation_flag,
            c.fraud_indicator,
            c.close_date,
            c.reopen_date,
            DATEDIFF('day', c.loss_date::DATE, c.report_date::DATE)
                as report_lag_days,
            DATEDIFF('day', c.report_date::DATE, c.entry_date::DATE)
                as entry_lag_days,
            CASE WHEN c.close_date IS NOT NULL
                 THEN DATEDIFF('day', c.report_date::DATE, c.close_date::DATE)
                 ELSE NULL END as days_to_close
        FROM core.claims c
        WHERE c.is_deleted = FALSE;

        -- Loss triangles
        CREATE OR REPLACE TABLE mart_claims.fct_loss_triangle AS
        SELECT
            line_of_business as lob_code,
            EXTRACT(YEAR FROM loss_date::DATE) as accident_year,
            EXTRACT(YEAR FROM report_date::DATE) as report_year,
            EXTRACT(YEAR FROM report_date::DATE)
                - EXTRACT(YEAR FROM loss_date::DATE) as development_lag,
            COUNT(*) as claim_count,
            SUM(paid_loss_amount) as paid_loss,
            SUM(paid_alae_amount + paid_ulae_amount) as paid_lae,
            SUM(total_incurred) as total_incurred
        FROM core.claims
        WHERE is_deleted = FALSE
        GROUP BY 1, 2, 3;
    """)
    print("  ✓ mart_claims (star schema: dim_date, dim_line_of_business, dim_geography, fct_claim_detail, fct_loss_triangle)")

    # mart_underwriting
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS mart_underwriting;

        CREATE OR REPLACE TABLE mart_underwriting.policy_book AS
        SELECT
            p.policy_id,
            p.policy_number,
            p.line_of_business,
            p.lob_description,
            p.product_code,
            p.effective_date,
            p.expiration_date,
            p.policy_status,
            p.state,
            p.territory_code,
            p.total_premium,
            p.total_exposure_units,
            p.deductible_amount,
            p.policy_limit,
            p.policy_term_months,
            p.version_number,
            a.agent_code,
            a.agency_name,
            a.commission_rate,
            i.insured_type,
            i.credit_score,
            CASE WHEN p.renewal_of_policy_id IS NOT NULL
                 THEN 'RENEWAL' ELSE 'NEW' END as business_type,
            COUNT(DISTINCT cl.claim_id) as claim_count,
            COALESCE(SUM(cl.total_incurred), 0) as total_incurred
        FROM core.policies p
        JOIN core.agents a ON p.agent_id = a.agent_id
        JOIN core.insureds i ON p.insured_id = i.insured_id
        LEFT JOIN core.claims cl ON p.policy_id = cl.policy_id
            AND cl.is_deleted = FALSE
        WHERE p.is_current_record = TRUE AND p.is_deleted = FALSE
        GROUP BY ALL;

        CREATE OR REPLACE TABLE mart_underwriting.rate_adequacy AS
        SELECT
            line_of_business,
            state,
            territory_code,
            EXTRACT(YEAR FROM effective_date::DATE) as policy_year,
            COUNT(*) as policy_count,
            SUM(total_exposure_units) as total_exposure,
            SUM(total_premium) as total_written_premium,
            AVG(total_premium) as avg_premium,
            AVG(deductible_amount) as avg_deductible,
            AVG(policy_limit) as avg_limit
        FROM core.policies
        WHERE is_current_record = TRUE AND is_deleted = FALSE
        GROUP BY 1, 2, 3, 4;
    """)
    print("  ✓ mart_underwriting.policy_book, mart_underwriting.rate_adequacy")

    # mart_finance
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS mart_finance;

        CREATE OR REPLACE TABLE mart_finance.premium_journal AS
        SELECT
            pt.transaction_id,
            pt.policy_id,
            pt.policy_number,
            pt.line_of_business,
            pt.transaction_type,
            pt.transaction_date,
            pt.accounting_date,
            pt.booking_date,
            pt.effective_date,
            pt.amount,
            pt.accounting_period,
            pt.state,
            pt.is_reversal,
            pt.reversal_of_transaction_id,
            a.agent_code,
            a.agency_name,
            a.commission_rate,
            ROUND(pt.amount * a.commission_rate, 2) as commission_amount
        FROM core.premium_transactions pt
        JOIN core.policies p ON pt.policy_id = p.policy_id
            AND p.is_current_record = TRUE AND p.is_deleted = FALSE
        JOIN core.agents a ON p.agent_id = a.agent_id;

        CREATE OR REPLACE TABLE mart_finance.monthly_financials AS
        SELECT
            pt.accounting_period,
            pt.line_of_business,
            pt.state,
            SUM(CASE WHEN pt.transaction_type = 'WRITTEN' AND NOT pt.is_reversal
                     THEN pt.amount ELSE 0 END) as written_premium,
            SUM(CASE WHEN pt.transaction_type = 'EARNED' AND NOT pt.is_reversal
                     THEN pt.amount ELSE 0 END) as earned_premium,
            SUM(CASE WHEN pt.transaction_type = 'CEDED' AND NOT pt.is_reversal
                     THEN pt.amount ELSE 0 END) as ceded_premium,
            SUM(CASE WHEN pt.transaction_type = 'RETURN' AND NOT pt.is_reversal
                     THEN pt.amount ELSE 0 END) as return_premium,
            SUM(CASE WHEN pt.transaction_type = 'ENDORSEMENT' AND NOT pt.is_reversal
                     THEN pt.amount ELSE 0 END) as endorsement_premium,
            SUM(CASE WHEN pt.transaction_type = 'REVERSAL'
                     THEN pt.amount ELSE 0 END) as reversals
        FROM core.premium_transactions pt
        GROUP BY 1, 2, 3;
    """)
    print("  ✓ mart_finance.premium_journal, mart_finance.monthly_financials")

    # mart_agency
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS mart_agency;

        CREATE OR REPLACE TABLE mart_agency.agent_performance AS
        SELECT
            a.agent_id,
            a.agent_code,
            a.agency_name,
            a.first_name || ' ' || a.last_name as agent_name,
            a.license_state,
            a.commission_rate,
            COUNT(DISTINCT p.policy_id) as policies_written,
            SUM(p.total_premium) as total_premium_written,
            AVG(p.total_premium) as avg_premium,
            COUNT(DISTINCT c.claim_id) as claims_on_book,
            COALESCE(SUM(c.total_incurred), 0) as total_incurred_on_book,
            COUNT(DISTINCT q.quote_id) as total_quotes,
            COUNT(DISTINCT CASE WHEN q.status = 'BOUND'
                                THEN q.quote_id END) as bound_quotes,
            ROUND(COUNT(DISTINCT CASE WHEN q.status = 'BOUND'
                                      THEN q.quote_id END)::FLOAT
                  / NULLIF(COUNT(DISTINCT q.quote_id), 0), 4) as close_ratio
        FROM core.agents a
        LEFT JOIN core.policies p ON a.agent_id = p.agent_id
            AND p.is_current_record = TRUE AND p.is_deleted = FALSE
        LEFT JOIN core.claims c ON p.policy_id = c.policy_id
            AND c.is_deleted = FALSE
        LEFT JOIN core.quotes q ON a.agent_id = q.agent_id
        GROUP BY ALL;
    """)
    print("  ✓ mart_agency.agent_performance")

    # mart_actuarial: star schema with conformed dimensions
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS mart_actuarial;

        CREATE OR REPLACE TABLE mart_actuarial.dim_policy AS
        SELECT
            policy_id as policy_key,
            policy_number,
            line_of_business,
            lob_description,
            product_code,
            state as risk_state,
            territory_code,
            policy_term_months,
            deductible_amount,
            policy_limit,
            effective_date::DATE as effective_dt,
            expiration_date::DATE as expiration_dt,
            total_exposure_units as exposure
        FROM core.policies
        WHERE is_current_record = TRUE AND is_deleted = FALSE;

        CREATE OR REPLACE TABLE mart_actuarial.fct_earned_premium AS
        SELECT
            pt.policy_id as policy_key,
            pt.line_of_business,
            pt.state as risk_state,
            pt.accounting_period,
            pt.transaction_type,
            pt.amount,
            pt.is_reversal,
            p.effective_date::DATE as policy_effective_dt,
            p.total_exposure_units as exposure
        FROM core.premium_transactions pt
        JOIN core.policies p ON pt.policy_id = p.policy_id
            AND p.is_current_record = TRUE AND p.is_deleted = FALSE;

        CREATE OR REPLACE TABLE mart_actuarial.fct_incurred_loss AS
        SELECT
            c.claim_id,
            c.policy_id as policy_key,
            c.line_of_business,
            c.loss_state as risk_state,
            c.loss_date::DATE as accident_dt,
            c.report_date::DATE as report_dt,
            EXTRACT(YEAR FROM c.loss_date::DATE) as accident_year,
            c.paid_loss_amount,
            c.paid_alae_amount,
            c.paid_ulae_amount,
            c.salvage_amount,
            c.subrogation_amount,
            c.paid_loss_amount - c.salvage_amount - c.subrogation_amount
                as net_incurred,
            c.paid_alae_amount + c.paid_ulae_amount as total_lae,
            c.reserve_amount
        FROM core.claims c
        WHERE c.is_deleted = FALSE;
    """)
    print("  ✓ mart_actuarial (dim_policy, fct_earned_premium, fct_incurred_loss)")

    # mart_executive: One Big Table (OBT) – denormalized everything
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS mart_executive;

        CREATE OR REPLACE TABLE mart_executive.obt_policy_claims_premium AS
        SELECT
            p.policy_id,
            p.policy_number,
            p.version_number,
            p.is_current_record,
            p.is_deleted as policy_is_deleted,
            p.line_of_business,
            p.lob_description,
            p.product_code,
            p.effective_date,
            p.expiration_date,
            p.binding_date,
            p.issue_date,
            p.system_entry_date,
            p.booking_date,
            p.policy_status,
            p.state,
            p.territory_code,
            p.total_premium,
            p.total_exposure_units,
            p.deductible_amount,
            p.policy_limit,
            p.cancellation_date,
            p.cancellation_reason,
            CASE WHEN p.renewal_of_policy_id IS NOT NULL
                 THEN 'RENEWAL' ELSE 'NEW_BUSINESS' END as business_origin,
            p.source_system as policy_source,
            a.agent_id,
            a.agent_code,
            a.agency_name,
            a.commission_rate,
            i.insured_id,
            i.insured_type,
            CASE WHEN i.insured_type = 'COMMERCIAL'
                 THEN i.company_name
                 ELSE i.first_name || ' ' || i.last_name
            END as insured_display_name,
            i.state as insured_state,
            i.credit_score,
            c.claim_id,
            c.claim_number,
            c.loss_date,
            c.report_date,
            c.entry_date as claim_entry_date,
            c.claim_status,
            c.cause_of_loss,
            c.paid_loss_amount,
            c.paid_alae_amount,
            c.paid_ulae_amount,
            c.salvage_amount,
            c.subrogation_amount,
            c.total_incurred,
            c.catastrophe_code,
            c.litigation_flag,
            c.is_deleted as claim_is_deleted
        FROM core.policies p
        JOIN core.agents a ON p.agent_id = a.agent_id
        JOIN core.insureds i ON p.insured_id = i.insured_id
        LEFT JOIN core.claims c ON p.policy_id = c.policy_id;
    """)
    print("  ✓ mart_executive.obt_policy_claims_premium (OBT – includes ALL versions + deleted)")

    # --- Data quality issues ---
    print("\n[8/8] Creating data quality log...")
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS data_quality;

        CREATE OR REPLACE TABLE data_quality.known_issues AS
        SELECT * FROM (VALUES
            ('core.policies', 'CDC_VERSIONING', 'Table contains multiple versions per policy_id. Filter on is_current_record=TRUE and is_deleted=FALSE for current state.', 'DOCUMENTED', CURRENT_DATE),
            ('core.claims', 'SOFT_DELETES', 'Some claims have is_deleted=TRUE. Must be excluded from metrics.', 'DOCUMENTED', CURRENT_DATE),
            ('core.claim_transactions', 'VOID_TRANSACTIONS', 'Contains VOID transaction_type rows that reverse prior payments. Must be netted out.', 'DOCUMENTED', CURRENT_DATE),
            ('core.premium_transactions', 'REVERSAL_TRANSACTIONS', 'Contains REVERSAL transaction_type with is_reversal=TRUE. Must be excluded or netted.', 'DOCUMENTED', CURRENT_DATE),
            ('core.premium_transactions', 'MULTIPLE_TIME_DIMS', 'transaction_date, accounting_date, booking_date, effective_date have different semantics. Use accounting_date for financial reporting.', 'DOCUMENTED', CURRENT_DATE),
            ('staging_legacy.policies_as400', 'ORPHAN_RECORDS', 'Contains records with no matching insured in core.', 'OPEN', CURRENT_DATE),
            ('staging_guidewire.claim_events', 'DUPLICATE_EVENTS', 'Overlapping extract snapshots produce ~8% duplicate events.', 'OPEN', CURRENT_DATE),
            ('staging_guidewire.claim_events', 'CASE_INCONSISTENCY', 'claimState field mixes upper and lower case values.', 'OPEN', CURRENT_DATE),
            ('staging_duckcreek.premium_transactions', 'FORMAT_INCONSISTENCY', 'premium_amt has mixed formats: plain numbers, $-formatted, and accounting notation with parentheses for negatives.', 'OPEN', CURRENT_DATE),
            ('staging_duckcreek.premium_transactions', 'DATE_FORMAT_MIX', 'txn_dt mixes ISO (YYYY-MM-DD) and US (MM/DD/YYYY) date formats.', 'OPEN', CURRENT_DATE),
            ('staging_broker.submissions_feed', 'DUPLICATE_SUBMISSIONS', '~10% of submissions appear twice with slightly different dates and premiums.', 'OPEN', CURRENT_DATE),
            ('staging_activity.cdc_event_log', 'UNPROCESSED_EVENTS', 'Some events have processed_flag=N and may not be reflected in core tables.', 'OPEN', CURRENT_DATE),
            ('mart_executive.obt_policy_claims_premium', 'ALL_VERSIONS_INCLUDED', 'OBT includes non-current CDC versions and soft-deleted records. Must filter for reporting.', 'DOCUMENTED', CURRENT_DATE)
        ) AS t(table_name, issue_type, description, status, identified_date);
    """)
    print("  ✓ data_quality.known_issues")

    # Add orphan records to staging_legacy
    con.execute("""
        INSERT INTO staging_legacy.policies_as400
        SELECT
            'POL-ORPHAN-' || i::VARCHAR, 'UNKNOWN', 'AGT-9999', 'UNK',
            '20200101', '20210101', 'ACT', '0.00', '0.00', 'XX', 'T00',
            '0', '0', 'N/A', 'N/A', '1', 'Y', 'N', '20200101',
            CURRENT_TIMESTAMP::VARCHAR,
            'BATCH-ORPHAN'
        FROM generate_series(1, 75) t(i);
    """)
    print("  ✓ Added 75 orphan staging records")

    # --- Summary ---
    print("\n" + "=" * 60)
    print("DATABASE SUMMARY")
    print("=" * 60)
    tables = con.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY 1, 2
    """).fetchall()
    print(f"Total tables: {len(tables)}\n")
    for schema, table in tables:
        cnt = con.execute(
            f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
        print(f"  {schema}.{table}: {cnt:,} rows")

    con.close()
    print(f"\n✅ Database written to: {DB_PATH}")


if __name__ == "__main__":
    main()
