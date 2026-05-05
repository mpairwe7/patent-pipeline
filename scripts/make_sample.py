"""Generate a realistic sample of PatentsView-shaped TSVs.

The real USPTO PatentsView disambiguated dataset is multiple gigabytes. This
script produces a small, deterministic sample that matches the **2025-era**
PatentsView column layout exactly, so the pipeline runs identically on the
sample and on the real download.

Output: data/sample/*.tsv (5 files, ~5,000 patents at scale 1).

Column layout (matches data.patentsview.org 2025):
  g_patent                 -> patent_id, patent_type, patent_date, patent_title, patent_abstract,
                              wipo_kind, num_claims, withdrawn, filename
  g_inventor_disambiguated -> patent_id, inventor_sequence, inventor_id,
                              disambig_inventor_name_first, disambig_inventor_name_last,
                              gender_code, location_id  (one row per (patent, inventor))
  g_assignee_disambiguated -> patent_id, assignee_sequence, assignee_id,
                              disambig_assignee_individual_name_first,
                              disambig_assignee_individual_name_last,
                              disambig_assignee_organization, assignee_type, location_id
                              (one row per (patent, assignee))
  g_location_disambiguated -> location_id, disambig_city, disambig_state, disambig_country, ...
  g_cpc_current            -> patent_id, cpc_sequence, cpc_section, cpc_class, cpc_subclass,
                              cpc_group, cpc_type
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 20260413
# 5,000 patents spread across 2010-01-01 → 2025-09-30 (≈ 16-year span,
# inclusive of the full previous decade 2010-2020 the assignment asks for).
# Pass --scale N on the CLI to multiply the volume; useful for stress-
# testing the chunked DuckDB pipeline at 50k+ rows without downloading
# the real PatentsView bundle.
N_PATENTS_BASE = 5_000
START_DATE = date(2010, 1, 1)
END_DATE = date(2025, 9, 30)

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"

COUNTRIES = [
    ("US", 0.48),
    ("CN", 0.18),
    ("JP", 0.09),
    ("KR", 0.06),
    ("DE", 0.05),
    ("GB", 0.03),
    ("FR", 0.02),
    ("CA", 0.02),
    ("IN", 0.02),
    ("TW", 0.02),
    ("NL", 0.01),
    ("CH", 0.01),
    ("SE", 0.01),
]

COMPANIES = [
    "International Business Machines Corporation",
    "Samsung Electronics Co., Ltd.",
    "Canon Inc.",
    "Microsoft Technology Licensing, LLC",
    "Intel Corporation",
    "Huawei Technologies Co., Ltd.",
    "Apple Inc.",
    "Google LLC",
    "Qualcomm Incorporated",
    "Sony Group Corporation",
    "Toyota Motor Corporation",
    "LG Electronics Inc.",
    "Panasonic Holdings Corporation",
    "TSMC Limited",
    "Tencent Technology (Shenzhen) Company Ltd.",
    "Amazon Technologies, Inc.",
    "Meta Platforms, Inc.",
    "Boeing Company, The",
    "General Electric Company",
    "Siemens Aktiengesellschaft",
    "Robert Bosch GmbH",
    "Ford Global Technologies, LLC",
    "NVIDIA Corporation",
    "Micron Technology, Inc.",
    "Hitachi, Ltd.",
    "BYD Company Limited",
    "Dell Products L.P.",
    "Cisco Technology, Inc.",
    "Oracle International Corporation",
    "SAP SE",
    "ASML Netherlands B.V.",
    "Tesla, Inc.",
]

FIRST_NAMES = [
    "John",
    "Alice",
    "Wei",
    "Liu",
    "Rajesh",
    "Priya",
    "Hiroshi",
    "Yuki",
    "Chen",
    "Min",
    "Carlos",
    "Sofia",
    "Ahmed",
    "Fatima",
    "Emma",
    "Olivia",
    "Noah",
    "Liam",
    "Santosh",
    "Anika",
    "Jin",
    "Seo-yun",
    "Hans",
    "Greta",
    "Pierre",
    "Camille",
    "Oliver",
    "Amelia",
    "Kenji",
    "Sakura",
    "Marco",
    "Isabella",
    "Ivan",
    "Natasha",
    "Mohammed",
    "Aisha",
    "Luca",
    "Giulia",
]

LAST_NAMES = [
    "Smith",
    "Johnson",
    "Zhang",
    "Wang",
    "Patel",
    "Kumar",
    "Sato",
    "Tanaka",
    "Li",
    "Chen",
    "Garcia",
    "Rodriguez",
    "Hassan",
    "Khan",
    "Brown",
    "Davis",
    "Miller",
    "Wilson",
    "Sharma",
    "Gupta",
    "Kim",
    "Park",
    "Müller",
    "Schmidt",
    "Dupont",
    "Martin",
    "Taylor",
    "Anderson",
    "Yamamoto",
    "Suzuki",
    "Rossi",
    "Ferrari",
    "Ivanov",
    "Petrov",
    "Ali",
    "Ahmed",
    "Conti",
    "Esposito",
]

TECH_AREAS = [
    ("G", "Machine-learning", "classifier", "neural network"),
    ("H", "5G modulation", "wireless", "signal processing"),
    ("A", "Pharmaceutical", "compound", "dosage"),
    ("B", "Additive manufacturing", "3D-printed", "lattice"),
    ("C", "Battery electrolyte", "lithium-ion", "cathode"),
    ("F", "Wind turbine", "blade pitch", "generator"),
    ("E", "Solar-panel mounting", "inverter", "tracker"),
    ("D", "Textile", "weaving", "stitch pattern"),
    ("Y", "Sustainable", "carbon-capture", "recycling"),
    ("G", "Autonomous-driving", "LiDAR", "sensor fusion"),
    ("H", "Quantum-computing", "qubit", "gate"),
    ("G", "Blockchain", "distributed ledger", "smart contract"),
]

CPC_SECTIONS = {
    "A": ("HUMAN NECESSITIES", ["61", "23", "01"]),
    "B": ("PERFORMING OPERATIONS; TRANSPORTING", ["33", "60", "29"]),
    "C": ("CHEMISTRY; METALLURGY", ["07", "12", "01"]),
    "D": ("TEXTILES; PAPER", ["06", "21"]),
    "E": ("FIXED CONSTRUCTIONS", ["02", "04", "21"]),
    "F": ("MECHANICAL ENGINEERING", ["03", "16", "02"]),
    "G": ("PHYSICS", ["06", "01", "02", "10"]),
    "H": ("ELECTRICITY", ["01", "02", "04"]),
    "Y": ("GENERAL TAGGING", ["02", "10"]),
}


def weighted_choice(rng: random.Random, items: list[tuple[str, float]]) -> str:
    r = rng.random()
    acc = 0.0
    for value, weight in items:
        acc += weight
        if r <= acc:
            return value
    return items[-1][0]


def main(scale: float = 1.0, start_date: date = START_DATE, end_date: date = END_DATE) -> None:
    rng = random.Random(SEED)
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    n_patents = int(N_PATENTS_BASE * scale)
    # Pre-generate pools — scaled with the larger patent volume.
    n_inventors = max(int(6_000 * scale), 6_000)
    n_companies = len(COMPANIES)
    n_locations = max(int(600 * scale), 600)

    # Inventors
    inventors = []
    for i in range(n_inventors):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        inv_id = f"fl:{first.lower()}_ln:{last.lower()}-{i:05d}"
        inventors.append(
            {
                "inventor_id": inv_id,
                "first": first,
                "last": last,
                "male_flag": rng.choice(["", "0", "1"]),
            }
        )

    # Locations (one per inventor/company grouped by country)
    locations = []
    for i in range(n_locations):
        country = weighted_choice(rng, COUNTRIES)
        loc_id = f"loc-{i:04d}"
        locations.append(
            {
                "location_id": loc_id,
                "country": country,
                "state": ""
                if country != "US"
                else rng.choice(["CA", "NY", "TX", "WA", "MA", "IL"]),
                "city": rng.choice(
                    [
                        "San Jose",
                        "New York",
                        "Shanghai",
                        "Tokyo",
                        "Seoul",
                        "Munich",
                        "London",
                        "Paris",
                        "Toronto",
                        "Bengaluru",
                        "Taipei",
                        "Amsterdam",
                        "Zurich",
                        "Stockholm",
                    ]
                ),
            }
        )

    # Companies (assignees)
    companies = []
    for idx, name in enumerate(COMPANIES):
        companies.append(
            {
                "assignee_id": f"org:{idx:04d}",
                "organization": name,
            }
        )

    # Patents
    patents = []
    pat_inv_rows = []
    pat_asn_rows = []
    pat_cpc_rows = []

    date_range_days = (end_date - start_date).days

    for i in range(n_patents):
        patent_id = f"{11000000 + i}"
        # Skew the date distribution slightly toward more recent years to
        # mimic the long-tail growth in real USPTO grants.
        offset = int(rng.triangular(0, date_range_days, date_range_days * 0.65))
        pdate = start_date + timedelta(days=offset)

        _section_code, theme, kw1, kw2 = rng.choice(TECH_AREAS)
        title = f"{theme} system with {kw1} and {kw2} — method #{i:04d}"
        abstract = (
            f"Embodiments describe a {theme.lower()} apparatus using {kw1} to improve {kw2} "
            f"efficiency. The invention comprises a plurality of modules adapted to receive "
            f"input signals and generate optimised outputs. Implementation #{i:04d}."
        )

        patents.append(
            {
                "patent_id": patent_id,
                "patent_date": pdate.isoformat(),
                "patent_title": title,
                "patent_abstract": abstract,
                "patent_type": "utility",
                "num_claims": rng.randint(5, 45),
            }
        )

        # 1–4 inventors per patent — emit one row in g_inventor_disambiguated.tsv
        # per (patent, inventor), matching the modern PatentsView schema where
        # the disambiguated link table inlines all inventor info.
        k_inv = rng.choices([1, 2, 3, 4, 5], weights=[30, 35, 20, 10, 5])[0]
        inv_sample = rng.sample(inventors, k_inv)
        for seq, inv in enumerate(inv_sample, start=1):
            loc = rng.choice(locations)
            pat_inv_rows.append(
                {
                    "patent_id": patent_id,
                    "inventor_sequence": seq,
                    "inventor_id": inv["inventor_id"],
                    "disambig_inventor_name_first": inv["first"],
                    "disambig_inventor_name_last": inv["last"],
                    "gender_code": inv["male_flag"],
                    "location_id": loc["location_id"],
                }
            )

        # 0–2 assignees per patent (some patents have no assignee).
        k_asn = rng.choices([0, 1, 2], weights=[5, 85, 10])[0]
        if k_asn:
            asn_sample = rng.sample(companies, k_asn)
            for seq, asn in enumerate(asn_sample, start=1):
                loc = rng.choice(locations)
                pat_asn_rows.append(
                    {
                        "patent_id": patent_id,
                        "assignee_sequence": seq,
                        "assignee_id": asn["assignee_id"],
                        "disambig_assignee_individual_name_first": "",
                        "disambig_assignee_individual_name_last": "",
                        "disambig_assignee_organization": asn["organization"],
                        "assignee_type": "2",
                        "location_id": loc["location_id"],
                    }
                )

        # 1–3 CPC classifications per patent.
        k_cpc = rng.choices([1, 2, 3], weights=[50, 35, 15])[0]
        sections_used = rng.sample(list(CPC_SECTIONS.keys()), k_cpc)
        for seq, sec in enumerate(sections_used, start=1):
            cls_options = CPC_SECTIONS[sec][1]
            cls = rng.choice(cls_options)
            sub = rng.choice(["A", "B", "C", "D", "K", "N"])
            pat_cpc_rows.append(
                {
                    "patent_id": patent_id,
                    "cpc_sequence": seq,
                    "cpc_section": sec,
                    "cpc_class": f"{sec}{cls}",
                    "cpc_subclass": f"{sec}{cls}{sub}",
                    "cpc_group": f"{sec}{cls}{sub}/{rng.randint(1, 99):02d}",
                    "cpc_type": "inventive",
                }
            )

    # Introduce a few messy cases the cleaner must handle.
    if patents:
        patents[0]["patent_title"] = "   Trimmed    title with inner   spacing   "
        patents[1]["patent_date"] = ""  # missing date
        patents[2]["patent_abstract"] = ""  # missing abstract
    if pat_inv_rows:
        pat_inv_rows[0]["inventor_id"] = ""  # must be dropped

    # ------------------------------------------------------------------
    # Write the 5 TSVs that match the real PatentsView 2025 layout.
    # ------------------------------------------------------------------
    _write_tsv(
        SAMPLE_DIR / "g_patent.tsv",
        patents,
        [
            "patent_id",
            "patent_type",
            "patent_date",
            "patent_title",
            "patent_abstract",
            "wipo_kind",
            "num_claims",
            "withdrawn",
            "filename",
        ],
    )
    _write_tsv(
        SAMPLE_DIR / "g_inventor_disambiguated.tsv",
        pat_inv_rows,
        [
            "patent_id",
            "inventor_sequence",
            "inventor_id",
            "disambig_inventor_name_first",
            "disambig_inventor_name_last",
            "gender_code",
            "location_id",
        ],
    )
    _write_tsv(
        SAMPLE_DIR / "g_assignee_disambiguated.tsv",
        pat_asn_rows,
        [
            "patent_id",
            "assignee_sequence",
            "assignee_id",
            "disambig_assignee_individual_name_first",
            "disambig_assignee_individual_name_last",
            "disambig_assignee_organization",
            "assignee_type",
            "location_id",
        ],
    )
    _write_tsv(
        SAMPLE_DIR / "g_location_disambiguated.tsv",
        [
            {
                "location_id": loc["location_id"],
                "disambig_city": loc["city"],
                "disambig_state": loc["state"],
                "disambig_country": loc["country"],
                "latitude": "",
                "longitude": "",
                "county": "",
                "state_fips": "",
                "county_fips": "",
            }
            for loc in locations
        ],
        [
            "location_id",
            "disambig_city",
            "disambig_state",
            "disambig_country",
            "latitude",
            "longitude",
            "county",
            "state_fips",
            "county_fips",
        ],
    )
    _write_tsv(
        SAMPLE_DIR / "g_cpc_current.tsv",
        pat_cpc_rows,
        [
            "patent_id",
            "cpc_sequence",
            "cpc_section",
            "cpc_class",
            "cpc_subclass",
            "cpc_group",
            "cpc_type",
        ],
    )

    print(
        f"Wrote {n_patents} patents, {n_inventors} inventors, {n_companies} companies "
        f"to {SAMPLE_DIR}"
    )
    print(f"  g_inventor_disambiguated rows: {len(pat_inv_rows):>6}")
    print(f"  g_assignee_disambiguated rows: {len(pat_asn_rows):>6}")
    print(f"  g_cpc_current rows:            {len(pat_cpc_rows):>6}")


def _write_tsv(path: Path, rows: list[dict], columns: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=columns, delimiter="\t", quoting=csv.QUOTE_MINIMAL, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="Volume multiplier (1.0 = 5k patents · 10.0 = 50k · 50.0 = 250k).",
    )
    p.add_argument("--year-from", type=int, default=START_DATE.year, help="Earliest filing year.")
    p.add_argument("--year-to", type=int, default=END_DATE.year, help="Latest filing year.")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(
        scale=args.scale,
        start_date=date(args.year_from, 1, 1),
        end_date=date(args.year_to, 9, 30),
    )
