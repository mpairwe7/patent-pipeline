"""Generate a realistic sample of PatentsView-shaped TSVs.

The real USPTO PatentsView disambiguated dataset is multiple gigabytes. This
script produces a small, deterministic sample that matches the real column
layout so the pipeline can be run end-to-end without downloading anything.

Output: data/sample/*.tsv (7 files, ~1,000 patents).

Column layout mirrors the PatentsView "g_*" bulk files:
  g_patent                 -> patent_id, patent_date, patent_title, patent_abstract, patent_type, ...
  g_inventor_disambiguated -> inventor_id, disambig_inventor_name_first, disambig_inventor_name_last, ...
  g_assignee_disambiguated -> assignee_id, disambig_assignee_organization, ...
  g_location_disambiguated -> location_id, disambig_country, disambig_state, disambig_city, ...
  g_patent_inventor        -> patent_id, inventor_id, location_id
  g_patent_assignee        -> patent_id, assignee_id, location_id
  g_cpc_current            -> patent_id, cpc_section, cpc_class, cpc_subclass, cpc_group, cpc_subgroup
"""

from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 20260413
N_PATENTS = 1_000

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


def main() -> None:
    rng = random.Random(SEED)
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Pre-generate pools
    n_inventors = 1_800
    n_companies = len(COMPANIES)
    n_locations = 200

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

    start_date = date(2020, 1, 1)
    end_date = date(2025, 9, 30)
    date_range_days = (end_date - start_date).days

    for i in range(N_PATENTS):
        patent_id = f"{11000000 + i}"
        # Skew the date distribution slightly toward more recent years
        offset = int(rng.triangular(0, date_range_days, date_range_days * 0.7))
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

        # 1–4 inventors per patent
        k_inv = rng.choices([1, 2, 3, 4, 5], weights=[30, 35, 20, 10, 5])[0]
        inv_sample = rng.sample(inventors, k_inv)
        for inv in inv_sample:
            loc = rng.choice(locations)
            pat_inv_rows.append(
                {
                    "patent_id": patent_id,
                    "inventor_id": inv["inventor_id"],
                    "location_id": loc["location_id"],
                }
            )

        # 0–2 assignees per patent (some patents have no assignee)
        k_asn = rng.choices([0, 1, 2], weights=[5, 85, 10])[0]
        if k_asn:
            asn_sample = rng.sample(companies, k_asn)
            for asn in asn_sample:
                loc = rng.choice(locations)
                pat_asn_rows.append(
                    {
                        "patent_id": patent_id,
                        "assignee_id": asn["assignee_id"],
                        "location_id": loc["location_id"],
                    }
                )

        # 1–3 CPC classifications per patent
        k_cpc = rng.choices([1, 2, 3], weights=[50, 35, 15])[0]
        sections_used = rng.sample(list(CPC_SECTIONS.keys()), k_cpc)
        for sec in sections_used:
            cls_options = CPC_SECTIONS[sec][1]
            cls = rng.choice(cls_options)
            sub = rng.choice(["A", "B", "C", "D", "K", "N"])
            pat_cpc_rows.append(
                {
                    "patent_id": patent_id,
                    "cpc_section": sec,
                    "cpc_class": f"{sec}{cls}",
                    "cpc_subclass": f"{sec}{cls}{sub}",
                    "cpc_group": f"{sec}{cls}{sub}/{rng.randint(1, 99):02d}",
                    "cpc_subgroup": f"{sec}{cls}{sub}/{rng.randint(100, 999)}",
                }
            )

    # Introduce a few messy cases the cleaner must handle
    if patents:
        patents[0]["patent_title"] = "   Trimmed    title with inner   spacing   "
        patents[1]["patent_date"] = ""  # missing date
        patents[2]["patent_abstract"] = ""  # missing abstract
    if pat_inv_rows:
        pat_inv_rows[0]["inventor_id"] = ""  # must be dropped

    # Write TSVs with real PatentsView column names
    _write_tsv(
        SAMPLE_DIR / "g_patent.tsv",
        patents,
        [
            "patent_id",
            "patent_date",
            "patent_title",
            "patent_abstract",
            "patent_type",
            "num_claims",
        ],
    )
    _write_tsv(
        SAMPLE_DIR / "g_inventor_disambiguated.tsv",
        [
            {
                "inventor_id": inv["inventor_id"],
                "disambig_inventor_name_first": inv["first"],
                "disambig_inventor_name_last": inv["last"],
                "male_flag": inv["male_flag"],
            }
            for inv in inventors
        ],
        ["inventor_id", "disambig_inventor_name_first", "disambig_inventor_name_last", "male_flag"],
    )
    _write_tsv(
        SAMPLE_DIR / "g_assignee_disambiguated.tsv",
        [
            {
                "assignee_id": c["assignee_id"],
                "disambig_assignee_organization": c["organization"],
                "disambig_assignee_individual_name_first": "",
                "disambig_assignee_individual_name_last": "",
                "assignee_type": "2",
            }
            for c in companies
        ],
        [
            "assignee_id",
            "disambig_assignee_organization",
            "disambig_assignee_individual_name_first",
            "disambig_assignee_individual_name_last",
            "assignee_type",
        ],
    )
    _write_tsv(
        SAMPLE_DIR / "g_location_disambiguated.tsv",
        [
            {
                "location_id": loc["location_id"],
                "disambig_country": loc["country"],
                "disambig_state": loc["state"],
                "disambig_city": loc["city"],
                "latitude": "",
                "longitude": "",
            }
            for loc in locations
        ],
        [
            "location_id",
            "disambig_country",
            "disambig_state",
            "disambig_city",
            "latitude",
            "longitude",
        ],
    )
    _write_tsv(
        SAMPLE_DIR / "g_patent_inventor.tsv",
        pat_inv_rows,
        ["patent_id", "inventor_id", "location_id"],
    )
    _write_tsv(
        SAMPLE_DIR / "g_patent_assignee.tsv",
        pat_asn_rows,
        ["patent_id", "assignee_id", "location_id"],
    )
    _write_tsv(
        SAMPLE_DIR / "g_cpc_current.tsv",
        pat_cpc_rows,
        ["patent_id", "cpc_section", "cpc_class", "cpc_subclass", "cpc_group", "cpc_subgroup"],
    )

    print(
        f"Wrote {N_PATENTS} patents, {n_inventors} inventors, {n_companies} companies "
        f"to {SAMPLE_DIR}"
    )
    print(f"  g_patent_inventor rows:  {len(pat_inv_rows):>6}")
    print(f"  g_patent_assignee rows:  {len(pat_asn_rows):>6}")
    print(f"  g_cpc_current rows:      {len(pat_cpc_rows):>6}")


def _write_tsv(path: Path, rows: list[dict], columns: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=columns, delimiter="\t", quoting=csv.QUOTE_MINIMAL, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
