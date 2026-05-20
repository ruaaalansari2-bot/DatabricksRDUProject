"""
Geographic scope configuration for the RDU Market Intelligence Lakehouse.

DESIGN PRINCIPLE — "RDU now, NC-scalable later":
The pipeline is keyed on standard FIPS county codes, which are stable and
identical for every county in North Carolina (and the US). To expand from
the Raleigh-Durham CSA to all of NC, you change ONLY the ACTIVE_SCOPE
assignment below. No schema, no transformation, and no mart logic changes.

This is the single point of geographic control for the entire project.
"""

# ---------------------------------------------------------------------------
# Raleigh-Durham-Cary Combined Statistical Area (CSA)
# 8 counties spanning the Raleigh-Cary MSA and the Durham-Chapel Hill MSA.
# FIPS codes are the 5-digit state(37=NC)+county identifiers.
# ---------------------------------------------------------------------------
RDU_CSA_COUNTIES = {
    "37183": {"name": "Wake",     "msa": "Raleigh-Cary",        "seat": "Raleigh"},
    "37063": {"name": "Durham",   "msa": "Durham-Chapel Hill",  "seat": "Durham"},
    "37135": {"name": "Orange",   "msa": "Durham-Chapel Hill",  "seat": "Hillsborough"},
    "37101": {"name": "Johnston", "msa": "Raleigh-Cary",        "seat": "Smithfield"},
    "37037": {"name": "Chatham",  "msa": "Durham-Chapel Hill",  "seat": "Pittsboro"},
    "37069": {"name": "Franklin", "msa": "Raleigh-Cary",        "seat": "Louisburg"},
    "37077": {"name": "Granville","msa": "Durham-Chapel Hill",  "seat": "Oxford"},
    "37145": {"name": "Person",   "msa": "Durham-Chapel Hill",  "seat": "Roxboro"},
}

# Placeholder for the future statewide expansion. Populate from the Census
# county gazetteer when you scale; the rest of the pipeline will not change.
NC_ALL_COUNTIES = {}  # TODO: load all 100 NC counties (FIPS 37001..37199 odd)

# ---------------------------------------------------------------------------
# THE ONE SWITCH. Change this line to expand scope. Nothing else.
# ---------------------------------------------------------------------------
ACTIVE_SCOPE = RDU_CSA_COUNTIES


def active_county_fips():
    """Return the list of FIPS codes currently in scope (filter key)."""
    return list(ACTIVE_SCOPE.keys())


def active_county_names():
    """Return the list of county names currently in scope."""
    return [c["name"] for c in ACTIVE_SCOPE.values()]


def fips_to_name(fips: str) -> str:
    """Map a FIPS code to a county name; returns 'Unknown' if out of scope."""
    return ACTIVE_SCOPE.get(fips, {}).get("name", "Unknown")
