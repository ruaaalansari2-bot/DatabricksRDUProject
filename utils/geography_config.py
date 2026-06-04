"""
Geographic scope configuration for the Syracuse Market Intelligence Lakehouse.

DESIGN PRINCIPLE — "Syracuse now, NY-scalable later":
The pipeline is keyed on standard FIPS county codes, which are stable and
identical for every county in New York (and the US). To expand from
the Syracuse MSA to all of NY, you change ONLY the ACTIVE_SCOPE
assignment below. No schema, no transformation, and no mart logic changes.

This is the single point of geographic control for the entire project.
"""

# ---------------------------------------------------------------------------
# Syracuse Metropolitan Statistical Area (MSA)
# 4 counties in upstate New York centered around Syracuse.
# FIPS codes are the 5-digit state(36=NY)+county identifiers.
# ---------------------------------------------------------------------------
SYRACUSE_MSA_COUNTIES = {
    "36067": {"name": "Onondaga",  "msa": "Syracuse",  "seat": "Syracuse"},
    "36053": {"name": "Madison",   "msa": "Syracuse",  "seat": "Wampsville"},
    "36069": {"name": "Oswego",    "msa": "Syracuse",  "seat": "Oswego"},
    "36075": {"name": "Cayuga",    "msa": "Syracuse",  "seat": "Auburn"},
}

# Placeholder for the future statewide expansion. Populate from the Census
# county gazetteer when you scale; the rest of the pipeline will not change.
NY_ALL_COUNTIES = {}  # TODO: load all 62 NY counties (FIPS 36001..36123 odd)

# ---------------------------------------------------------------------------
# THE ONE SWITCH. Change this line to expand scope. Nothing else.
# ---------------------------------------------------------------------------
ACTIVE_SCOPE = SYRACUSE_MSA_COUNTIES


def active_county_fips():
    """Return the list of FIPS codes currently in scope (filter key)."""
    return list(ACTIVE_SCOPE.keys())


def active_county_names():
    """Return the list of county names currently in scope."""
    return [c["name"] for c in ACTIVE_SCOPE.values()]


def fips_to_name(fips: str) -> str:
    """Map a FIPS code to a county name; returns 'Unknown' if out of scope."""
    return ACTIVE_SCOPE.get(fips, {}).get("name", "Unknown")
