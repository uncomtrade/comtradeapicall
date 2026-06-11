# ---------------------------------------------
# Core libraries
# ---------------------------------------------
import pandas as pd
import requests
from . import PreviewGet
import re

# -------------------------------------------------
# Load country group reference data from Comtrade
# -------------------------------------------------


def load_country_groups_from_url(
    url="https://comtradeapi.un.org/files/v1/app/reference/country_groups.json",
    timeout=300
):
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


# -------------------------------------------------
# Build country -> group lookup
# -------------------------------------------------
def build_country_group_lookup(country_group_json):
    lookup = {}
    for grp in country_group_json["results"]:
        gid = grp["GroupID"]
        for code in grp["GroupDefinition"]:
            code = str(code)
            lookup.setdefault(code, []).append(gid)
    return lookup


def parse_input(group_str, group_lookup=None):

    if not group_str or not str(group_str).strip():
        return []

    group_str = str(group_str).strip()

    result = []
    buffer = ""
    inside_parenthesis = False

    # -----------------------------
    # Step 1: Split input correctly
    # -----------------------------
    for char in group_str:
        if char == "(":
            inside_parenthesis = True
            buffer = ""
        elif char == ")":
            inside_parenthesis = False
            result.append(buffer)
            buffer = ""
        elif char == "," and not inside_parenthesis:
            if buffer.strip():
                result.append(buffer.strip())
            buffer = ""
        else:
            buffer += char

    if buffer.strip():
        result.append(buffer.strip())

    final_groups = []

    # -----------------------------
    # Step 2: Process each item
    # -----------------------------
    for item in result:
        item = item.strip()

        # ----------------------------------------
        # CASE 1: Mixed group (e.g., LDC+360+699)
        # ----------------------------------------
        if "+" in item:
            parts = [x.strip() for x in item.split("+")]

            expanded_codes = []

            for p in parts:
                # Expand named group (LDC, ACP, etc.)
                if group_lookup and p in group_lookup:
                    expanded_codes.extend(group_lookup[p])
                else:
                    expanded_codes.append(p)

            codes = expanded_codes
            label = f"({'+'.join(parts)})"

        # ----------------------------------------
        # CASE 2: Named group only (e.g., LDC)
        # ----------------------------------------
        elif group_lookup and item in group_lookup:
            codes = group_lookup[item]
            label = f"({item})"

        # ----------------------------------------
        # CASE 3: Single value (e.g., 360)
        # ----------------------------------------
        else:
            codes = [item]
            label = f"({item})"

        final_groups.append((label, codes))

    return final_groups


def getPreviewData_safe(
    subscription_key,
    typeCode,
    freqCode,
    clCode,
    period,
    reporterCode,
    cmdCode,
    flowCode,
    partnerCode,
    proxy_url=None
):

    df = PreviewGet.getPreviewData(
        subscription_key,
        "TRADEMATRIX",
        typeCode=typeCode,
        freqCode=freqCode,
        clCode=clCode,
        period=period,
        reporterCode=reporterCode,
        cmdCode=cmdCode,
        flowCode=flowCode,
        partnerCode=partnerCode,
        partner2Code=None,
        customsCode=None,
        motCode=None,
        maxRecords=None,
        format_output="JSON",
        aggregateBy=None,
        countOnly=None,
        includeDesc=True,
        breakdownMode="classic",
        proxy_url=proxy_url
    )
    # -------------------------------------------------
    # CHECK SIZE
    # -------------------------------------------------
    if df is not None and len(df) >= 100000:

        print("\n WARNING:API call result exceeds 100K records.The data may be truncated and inaccurate.")
        print("Attempting to refine the query based on period values...")
       # print("To get accurate results, please add more filters.\n")

        # -------------------------------------------------
        # CHECK IF PERIOD IS GROUP (contains '+')
        # -------------------------------------------------
        period = str(period).replace("(", "").replace(")", "").strip()
        print("period", period)

        if "+" in period or "," in period:

            print("Detected grouped period input.\n")
            print("Splitting into individual periods for accurate results......\n")

            # Remove brackets if any
            clean_period = period.replace("(", "").replace(")", "")

            period_list = [p.strip() for p in re.split(
                r"[+,]", clean_period) if p.strip()]

            results = []

            for p in period_list:
                print(f"Processing period: {p}")

                sub_df = PreviewGet.getPreviewData(
                    subscription_key,
                    "TRADEMATRIX",
                    typeCode=typeCode,
                    freqCode=freqCode,
                    clCode=clCode,
                    period=p,
                    reporterCode=reporterCode,
                    cmdCode=cmdCode,
                    flowCode=flowCode,
                    partnerCode=partnerCode,
                    partner2Code=None,
                    customsCode=None,
                    motCode=None,
                    maxRecords=None,
                    format_output="JSON",
                    aggregateBy=None,
                    countOnly=None,
                    includeDesc=True,
                    breakdownMode="classic",
                    proxy_url=proxy_url
                )

                if sub_df is not None:

                    if len(sub_df) >= 100000:
                        print(
                            f" Data size on Period {p} still greater than 100K → truncated")

                    else:
                        print(
                            f" Period {p}: data size = {len(sub_df)} (accurate)")

                    results.append(sub_df)

            if results:
                print("\n Final Result:")
                print(
                    "Split API calls using period and generated accurate results where possible.\n")
                return pd.concat(results, ignore_index=True)

        else:
            # -------------------------------------------------
            # PERIOD NOT GROUP
            # -------------------------------------------------
            print("Period is not grouped. Cannot split further.")
            print("Result remains truncated. Please refine filters.\n")

    # -------------------------------------------------
    # NORMAL CASE
    # -------------------------------------------------
    # if df is not None:
    #     print("Data size is", len(df))

    return df


def getTradeMatrixGroup(
    subscription_key,
    typeCode,
    freqCode,
    period,
    reporterCode,
    cmdCode,
    flowCode,
    partnerCode,
    proxy_url=None
):

    country_groups = load_country_groups_from_url()

    group_definition_lookup = {
        grp["GroupID"]: [str(code) for code in grp["GroupDefinition"]]
        for grp in country_groups["results"]
    }

    periods = parse_input(period)
    reporter_codes = parse_input(reporterCode, group_definition_lookup)
    cmd_codes = parse_input(cmdCode)
    partner_codes = parse_input(partnerCode, group_definition_lookup)

    results = []

    for plabel, pgrp in periods:
        for rlabel, rgrp in reporter_codes:
            for clabel, cgrp in cmd_codes:
                for prtlabel, prtgrp in partner_codes:

                    df = getPreviewData_safe(
                        subscription_key,
                        typeCode,
                        freqCode,
                        "HS",
                        ",".join(pgrp),
                        ",".join(rgrp),
                        ",".join(cgrp),
                        flowCode,
                        ",".join(prtgrp),
                        proxy_url
                    )

                    if df is not None and not df.empty:
                        df = df.copy()
                        # -------------------------------------------------
                        # Handle isReported → isPrimaryValueEstimated
                        # -------------------------------------------------
                        if "isReported" in df.columns:
                            df["isReported"] = df["isReported"].astype(
                                str).str.lower()

                            df["isPrimaryValueEstimated"] = df["isReported"].apply(
                                lambda x: False if x == "true" else True
                            )
                        else:
                            # If column not present, default to False or "Not Available"
                            df["isPrimaryValueEstimated"] = "Not Available"

                        df["period"] = plabel
                        df["reporterCode"] = rlabel
                        df["cmdCode"] = clabel
                        df["partnerCode"] = prtlabel

                        df["reporterLabel"] = rlabel.strip("()")
                        df["partnerLabel"] = prtlabel.strip("()")
                        df["cmdLabel"] = clabel.strip("()")
                        df["flowLabel"] = df["flowDesc"]

                        results.append(df)

    if not results:
        return pd.DataFrame()

    full_df = pd.concat(results, ignore_index=True)

    value_cols = [
        col for col in ["primaryValue"]
        if col in full_df.columns
    ]

    if value_cols:
        full_df[value_cols] = full_df[value_cols].apply(
            pd.to_numeric, errors="coerce"
        )

    agg_dict = {
        "primaryValue": "sum",
        "reporterDesc": lambda x: "+".join(sorted(set(x))),
        "reporterLabel": "first"
    }
    if "isPrimaryValueEstimated" in full_df.columns:
        agg_dict["isPrimaryValueEstimated"] = "max"

    if "partnerDesc" in full_df.columns:
        agg_dict["partnerDesc"] = lambda x: "+".join(sorted(set(x)))
        agg_dict["partnerLabel"] = "first"

    if "cmdDesc" in full_df.columns:
        agg_dict["cmdDesc"] = lambda x: "+".join(sorted(set(x)))
        agg_dict["cmdLabel"] = "first"

    if "flowDesc" in full_df.columns:
        agg_dict["flowDesc"] = lambda x: "+".join(sorted(set(x)))
        agg_dict["flowLabel"] = "first"

    agg_df = (
        full_df
        .groupby(
            ["period", "reporterCode", "partnerCode", "cmdCode", "flowCode"],
            as_index=False
        )
        .agg(agg_dict)
    )

    def replace_desc(row, desc_col, label_col):
        label = row[label_col]
        if isinstance(label, str) and label.isalpha():
            return label
        return row[desc_col]

    agg_df["reporterDesc"] = agg_df.apply(
        lambda r: replace_desc(r, "reporterDesc", "reporterLabel"), axis=1
    )

    if "partnerDesc" in agg_df.columns:
        agg_df["partnerDesc"] = agg_df.apply(
            lambda r: replace_desc(r, "partnerDesc", "partnerLabel"), axis=1
        )

    if "cmdDesc" in agg_df.columns:
        agg_df["cmdDesc"] = agg_df.apply(lambda r: r["cmdLabel"]
                                         if ("+" in r["cmdLabel"] or r["cmdLabel"].isalpha() or
                                             (
                                             any(c.isalpha() for c in r["cmdLabel"]) and
                                             any(c.isdigit()
                                                 for c in r["cmdLabel"])
                                         )
        )
            else r["cmdDesc"],
            axis=1
        )

    if "flowDesc" in agg_df.columns:
        agg_df["flowDesc"] = agg_df.apply(
            lambda r: replace_desc(r, "flowDesc", "flowLabel"), axis=1
        )

    agg_df = agg_df.drop(
        columns=["reporterLabel", "partnerLabel", "cmdLabel", "flowLabel"],
        errors="ignore"
    )
    # -------------------------------------------------
# Add typeCode and freqCode (constant for this call)
# -------------------------------------------------
    agg_df["typeCode"] = typeCode
    agg_df["freqCode"] = freqCode

    # -------------------------------------------------
# Ensure all required output columns exist
# -------------------------------------------------
    required_columns = [
        "typeCode",
        "freqCode",
        "period",
        "reporterCode",
        "reporterDesc",
        "flowCode",
        "flowDesc",
        "partnerCode",
        "partnerDesc",
        "cmdCode",
        "cmdDesc",
        "primaryValue",
        "isPrimaryValueEstimated"
    ]

# Add missing columns with default value
    for col in required_columns:
        if col not in agg_df.columns:
            agg_df[col] = "Not Available"

# -------------------------------------------------
# Reorder columns exactly as required
# -------------------------------------------------
    agg_df = agg_df[required_columns]

    return agg_df
