import json
import sys
import pandas as pd
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

from google.oauth2 import service_account
from google.cloud import compute_v1
from google.cloud import logging_v2

logging.basicConfig(level=logging.INFO)

projects = json.loads(sys.argv[1])
excel_file = sys.argv[2]

inventory_rows = []
weekly_rows = []

week_ago = datetime.now(timezone.utc) - timedelta(days=7)


def collect_project(project_cfg):

    project_id = project_cfg["name"]
    env = project_cfg["environment"]
    key_file = project_cfg["service_account"]

    logging.info(f"Scanning project {project_id}")

    creds = service_account.Credentials.from_service_account_file(key_file)

    # Compute Clients
    network_client = compute_v1.NetworksClient(credentials=creds)
    router_client = compute_v1.RoutersClient(credentials=creds)

    # Logging Client
    log_client = logging_v2.Client(
        project=project_id,
        credentials=creds
    )

    local_inventory = []
    local_weekly = []

    # -------------------
    # VPC Networks
    # -------------------

    try:
        for net in network_client.list(project=project_id):

            local_inventory.append({
                "Project": project_id,
                "Environment": env,
                "ResourceType": "VPC",
                "Name": net.name,
                "Region": "GLOBAL",
                "Network": "",
                "CreationTime": net.creation_timestamp
            })

    except Exception as e:
        logging.error(f"Network error {project_id}: {e}")

    # -------------------
    # Routers
    # -------------------

    try:
        agg = router_client.aggregated_list(project=project_id)

        for region, data in agg:

            if not data.routers:
                continue

            for r in data.routers:

                region_name = r.region.split("/")[-1]

                local_inventory.append({
                    "Project": project_id,
                    "Environment": env,
                    "ResourceType": "Router",
                    "Name": r.name,
                    "Region": region_name,
                    "Network": r.network.split("/")[-1],
                    "CreationTime": r.creation_timestamp
                })

    except Exception as e:
        logging.error(f"Router error {project_id}: {e}")

    # -------------------
    # Audit Logs
    # -------------------

    try:

        query = f'''
        timestamp >= "{week_ago.isoformat()}"
        AND protoPayload.serviceName="compute.googleapis.com"
        '''

        entries = log_client.list_entries(filter_=query)

        for entry in entries:

            proto = entry.payload

            method = proto.get("methodName", "")

            action = None

            if "insert" in method:
                action = "CREATED"
            elif "delete" in method:
                action = "DELETED"

            if not action:
                continue

            local_weekly.append({
                "Project": project_id,
                "ActionDate": entry.timestamp,
                "ActionBy": proto.get(
                    "authenticationInfo", {}
                ).get("principalEmail"),
                "Action": action,
                "Resource": proto.get("resourceName")
            })

    except Exception as e:
        logging.error(f"Logging error {project_id}: {e}")

    return local_inventory, local_weekly


# --------------------------
# Parallel Execution
# --------------------------

with ThreadPoolExecutor(max_workers=5) as executor:

    results = executor.map(collect_project, projects)

    for inv, week in results:
        inventory_rows.extend(inv)
        weekly_rows.extend(week)


# --------------------------
# DataFrames
# --------------------------

inventory_df = pd.DataFrame(inventory_rows)
weekly_df = pd.DataFrame(weekly_rows)

if not weekly_df.empty:

    weekly_df["ActionDate"] = pd.to_datetime(weekly_df["ActionDate"])
    weekly_df["DaysAgo"] = (
        pd.Timestamp.now(tz="UTC") - weekly_df["ActionDate"]
    ).dt.days

    weekly_df["ActionDate"] = weekly_df["ActionDate"].dt.tz_localize(None)


summary = (
    inventory_df.groupby(["Environment", "ResourceType"])
    .size()
    .reset_index(name="Count")
)

added = weekly_df[weekly_df["Action"] == "CREATED"].shape[0]
deleted = weekly_df[weekly_df["Action"] == "DELETED"].shape[0]

summary["AddedLast7Days"] = added
summary["DeletedLast7Days"] = deleted


# --------------------------
# Excel Report
# --------------------------

with pd.ExcelWriter(excel_file) as writer:

    inventory_df.to_excel(writer, "Inventory", index=False)
    weekly_df.to_excel(writer, "Weekly_Actions", index=False)
    summary.to_excel(writer, "Summary", index=False)

logging.info(f"Report generated: {excel_file}")
