from fastapi import FastAPI, HTTPException, Query
from ffiec_data_connect import methods, credentials, ffiec_connection

app = FastAPI(title="Deposit Data API")

@app.get("/bank_deposit")
def get_deposit_data(
    user: str = Query(..., description="FFIEC username"),
    token: str = Query(..., description="FFIEC security token"),
    bank_name: str = Query(..., description="Bank name to search for"),
    state: str = Query(None, description="Optional state filter"),
    reporting_period: str = Query(..., description="Reporting period (mm/dd/yyyy)")
):
    """
    Retrieve only the deposit number from a bank's call report filing by filtering
    for the MDRM code "RCON2200", which corresponds to Total Deposits.
    """
    try:
        # 1) Setup credentials/connection
        creds = credentials.WebserviceCredentials(username=user, password=token)
        conn = ffiec_connection.FFIECConnection()

        # 2) Retrieve filers for the reporting period
        filers = methods.collect_filers_on_reporting_period(
            session=conn,
            creds=creds,
            reporting_period=reporting_period,
            output_type="list"
        )
        if not filers:
            return {
                "status": "no_filers",
                "message": f"No filers returned for period {reporting_period}.",
                "selected_filer": None,
                "deposit_data": None
            }

        # 3) Filter to find the bank
        selected_filer = None
        for filer in filers:
            if isinstance(filer, dict):
                name = filer.get("name", "")
                filer_state = filer.get("state", "")
                if bank_name.lower() in name.lower():
                    if state:
                        if state.lower() in filer_state.lower():
                            selected_filer = filer
                            break
                    else:
                        selected_filer = filer
                        break

        if not selected_filer:
            return {
                "status": "bank_not_found",
                "message": f"Bank '{bank_name}' not found for period {reporting_period}.",
                "selected_filer": None,
                "deposit_data": None
            }

        rssd_id = selected_filer.get("id_rssd")
        if not rssd_id:
            return {
                "status": "no_rssd",
                "message": "No RSSD ID found for the selected bank.",
                "selected_filer": selected_filer,
                "deposit_data": None
            }

        # 4) Retrieve raw call report data for the bank
        try:
            time_series = methods.collect_data(
                session=conn,
                creds=creds,
                rssd_id=rssd_id,
                reporting_period=reporting_period,
                series="call"
            )
        except Exception as inner_exc:
            # Check if it’s the “object reference” error from FFIEC
            if "Object reference not set to an instance of an object" in str(inner_exc):
                return {
                    "status": "no_data",
                    "message": "The FFIEC service returned 'Object reference not set...' for this period. Possibly no data was filed.",
                    "selected_filer": selected_filer,
                    "deposit_data": None
                }
            else:
                # Otherwise, re-raise or handle differently
                raise HTTPException(status_code=500, detail=str(inner_exc))

        if not time_series:
            return {
                "status": "no_timeseries",
                "message": "No time series data returned.",
                "selected_filer": selected_filer,
                "deposit_data": None
            }

        # 5) Filter for the deposit metric
        deposit_metrics = [
            metric for metric in time_series
            if metric.get("mdrm", "").upper() == "RCON2200"
        ]

        if not deposit_metrics:
            return {
                "status": "no_deposit",
                "message": "No deposit metric (RCON2200) found in the filing for this period.",
                "selected_filer": selected_filer,
                "deposit_data": None
            }

        return {
            "status": "success",
            "message": "Deposit metric found.",
            "selected_filer": selected_filer,
            "deposit_data": deposit_metrics
        }

    except Exception as e:
        # If it’s some other error, we can still return 500 or handle differently
        raise HTTPException(status_code=500, detail=str(e))
