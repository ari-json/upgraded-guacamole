from fastapi import FastAPI, HTTPException, Query
from ffiec_data_connect import methods, credentials, ffiec_connection

app = FastAPI(title="Institution-Wide Deposit Data API")

@app.get("/bank_deposit")
def get_deposit_data(
    user: str = Query(..., description="FFIEC username"),
    token: str = Query(..., description="FFIEC security token"),
    bank_name: str = Query(..., description="Bank name to search for"),
    state: str = Query(None, description="Optional state filter"),
    reporting_period: str = Query(..., description="Reporting period (mm/dd/yyyy)")
):
    """
    Retrieve the institution-wide deposit number from a bank's call report filing.
    This endpoint filters for the MDRM code "RCFD2200" (which is used for total deposits including domestic and foreign).
    """
    try:
        # 1) Set up credentials and connection
        creds = credentials.WebserviceCredentials(username=user, password=token)
        conn = ffiec_connection.FFIECConnection()

        # 2) Retrieve filers for the specified reporting period
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

        # 3) Filter to find the specified bank by name (and optionally state)
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

        # 4) Retrieve raw call report time series data for the bank
        try:
            time_series = methods.collect_data(
                session=conn,
                creds=creds,
                rssd_id=rssd_id,
                reporting_period=reporting_period,
                series="call"
            )
        except Exception as inner_exc:
            if "Object reference not set to an instance of an object" in str(inner_exc):
                return {
                    "status": "no_data",
                    "message": "The FFIEC service returned an 'Object reference not set...' error for this period, possibly indicating no data was filed.",
                    "selected_filer": selected_filer,
                    "deposit_data": None
                }
            else:
                raise HTTPException(status_code=500, detail=str(inner_exc))

        if not time_series:
            return {
                "status": "no_timeseries",
                "message": "No time series data returned.",
                "selected_filer": selected_filer,
                "deposit_data": None
            }

        # 5) Filter for the deposit metric with MDRM code "RCFD2200"
        deposit_metrics = [
            metric for metric in time_series
            if metric.get("mdrm", "").upper() == "RCFD2200"
        ]

        if not deposit_metrics:
            return {
                "status": "no_deposit",
                "message": "Deposit metric (RCFD2200) not found in the filing for the specified period.",
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
        raise HTTPException(status_code=500, detail=str(e))
