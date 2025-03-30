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
        # Set up credentials and connection
        creds = credentials.WebserviceCredentials(username=user, password=token)
        conn = ffiec_connection.FFIECConnection()

        # Retrieve filers for the reporting period.
        filers = methods.collect_filers_on_reporting_period(
            session=conn,
            creds=creds,
            reporting_period=reporting_period,
            output_type="list"
        )
        if not filers:
            raise HTTPException(
                status_code=500,
                detail="No filers returned. Check the reporting period format or availability."
            )

        # Filter to find the specified bank (by name and optionally state)
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
            raise HTTPException(status_code=404, detail="Bank not found for that reporting period.")

        rssd_id = selected_filer.get("id_rssd")
        if not rssd_id:
            raise HTTPException(status_code=500, detail="No RSSD ID found for the selected bank.")

        # Retrieve raw call report data for the bank
        time_series = methods.collect_data(
            session=conn,
            creds=creds,
            rssd_id=rssd_id,
            reporting_period=reporting_period,
            series="call"
        )
        if not time_series:
            raise HTTPException(status_code=500, detail="No time series data returned.")

        # Filter for the deposit metric with MDRM code "RCON2200"
        deposit_metrics = [metric for metric in time_series if metric.get("mdrm", "").upper() == "RCON2200"]

        if not deposit_metrics:
            raise HTTPException(status_code=404, detail="Deposit metric (RCON2200) not found in the filing for the specified reporting period.")

        return {
            "selected_filer": selected_filer,
            "deposit_data": deposit_metrics
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
