from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery

app = FastAPI()

PROJECT_ID = "leafy-clone-452120-b6"
DATASET = "property_mgmt"


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    properties = [dict(row) for row in results]
    return properties


@app.get("/properties/{property_id}")
def get_property_by_id(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Returns a single property by ID.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
        property_record = [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not property_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    return property_record[0]


# ---------------------------------------------------------------------------
# Income
# ---------------------------------------------------------------------------

@app.get("/income/{property_id}")
def get_income_by_property(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Returns all income records for a property.
    """
    query = f"""
        SELECT
            income_id,
            property_id,
            income_date,
            income_type,
            amount,
            notes
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY income_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    income_records = [dict(row) for row in results]
    return income_records


@app.post("/income/{property_id}")
def create_income_record(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Creates a new income record for a property.
    """
    try:
        property_check_query = f"""
            SELECT property_id
            FROM `{PROJECT_ID}.{DATASET}.properties`
            WHERE property_id = @property_id
        """

        property_check_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
            ]
        )

        property_results = list(
            bq.query(property_check_query, job_config=property_check_config).result()
        )

        if not property_results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Property with ID {property_id} not found"
            )

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.income`
                (property_id, income_date, income_type, amount, notes)
            VALUES
                (@property_id, CURRENT_DATE(), 'Rent', 1000.00, 'Sample income record')
        """

        insert_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
            ]
        )

        bq.query(insert_query, job_config=insert_config).result()

        return {
            "message": f"Income record created successfully for property {property_id}"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create income record: {str(e)}"
        )


# ---------------------------------------------------------------------------
# Expenses
# ---------------------------------------------------------------------------

@app.get("/expenses/{property_id}")
def get_expenses_by_property(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Returns all expense records for a property.
    """
    query = f"""
        SELECT
            expense_id,
            property_id,
            expense_date,
            expense_type,
            amount,
            vendor,
            notes
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        ORDER BY expense_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    expense_records = [dict(row) for row in results]
    return expense_records


@app.post("/expenses/{property_id}")
def create_expense_record(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Creates a new expense record for a property.
    """
    try:
        property_check_query = f"""
            SELECT property_id
            FROM `{PROJECT_ID}.{DATASET}.properties`
            WHERE property_id = @property_id
        """

        property_check_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
            ]
        )

        property_results = list(
            bq.query(property_check_query, job_config=property_check_config).result()
        )

        if not property_results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Property with ID {property_id} not found"
            )

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
                (property_id, expense_date, expense_type, amount, vendor, notes)
            VALUES
                (@property_id, CURRENT_DATE(), 'Maintenance', 150.00, 'Sample Vendor', 'Sample expense record')
        """

        insert_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
            ]
        )

        bq.query(insert_query, job_config=insert_config).result()

        return {
            "message": f"Expense record created successfully for property {property_id}"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create expense record: {str(e)}"
        )
