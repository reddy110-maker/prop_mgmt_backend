from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery

app = FastAPI()

PROJECT_ID = "leafy-clone-452120-b6"
DATASET = "property_mgmt"

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client(project=PROJECT_ID)
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Helper: check property exists
# ---------------------------------------------------------------------------

def validate_property_exists(property_id: int, bq: bigquery.Client):
    query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    results = list(bq.query(query, job_config=job_config).result())

    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/")
def health_check():
    """
    Basic health check endpoint.
    """
    return {"status": "healthy"}


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
        properties = [dict(row) for row in results]
        return properties
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


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

        if not property_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Property with ID {property_id} not found"
            )

        return property_record[0]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


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
            amount,
            date,
            description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        validate_property_exists(property_id, bq)
        results = bq.query(query, job_config=job_config).result()
        income_records = [dict(row) for row in results]
        return income_records
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.post("/income/{property_id}")
def create_income_record(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Creates a new sample income record for a property.
    """
    try:
        validate_property_exists(property_id, bq)

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.income`
                (income_id, property_id, amount, date, description)
            VALUES
                (
                    (SELECT IFNULL(MAX(income_id), 0) + 1 FROM `{PROJECT_ID}.{DATASET}.income`),
                    @property_id,
                    1000.00,
                    CURRENT_DATE(),
                    'Sample income record'
                )
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
            amount,
            date,
            category,
            vendor,
            description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        ORDER BY date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        validate_property_exists(property_id, bq)
        results = bq.query(query, job_config=job_config).result()
        expense_records = [dict(row) for row in results]
        return expense_records
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.post("/expenses/{property_id}")
def create_expense_record(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Creates a new sample expense record for a property.
    """
    try:
        validate_property_exists(property_id, bq)

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
                (expense_id, property_id, amount, date, category, vendor, description)
            VALUES
                (
                    (SELECT IFNULL(MAX(expense_id), 0) + 1 FROM `{PROJECT_ID}.{DATASET}.expenses`),
                    @property_id,
                    150.00,
                    CURRENT_DATE(),
                    'Maintenance',
                    'Sample Vendor',
                    'Sample expense record'
                )
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


# ---------------------------------------------------------------------------
# Additional Endpoints
# ---------------------------------------------------------------------------

@app.get("/properties/{property_id}/income/total")
def get_total_income_for_property(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Returns the total income amount for a property.
    """
    query = f"""
        SELECT
            property_id,
            IFNULL(SUM(amount), 0) AS total_income
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        GROUP BY property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        validate_property_exists(property_id, bq)
        results = [dict(row) for row in bq.query(query, job_config=job_config).result()]

        if not results:
            return {
                "property_id": property_id,
                "total_income": 0
            }

        return results[0]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.get("/properties/{property_id}/expenses/total")
def get_total_expenses_for_property(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Returns the total expense amount for a property.
    """
    query = f"""
        SELECT
            property_id,
            IFNULL(SUM(amount), 0) AS total_expenses
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        GROUP BY property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        validate_property_exists(property_id, bq)
        results = [dict(row) for row in bq.query(query, job_config=job_config).result()]

        if not results:
            return {
                "property_id": property_id,
                "total_expenses": 0
            }

        return results[0]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.get("/expenses/{property_id}/by-category")
def get_expenses_by_category(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Returns total expenses grouped by category for a property.
    """
    query = f"""
        SELECT
            category,
            SUM(amount) AS total_amount
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        GROUP BY category
        ORDER BY total_amount DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        validate_property_exists(property_id, bq)
        results = bq.query(query, job_config=job_config).result()
        categories = [dict(row) for row in results]

        return {
            "property_id": property_id,
            "expense_categories": categories
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )


@app.get("/properties/{property_id}/summary")
def get_property_summary(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Returns a financial summary for a property including total income,
    total expenses, and net income.
    """
    property_query = f"""
        SELECT
            property_id,
            name,
            city,
            state,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    income_query = f"""
        SELECT IFNULL(SUM(amount), 0) AS total_income
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
    """

    expense_query = f"""
        SELECT IFNULL(SUM(amount), 0) AS total_expenses
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        validate_property_exists(property_id, bq)

        property_result = [dict(row) for row in bq.query(property_query, job_config=job_config).result()]
        income_result = [dict(row) for row in bq.query(income_query, job_config=job_config).result()]
        expense_result = [dict(row) for row in bq.query(expense_query, job_config=job_config).result()]

        property_info = property_result[0]
        total_income = income_result[0]["total_income"]
        total_expenses = expense_result[0]["total_expenses"]
        net_income = total_income - total_expenses

        return {
            "property": property_info,
            "total_income": total_income,
            "total_expenses": total_expenses,
            "net_income": net_income
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )
