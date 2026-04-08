from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel, ConfigDict

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
# Pydantic models
# ---------------------------------------------------------------------------

class PropertyCreate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: float


class PropertyUpdate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: float


class IncomeCreate(BaseModel):
    amount: float
    date: str
    description: Optional[str] = None
    source: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)

    def resolved_description(self) -> str:
        return self.description or self.source or "Income record"


class ExpenseCreate(BaseModel):
    amount: float
    date: str
    category: str
    description: str
    vendor: Optional[str] = "Unknown"


# ---------------------------------------------------------------------------
# Helpers
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


def get_next_id(table_name: str, id_column: str, bq: bigquery.Client) -> int:
    query = f"""
        SELECT IFNULL(MAX({id_column}), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.{table_name}`
    """

    try:
        results = list(bq.query(query).result())
        return int(results[0]["next_id"])
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate next ID: {str(e)}"
        )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/")
def health_check():
    return {"status": "healthy"}


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
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
        return [dict(row) for row in results]
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


@app.post("/properties", status_code=status.HTTP_201_CREATED)
def create_property(
    property_data: PropertyCreate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    try:
        new_property_id = get_next_id("properties", "property_id", bq)

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.properties`
                (property_id, name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
            VALUES
                (@property_id, @name, @address, @city, @state, @postal_code, @property_type, @tenant_name, @monthly_rent)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", new_property_id),
                bigquery.ScalarQueryParameter("name", "STRING", property_data.name),
                bigquery.ScalarQueryParameter("address", "STRING", property_data.address),
                bigquery.ScalarQueryParameter("city", "STRING", property_data.city),
                bigquery.ScalarQueryParameter("state", "STRING", property_data.state),
                bigquery.ScalarQueryParameter("postal_code", "STRING", property_data.postal_code),
                bigquery.ScalarQueryParameter("property_type", "STRING", property_data.property_type),
                bigquery.ScalarQueryParameter("tenant_name", "STRING", property_data.tenant_name),
                bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property_data.monthly_rent),
            ]
        )

        bq.query(insert_query, job_config=job_config).result()

        return {
            "property_id": new_property_id,
            "name": property_data.name,
            "address": property_data.address,
            "city": property_data.city,
            "state": property_data.state,
            "postal_code": property_data.postal_code,
            "property_type": property_data.property_type,
            "tenant_name": property_data.tenant_name,
            "monthly_rent": property_data.monthly_rent,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create property: {str(e)}"
        )


@app.put("/properties/{property_id}")
def update_property(
    property_id: int,
    property_data: PropertyUpdate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    try:
        validate_property_exists(property_id, bq)

        update_query = f"""
            UPDATE `{PROJECT_ID}.{DATASET}.properties`
            SET
                name = @name,
                address = @address,
                city = @city,
                state = @state,
                postal_code = @postal_code,
                property_type = @property_type,
                tenant_name = @tenant_name,
                monthly_rent = @monthly_rent
            WHERE property_id = @property_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("name", "STRING", property_data.name),
                bigquery.ScalarQueryParameter("address", "STRING", property_data.address),
                bigquery.ScalarQueryParameter("city", "STRING", property_data.city),
                bigquery.ScalarQueryParameter("state", "STRING", property_data.state),
                bigquery.ScalarQueryParameter("postal_code", "STRING", property_data.postal_code),
                bigquery.ScalarQueryParameter("property_type", "STRING", property_data.property_type),
                bigquery.ScalarQueryParameter("tenant_name", "STRING", property_data.tenant_name),
                bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", property_data.monthly_rent),
            ]
        )

        bq.query(update_query, job_config=job_config).result()

        return {
            "message": f"Property {property_id} updated successfully",
            "property_id": property_id,
            "name": property_data.name,
            "address": property_data.address,
            "city": property_data.city,
            "state": property_data.state,
            "postal_code": property_data.postal_code,
            "property_type": property_data.property_type,
            "tenant_name": property_data.tenant_name,
            "monthly_rent": property_data.monthly_rent,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update property: {str(e)}"
        )


@app.delete("/properties/{property_id}")
def delete_property(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
    try:
        validate_property_exists(property_id, bq)

        delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET}.properties`
            WHERE property_id = @property_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
            ]
        )

        bq.query(delete_query, job_config=job_config).result()

        return {
            "status": "deleted",
            "property_id": property_id
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete property: {str(e)}"
        )


# ---------------------------------------------------------------------------
# Income
# ---------------------------------------------------------------------------

@app.get("/income/{property_id}")
def get_income_by_property(
    property_id: int,
    bq: bigquery.Client = Depends(get_bq_client)
):
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
        return [dict(row) for row in results]
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
    income_data: IncomeCreate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    try:
        validate_property_exists(property_id, bq)
        new_income_id = get_next_id("income", "income_id", bq)

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.income`
                (income_id, property_id, amount, date, description)
            VALUES
                (@income_id, @property_id, @amount, @date, @description)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("income_id", "INT64", new_income_id),
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("amount", "FLOAT64", income_data.amount),
                bigquery.ScalarQueryParameter("date", "DATE", income_data.date),
                bigquery.ScalarQueryParameter("description", "STRING", income_data.resolved_description()),
            ]
        )

        bq.query(insert_query, job_config=job_config).result()

        return {
            "message": f"Income record created successfully for property {property_id}",
            "income_id": new_income_id,
            "property_id": property_id,
            "amount": income_data.amount,
            "date": income_data.date,
            "description": income_data.resolved_description(),
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
        return [dict(row) for row in results]
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
    expense_data: ExpenseCreate,
    bq: bigquery.Client = Depends(get_bq_client)
):
    try:
        validate_property_exists(property_id, bq)
        new_expense_id = get_next_id("expenses", "expense_id", bq)

        insert_query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
                (expense_id, property_id, amount, date, category, vendor, description)
            VALUES
                (@expense_id, @property_id, @amount, @date, @category, @vendor, @description)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("expense_id", "INT64", new_expense_id),
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("amount", "FLOAT64", expense_data.amount),
                bigquery.ScalarQueryParameter("date", "DATE", expense_data.date),
                bigquery.ScalarQueryParameter("category", "STRING", expense_data.category),
                bigquery.ScalarQueryParameter("vendor", "STRING", expense_data.vendor),
                bigquery.ScalarQueryParameter("description", "STRING", expense_data.description),
            ]
        )

        bq.query(insert_query, job_config=job_config).result()

        return {
            "message": f"Expense record created successfully for property {property_id}",
            "expense_id": new_expense_id,
            "property_id": property_id,
            "amount": expense_data.amount,
            "date": expense_data.date,
            "category": expense_data.category,
            "vendor": expense_data.vendor,
            "description": expense_data.description,
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

        property_result = [
            dict(row) for row in bq.query(property_query, job_config=job_config).result()
        ]
        income_result = [
            dict(row) for row in bq.query(income_query, job_config=job_config).result()
        ]
        expense_result = [
            dict(row) for row in bq.query(expense_query, job_config=job_config).result()
        ]

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
