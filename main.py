from mock_database.database import MockDatabase
from mock_database.seed_data import seed_leads

db = MockDatabase()

seed_leads(db)

result = db.query(
    "BCG",
    lambda r: r["source"] == "MICM" and r["status_score"] == "98"
)

print(result)
