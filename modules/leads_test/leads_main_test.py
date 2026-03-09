import sys, os, json, boto3

os.environ["AWS_DEFAULT_REGION"] = "ap-south-1"

# Project Paths
project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../")
)

src_path = os.path.join(project_root, "src")

sys.path.insert(0, src_path)
sys.path.insert(0, project_root)
print("SRC path:", src_path)

from integration_testing.mock_database.database import MockDatabase
fake_db = MockDatabase()
boto3.client = lambda service, **kwargs: fake_db
from src.leads.api import lambda_handler

# JSON Test File
TEST_CASE_FILE = os.path.join(
    project_root,
    "integration_testing",
    "modules",
    "leads_test",
    "test_cases.json"
)

RESULT_FILE = os.path.join(
    project_root,
    "integration_testing",
    "modules",
    "leads_test",
    "test_results.json"
)


# -------------------------------
# Load Test Cases
# -------------------------------
def load_tests():
    with open(TEST_CASE_FILE) as f:
        return json.load(f)


# -------------------------------
# Save Test Results
# -------------------------------
def save_results(results):
    with open(RESULT_FILE, "w") as f:
        json.dump(results, f, indent=4)


# -------------------------------
# Evaluate JSON Assertions
# -------------------------------
def evaluate_rules(case, body):
    expected = case.get("expected", {})
    rules = case.get("result", {})

    failures = {}
    for rule_id, rule_expression in rules.items():
        try:
            result = eval(rule_expression, {}, {
                "body": body,
                "expected": expected
            })
            if not result:
                failures[rule_id] = f"{rule_expression} returned False"

        except Exception as err:
            failures[rule_id] = f"{rule_expression} -> {str(err)}"
    return failures

# -------------------------------
# Run Individual Test Case
# -------------------------------
def run_test_case(name, case, test_cases):
    print(f"\n===== Running Test Case: {name} =====")

    table_name = case.get("table_name")
    if not table_name:
        return {"table_name": "Missing table_name"}
    
    fake_db.create_collection(table_name)

    # ---------------------------
    # Seed Mock Database
    # ---------------------------
    for item in case.get("feed_data", []):
        if item.get("PK") == "VARS":
            item["vars"]["table_name"] = table_name
        fake_db.insert(table_name, item)

    # ---------------------------
    # Prepare Lambda Event
    # ---------------------------
    event = case.get("payload", {})
    event["requestContext"]["authorizer"]["lambda"]["table_name"] = table_name

    # ---------------------------
    # Call Lambda Handler
    # ---------------------------
    response = lambda_handler(event, None)
    body = response.get("profiles", [])
    print("Lambda Response:", body)

    # ---------------------------
    # Execute Assertions
    # ---------------------------
    failures = evaluate_rules(case, body)
    return failures


# -------------------------------
# Main Pytest Function
# -------------------------------
def test_leads():
    test_cases = load_tests()
    overall_failures = {}
    for name, case in test_cases.items():
        try:
            failures = run_test_case(name, case, test_cases)
            if failures:
                test_cases[name]["status"] = "Fail"
                test_cases[name]["reason"] = failures
                overall_failures[name] = failures
            else:
                test_cases[name]["status"] = "Pass"
                test_cases[name]["reason"] = {}

        except Exception as e:
            test_cases[name]["status"] = "Error"
            test_cases[name]["reason"] = {"exception": str(e)}
            overall_failures[name] = {"exception": str(e)}

    save_results(test_cases)

    print("\n================ TEST SUMMARY ================\n")

    for name, case in test_cases.items():
        status = case["status"]
        if status == "Pass":
            print(f"✔ {name} -> PASS")
        else:
            print(f"✖ {name} -> {status}")
            for rule, reason in case.get("reason", {}).items():
                print(f"     Rule {rule} : {reason}")

    print("\n==============================================\n")

    if overall_failures:
        raise AssertionError(f"{len(overall_failures)} test(s) failed out of {len(test_cases)}")