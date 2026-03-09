def lambda_handler(event, context):
    return {
        "profiles": [
            {"name": "Raj"},
            {"name": "Test User"}
        ]
    }