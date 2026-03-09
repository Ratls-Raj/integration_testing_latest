
bcg_data = [
    {
        "lead_id": "L001",
        "source": "MICM",
        "status_score": "98"
    },
    {
        "lead_id": "L003",
        "source": "REFERRAL",
        "status_score": "85"
    },
    {
        "lead_id": "L004",
        "source": "EMAIL_CAMPAIGN",
        "status_score": "90"
    }
]

kc_data = [
    {
        "lead_id": "L002",
        "source": "WALKIN",
        "status_score": "70"
    },
    {
        "lead_id": "L005",
        "source": "SOCIAL_MEDIA",
        "status_score": "80"
    },
    {
        "lead_id": "L006",
        "source": "EVENT",
        "status_score": "75"
    }
]

def seed_leads(db):
    db.insert("BCG", bcg_data)
    db.insert("KC", kc_data)


