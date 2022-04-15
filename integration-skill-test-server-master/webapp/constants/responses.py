import os

RICHARD_ID = os.environ.get("RICHARD_ID")
BEAUTY_ID = os.environ.get("BEAUTY_ID")

merchants_data = {
    "merchants": [
        {
            "id": "14f28a01-2bda-42e1-ba3a-57efd8c3d078",
            "name": "CKD",
            "is_active": True,
            "can_be_updated": False,
            "can_be_deleted": False
        },
        {
            "id": "b5a785d7-d386-4354-bcf5-cb7bdd2d28e5",
            "name": "Laider",
            "is_active": False,
            "can_be_updated": False,
            "can_be_deleted": False
        },
        {
            "id": RICHARD_ID,
            "name": "Richard's",
            "is_active": False,
            "can_be_updated": True,
            "can_be_deleted": False
        },
        {
            "id": BEAUTY_ID,
            "name": "Beauty",
            "is_active": True,
            "can_be_updated": False,
            "can_be_deleted": True
        }
    ]
}
