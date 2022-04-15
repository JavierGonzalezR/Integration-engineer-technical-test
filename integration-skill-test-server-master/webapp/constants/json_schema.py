json_schema = {
    "definitions": {},
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://example.com/object1606959954.json",
    "title": "Root",
    "type": "object",
    "required": [
        "merchant_id",
        "sku",
        "barcodes",
        "brand",
        "name",
        "description",
        "package",
        "image_url",
        "category",
        "url",
        "branch_products"
    ],
    "properties": {
        "merchant_id": {
            "$id": "#root/store",
            "title": "Merchant Id",
            "type": "string",
            "default": "",
            "examples": [
                "a7a5dac4-d041-4c30-8fb1-0cd3b9534340"
            ],
            "pattern": "^.*$"
        },
        "sku": {
            "$id": "#root/sku",
            "title": "Sku",
            "type": "string",
            "default": "",
            "examples": [
                "6000201734399"
            ],
            "pattern": "^.*$"
        },
        "barcodes": {
            "$id": "#root/barcodes",
            "title": "Barcodes",
            "type": ["null", "array"],
            "items": {
                "$id": "#root/barcodes/items",
                "title": "Items",
                "type": "string",
                "examples": [
                    "62773501448"
                ],
                "pattern": "^.*$"
            }
        },
        "brand": {
            "$id": "#root/brand",
            "title": "Brand",
            "type": ["null", "string"],
            "examples": [
                "Great Value"
            ],
            "pattern": "^.*$"
        },
        "name": {
            "$id": "#root/name",
            "title": "Name",
            "type": "string",
            "examples": [
                "Great Value Pizza Mozzarella Cheese"
            ],
            "pattern": "^.*$"
        },
        "description": {
            "$id": "#root/description",
            "title": "Description",
            "type": "string",
            "default": "",
            "examples": [
                "Say yes please to extra cheese at your next pizza night. Made with 100% Canadian milk, Great ValueTM Pizza Mozzarella Cheese is a good source of calcium that’s high in protein, too. This family-sized 400 g block is a great way to create ooey-gooey, extra cheesy, luscious layers of lasagna, and melty masterpieces of pizza pie perfection."
            ],
            "pattern": "^.*$"
        },
        "package": {
            "$id": "#root/package",
            "title": "Package",
            "type": "string",
            "default": "",
            "examples": [
                "400 g"
            ],
            "pattern": "^.*$"
        },
        "image_url": {
            "$id": "#root/image_url",
            "title": "Image_url",
            "type": ["null", "string"],
            "examples": [
                "https://i5.walmartimages.ca/images/Enlarge/014/487/627735014487.jpg"
            ],
            "pattern": "^.*$"
        },
        "category": {
            "$id": "#root/category",
            "title": "Category",
            "type": ["null", "string"],
            "examples": [
                "Grocery›Dairy & Eggs›Cheese›Cheese Blocks"
            ],
            "pattern": "^.*$"
        },
        "url": {
            "$id": "#root/url",
            "title": "Url",
            "type": ["null", "string"],
            "examples": [
                "https://www.walmart.ca/en/ip/great-value-pizza-mozzarella-cheese/6000201734398"
            ],
            "pattern": "^.*$"
        },
        "branch_products": {
            "$id": "#root/branch_products",
            "title": "Branch_products",
            "type": ["null", "array"],
            "items": {
                "$id": "#root/branch_products/items",
                "title": "Items",
                "type": "object",
                "required": [
                    "branch",
                    "stock",
                    "price"
                ],
                "properties": {
                    "branch": {
                        "$id": "#root/branch_products/items/branch",
                        "title": "Branch",
                        "type": "string",
                        "examples": [
                            "Heartland Supercentre"
                        ],
                        "pattern": "^.*$"
                    },
                    "stock": {
                        "$id": "#root/branch_products/items/stock",
                        "title": "Stock",
                        "type": "integer",
                        "examples": [
                            23
                        ]
                    },
                    "price": {
                        "$id": "#root/branch_products/items/price",
                        "title": "Price",
                        "type": "number",
                        "examples": [
                            12.34
                        ]
                    }
                }
            }

        }
    }
}

merchant_schema = {
    "definitions": {},
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://example.com/object1606966130.json",
    "title": "Root",
    "type": "object",
    "required": [
        "id",
        "name",
        "is_active",
        "can_be_updated",
        "can_be_deleted"
    ],
    "properties": {
        "id": {
            "$id": "#root/id",
            "title": "Id",
            "type": "string",
            "default": "",
            "examples": [
                "14f28a01-2bda-42e1-ba3a-57efd8c3d078"
            ],
            "pattern": "^.*$"
        },
        "name": {
            "$id": "#root/name",
            "title": "Name",
            "type": "string",
            "examples": [
                "CKD"
            ],
            "pattern": "^.*$"
        },
        "is_active": {
            "$id": "#root/is_active",
            "title": "Is_active",
            "type": "boolean",
            "examples": [
                True
            ],
            "default": True
        },
        "can_be_updated": {
            "$id": "#root/can_be_updated",
            "title": "Can_be_updated",
            "type": "boolean",
            "examples": [
                False
            ],
            "default": True
        },
        "can_be_deleted": {
            "$id": "#root/can_be_deleted",
            "title": "Can_be_deleted",
            "type": "boolean",
            "examples": [
                True
            ],
            "default": True
        }
    }
}
