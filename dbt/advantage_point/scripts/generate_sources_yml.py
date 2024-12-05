import yaml

# Define reusable audit_fields
audit_fields = [
    {
        "name": "audit_field_active_flag",
        "description": "Boolean field to determine if record is the most recent/currently active version",
        "data_tests": [
            {
                "not_null": None,
            },
            {
                "accepted_values": {
                    "values": [True, False],
                    "quote": False,
                },
            },
        ],
    },
    {
        "name": "audit_field_record_type",
        "description": "Denotes the type of record operation.",
        "data_tests": [
            {
                "not_null": None,
            },
            {
                "accepted_values": {
                    "values": [
                        "delete",
                        "insert",
                        "update",
                    ],
                    "quote": True,
                },
            },
        ],
    },
    {
        "name": "audit_field_start_datetime_utc",
        "description": "Timestamp (in UTC) to denote time when record entered the database and became active.",
        "data_tests": [
            {
                "not_null": None,
            },
        ],
    },
    {
        "name": "audit_field_end_datetime_utc",
        "description": "Timestamp (in UTC) to denote time when record became irrelevant/deactivated.",
    },
    {
        "name": "audit_field_insert_datetime_utc",
        "description": "Timestamp (in UTC) to denote time when record was inserted into the database.",
        "data_tests": [
            {
                "not_null": None,
            },
        ],
    },
    {
        "name": "audit_field_update_datetime_utc",
        "description": "Timestamp (in UTC) to denote time when record was updated from source in the database.",
    },
    {
        "name": "audit_field_delete_datetime_utc",
        "description": "Timestamp (in UTC) to denote time when record was deleted from source in the database.",
    },
]

# Define tables
tables = [
    {
        "name": "match_points",
        "identifier": "tennisabstract_match_points",
        "description": "Contains points for a given match.",
        "data_tests": [
            {
                "dbt_utils.unique_combination_of_columns": {
                    "combination_of_columns": [
                        "match_url",
                        "point_number",
                        "audit_field_active_flag",
                        "audit_field_start_datetime_utc",
                    ],
                },
            },
        ],
        "columns": [
            {
                "name": "match_url",
                "description": "Link to the match data.",
                "data_tests": [
                    {
                        "not_null": None,
                    },
                ],
            },
            {
                "name": "point_number",
                "description": "Point number in the match.",
                "data_tests": [
                    {
                        "not_null": None,
                    },
                ],
            },
            {
                "name": "server",
                "description": "Player who is serving that point.",
                "data_tests": [
                    {
                        "not_null": None,
                    },
                ],
            },
            {
                "name": "sets",
                "description": "Set score in that match (from server point of view).",
                "data_tests": [
                    {
                        "not_null": None,
                    },
                ],
            },
            {
                "name": "games",
                "description": "Game score in that set (from server point of view).",
                "data_tests": [
                    {
                        "not_null": None,
                    },
                ],
            },
            {
                "name": "points",
                "description": "Point score in that game (from server point of view).",
                "data_tests": [
                    {
                        "not_null": None,
                    },
                ],
            },
            {
                "name": "point_description",
                "description": "Semicolon separated list-like string of describe shots in that point rally.",
                "data_tests": [
                    {
                        "not_null": None,
                    },
                ],
            },
        ] + audit_fields,
    },
    {
        "name": "matches",
        "identifier": "tennisabstract_matches",
        "description": "Contains match information.",
        "data_tests": [
            {
                "dbt_utils.unique_combination_of_columns": {
                    "combination_of_columns": [
                        "match_url",
                        "audit_field_active_flag",
                        "audit_field_start_datetime_utc",
                    ],
                },
            },
        ],
        "columns": [
            {
                "name": "match_url",
                "description": "Link to the match data.",
                "data_tests": [
                    {
                        "not_null": None,
                    },
                ],
            },
            {
                "name": "match_date",
                "description": "Date when match occurred",
                "data_tests": [
                    {
                        "not_null": None,
                    },
                ],
            },
            {
                "name": "match_gender",
                "description": "Gender of participants in the match.",
            },
        ] + audit_fields,
    },
    {
        "name": "players",
        "identifier": "tennisabstract_players",
        "description": "Contains player information.",
        "data_tests": [
            {
                "dbt_utils.unique_combination_of_columns": {
                    "combination_of_columns": [
                        "player_url",
                        "audit_field_active_flag",
                        "audit_field_start_datetime_utc",
                    ],
                },
            },
        ],
        "columns": [
        ] + audit_fields,
    },
]

# Define the full sources.yml structure
sources = {
    "version": 2,
    "sources": [
        {
            "name": "tennisabstract",
            "description": "Data obtained from tennisabstract.com",
            "schema": "{{ env_var('DBT_SCHEMA_INGESTION') }}",
            "tables": tables,
        },
    ],
}

# Write the YAML to a file
with open("dbt/advantage_point/models/sources/sources.yml", "w") as f:
    yaml.dump(
        sources,
        f,
        default_flow_style=False,
        sort_keys=False,
    )
