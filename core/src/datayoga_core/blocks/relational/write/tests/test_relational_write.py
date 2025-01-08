from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest
import sqlalchemy as sa
from datayoga_core import write_utils
from datayoga_core.blocks.relational import utils as relational_utils
from datayoga_core.blocks.relational.write.block import Block
from datayoga_core.opcode import OpCode
from datayoga_core.result import BlockResult
from sqlalchemy.exc import SQLAlchemyError


@pytest.mark.asyncio
@pytest.mark.parametrize("test_scenario", [
    {
        "name": "Mixed Success Scenario with Create and Delete",
        "input_data": [
            {
                "id": 1,
                "name": "John Doe",
                "email": "john@ex.com",
                "opcode": OpCode.CREATE.value
            },
            {
                "id": 2,
                "name": "Jane Smith",
                "email": "jane@example.com",
                "opcode": OpCode.DELETE.value
            },
            {
                "id": 3,
                "name": "Bob Wilson",
                "email": "bob@ex.com",
                "opcode": OpCode.CREATE.value
            },
            {
                "id": 4,
                "name": "Alice Johnson",
                "email": "alice@ex.com",
                "opcode": OpCode.CREATE.value
            },
            {
                "id": 5,
                "name": "Charlie Brown",
                "email": "charlie@ex.com",
                "opcode": OpCode.DELETE.value
            }
        ],
        "expected_processed_create": [1, 3, 4],
        "expected_rejected_create": [],
        "expected_processed_delete": [],
        "expected_rejected_delete": [2, 5],
        "fail_create_records": [],
        "fail_delete_records": [2, 5]
    },
    {
        "name": "All Records Succeed",
        "input_data": [
            {
                "id": 1,
                "name": "Alice",
                "email": "alice@ex.com",
                "opcode": OpCode.CREATE.value
            },
            {
                "id": 2,
                "name": "Bob",
                "email": "bob@ex.com",
                "opcode": OpCode.CREATE.value
            },
            {
                "id": 3,
                "name": "Charlie",
                "email": "charlie@ex.com",
                "opcode": OpCode.DELETE.value
            },
            {
                "id": 4,
                "name": "David",
                "email": "david@ex.com",
                "opcode": OpCode.DELETE.value
            }
        ],
        "expected_processed_create": [1, 2],
        "expected_rejected_create": [],
        "expected_processed_delete": [3, 4],
        "expected_rejected_delete": [],
        "fail_create_records": [],
        "fail_delete_records": []
    },
    {
        "name": "All Records Fail",
        "input_data": [
            {
                "id": 1,
                "name": "Fail One",
                "email": "fail1@ex.com",
                "opcode": OpCode.CREATE.value
            },
            {
                "id": 2,
                "name": "Fail Two",
                "email": "fail2@ex.com",
                "opcode": OpCode.CREATE.value
            },
            {
                "id": 3,
                "name": "Fail Three",
                "email": "fail3@ex.com",
                "opcode": OpCode.DELETE.value
            },
            {
                "id": 4,
                "name": "Fail Four",
                "email": "fail4@ex.com",
                "opcode": OpCode.DELETE.value
            }
        ],
        "expected_processed_create": [],
        "expected_rejected_create": [1, 2],
        "expected_processed_delete": [],
        "expected_rejected_delete": [3, 4],
        "fail_create_records": [1, 2],
        "fail_delete_records": [3, 4]
    }
])
async def test_batch_write_scenarios(test_scenario: Dict[str, Any]):
    """Test different scenarios for batch write operations."""
    # Mock SQLAlchemy engine and connection
    mock_engine: Mock = Mock()
    mock_connection: MagicMock = MagicMock()
    mock_engine.connect.return_value = mock_connection
    mock_engine.dispose = Mock()  # Add dispose method to avoid errors

    # Prepare necessary mocks for table and other dependencies
    metadata: sa.MetaData = sa.MetaData()
    mock_table: sa.Table = sa.Table(
        "users", metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(10)),
        sa.Column("email", sa.String(10))
    )
    for col in mock_table.columns:
        col.name = col.key

    def table_factory(*args: Any, **kwargs: Any) -> sa.Table:
        return mock_table

    def mock_map_record(
        record: Dict[str, Any],
        keys: List[Dict[str, str]],
        mapping: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key in keys:
            field: str = key["field"]
            column: str = key["column"]
            result[column] = record.get(field)

        if mapping:
            for map_item in mapping:
                field: str = map_item["field"]
                column: str = map_item["column"]
                result[column] = record.get(field)

        return result

    # Initialize block with test configuration
    block: Block = Block({
        "connection": "mock_connection",
        "table": "users",
        "opcode_field": "opcode",
        "keys": [{"field": "id", "column": "id"}],
        "mapping": [
            {"field": "name", "column": "name"},
            {"field": "email", "column": "email"}
        ]
    })

    # Mock execute_upsert and execute_delete to simulate selective failures
    def mocked_execute_upsert(records: List[Dict[str, Any]]):
        # Identify records to fail
        failed_records: List[Dict[str, Any]] = [
            record for record in records
            if record["id"] in test_scenario.get("fail_create_records", [])
        ]

        # Raise an exception for batch operation if any records should fail
        if failed_records:
            raise SQLAlchemyError("Batch create operation failed for some records")

    def mocked_execute_delete(records: List[Dict[str, Any]]):
        # Identify records to fail
        failed_records: List[Dict[str, Any]] = [
            record for record in records
            if record["id"] in test_scenario.get("fail_delete_records", [])
        ]

        # Raise an exception for batch operation if any records should fail
        if failed_records:
            raise SQLAlchemyError("Batch delete operation failed for some records")

    # Patch all the necessary dependencies
    with patch.object(relational_utils, "get_engine", return_value=(mock_engine, relational_utils.DbType.PSQL)), \
            patch("sqlalchemy.Table", side_effect=table_factory), \
            patch.object(write_utils, "get_column_mapping",
                         side_effect=lambda m: [{"column": item["column"]} for item in m]), \
            patch.object(write_utils, "map_record", side_effect=mock_map_record), \
            patch.object(block, "execute_upsert", side_effect=mocked_execute_upsert) as mock_upsert, \
            patch.object(block, "execute_delete", side_effect=mocked_execute_delete) as mock_delete:

        block.init()
        result: BlockResult = await block.run(test_scenario["input_data"])

        # Verify result
        assert isinstance(result, BlockResult)

        # For "All Records Succeed" scenario, verify batch processing
        if not test_scenario.get("fail_create_records") and not test_scenario.get("fail_delete_records"):
            # Check that execute methods were called once with all records
            create_records = [r for r in test_scenario["input_data"] if r["opcode"] == OpCode.CREATE.value]
            delete_records = [r for r in test_scenario["input_data"] if r["opcode"] == OpCode.DELETE.value]

            if create_records:
                mock_upsert.assert_called_once()
                # Check that the upsert was called with all create records
                call_args = mock_upsert.call_args[0][0]
                assert len(call_args) == len(create_records)
                assert all(record in call_args for record in create_records)

            if delete_records:
                mock_delete.assert_called_once()
                # Check that the delete was called with all delete records
                call_args = mock_delete.call_args[0][0]
                assert len(call_args) == len(delete_records)
                assert all(record in call_args for record in delete_records)

        # Separate processing for CREATE and DELETE records
        create_processed_ids: List[int] = [
            r.payload["id"] for r in result.processed
            if r.payload.get("opcode") == OpCode.CREATE.value
        ]
        create_rejected_ids: List[int] = [
            r.payload["id"] for r in result.rejected
            if r.payload.get("opcode") == OpCode.CREATE.value
        ]
        delete_processed_ids: List[int] = [
            r.payload["id"] for r in result.processed
            if r.payload.get("opcode") == OpCode.DELETE.value
        ]
        delete_rejected_ids: List[int] = [
            r.payload["id"] for r in result.rejected
            if r.payload.get("opcode") == OpCode.DELETE.value
        ]

        # Check processed and rejected records for CREATE
        assert create_processed_ids == test_scenario["expected_processed_create"], \
            f"Failed in scenario: {test_scenario['name']} - Processed CREATE records mismatch"
        assert create_rejected_ids == test_scenario["expected_rejected_create"], \
            f"Failed in scenario: {test_scenario['name']} - Rejected CREATE records mismatch"

        # Check processed and rejected records for DELETE
        assert delete_processed_ids == test_scenario["expected_processed_delete"], \
            f"Failed in scenario: {test_scenario['name']} - Processed DELETE records mismatch"
        assert delete_rejected_ids == test_scenario["expected_rejected_delete"], \
            f"Failed in scenario: {test_scenario['name']} - Rejected DELETE records mismatch"
