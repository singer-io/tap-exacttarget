from datetime import datetime
from unittest.mock import Mock
from .base_test import BaseClientTest


class TestFilterCreation(BaseClientTest):
    """Tests for creating simple and complex SOAP filters."""

    def test_create_simple_filter_with_string_value(self):
        """Test creating a simple filter with a single string value."""

        mock_filter_instance = Mock()
        mock_filter_type = Mock(return_value=mock_filter_instance)
        self.client_instance.soap_client.get_type = Mock(return_value=mock_filter_type)

        result = self.client_instance.create_simple_filter(
            property_name="Status", operator="equals", value="Active"
        )

        self.client_instance.soap_client.get_type.assert_called_once_with("ns0:SimpleFilterPart")
        mock_filter_type.assert_called_once_with(Property="Status", SimpleOperator="equals")
        assert mock_filter_instance.Value == ["Active"]
        assert result is mock_filter_instance

    def test_create_simple_filter_with_date_value(self):
        """Test creating a simple filter with a date value instead of string value."""

        mock_filter_instance = Mock()
        mock_filter_type = Mock(return_value=mock_filter_instance)
        self.client_instance.soap_client.get_type = Mock(return_value=mock_filter_type)

        test_date = datetime(2024, 1, 1, 12, 0, 0)
        result = self.client_instance.create_simple_filter(
            property_name="CreatedDate", operator="greaterThan", date_value=test_date
        )

        self.client_instance.soap_client.get_type.assert_called_once_with("ns0:SimpleFilterPart")
        mock_filter_type.assert_called_once_with(
            Property="CreatedDate", SimpleOperator="greaterThan"
        )
        assert mock_filter_instance.DateValue == test_date
        assert result is mock_filter_instance

    def test_create_simple_filter_with_list_values(self):
        """Test creating a simple filter with a list of values (for IN operator)."""

        mock_filter_instance = Mock()
        mock_filter_type = Mock(return_value=mock_filter_instance)
        self.client_instance.soap_client.get_type = Mock(return_value=mock_filter_type)

        values = ["Active", "Pending", "Suspended"]
        result = self.client_instance.create_simple_filter(
            property_name="Status", operator="IN", value=values
        )
        self.client_instance.soap_client.get_type.assert_called_once_with("ns0:SimpleFilterPart")
        mock_filter_type.assert_called_once_with(Property="Status", SimpleOperator="IN")
        assert mock_filter_instance.Value == values
        assert result is mock_filter_instance

    def test_create_simple_filter_handles_none_values_safely(self):
        """Test that filter creation handles case where no value or date_value provided."""

        mock_filter_instance = Mock()
        mock_filter_type = Mock(return_value=mock_filter_instance)
        self.client_instance.soap_client.get_type = Mock(return_value=mock_filter_type)

        self.client_instance.create_simple_filter(property_name="Field", operator="isNull")

        mock_filter_type.assert_called_once_with(Property="Field", SimpleOperator="isNull")

        assert "Value" not in mock_filter_instance.__dict__
        assert "DateValue" not in mock_filter_instance.__dict__

    def test_create_complex_filter_combines_filters_correctly(self):
        """Test creating a complex filter that combines two conditions with AND/OR."""

        mock_complex_instance = Mock()
        mock_complex_filter_type = Mock(return_value=mock_complex_instance)
        self.client_instance.soap_client.get_type = Mock(return_value=mock_complex_filter_type)

        left_filter = Mock(name="LeftFilter")
        right_filter = Mock(name="RightFilter")

        self.client_instance.create_complex_filter(
            left_operand=left_filter, logical_operator="AND", right_operand=right_filter
        )

        mock_complex_filter_type.assert_called_once_with(
            LeftOperand=left_filter, LogicalOperator="AND", RightOperand=right_filter
        )

    def test_create_complex_filter_with_nested_filters(self):
        """Test creating nested complex filters (complex filter as operand)."""
        mock_complex_filter_type = Mock()

        instances = []

        def create_instance(**kwargs):
            instance = Mock()
            instance.kwargs = kwargs
            instances.append(instance)
            return instance

        mock_complex_filter_type.side_effect = create_instance
        self.client_instance.soap_client.get_type = Mock(return_value=mock_complex_filter_type)

        filter_a = Mock(name="FilterA")
        filter_b = Mock(name="FilterB")
        inner_complex = self.client_instance.create_complex_filter(
            left_operand=filter_a, logical_operator="AND", right_operand=filter_b
        )

        filter_c = Mock(name="FilterC")
        outer_complex = self.client_instance.create_complex_filter(
            left_operand=inner_complex, logical_operator="OR", right_operand=filter_c
        )

        assert mock_complex_filter_type.call_count == 2

        assert inner_complex is instances[0]
        assert outer_complex is instances[1]

        assert instances[0].kwargs == {
            "LeftOperand": filter_a,
            "LogicalOperator": "AND",
            "RightOperand": filter_b,
        }

        assert instances[1].kwargs == {
            "LeftOperand": inner_complex,
            "LogicalOperator": "OR",
            "RightOperand": filter_c,
        }
